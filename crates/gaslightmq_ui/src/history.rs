//! The undo history of a tab: the changes that this tab made, what takes each back, and what makes it again.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName};
use gaslightmq_control_api::surface::{Encoder, Hold};

/// Whatever a change of the tab changed, which has a revision of its own.
///
/// A rule of the workspace is two subjects: its definition, and the rule in each environment, with its state there.
/// A change of the definition is a change of the rule in every environment too, because the revision of a rule in an
/// environment is the later of the two.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Subject {
    /// A rule in an environment: a live rule, or a rule of the workspace with its state there.
    Rule {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// A definition of the workspace, as its file holds it.
    Definition(DefinitionRef),
}

impl Subject {
    /// The rule of that name in `environment`.
    pub fn rule(environment: &EnvironmentName, rule: &RuleName) -> Self {
        Subject::Rule {
            environment: environment.clone(),
            rule: rule.clone(),
        }
    }

    /// The environment that the subject belongs to, or `None` for a definition of the workspace.
    pub fn environment(&self) -> Option<&EnvironmentName> {
        match self {
            Subject::Rule { environment, .. } => Some(environment),
            Subject::Definition(_) => None,
        }
    }

    /// Whether a change of `self` that left a revision is a change of `other` too:
    /// a rule of the workspace changes in every environment when its definition changes.
    fn covers(&self, other: &Subject) -> bool {
        match (self, other) {
            (Subject::Definition(DefinitionRef::Rule(changed)), Subject::Rule { rule, .. }) => changed == rule,
            _ => self == other,
        }
    }
}

/// Writes the rule by its name, and a definition by its kind and its name: `slow-order-producers`,
/// `rule "slow-order-producers"`.
impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::Rule { rule, .. } => write!(f, "{rule}"),
            Subject::Definition(definition) => write!(f, "{definition}"),
        }
    }
}

/// An operation that the history sends: what takes a change of the tab back, or what makes it again.
///
/// It is an ordinary operation of the control plane. The history sends it with what it
/// [expects](Expects): the run and the revision that the tab left on its [subject](Self::subject) last.
/// A create expects nothing: it refuses a name that is taken, so it never overwrites.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Arm a rule.
    ArmRule(ArmRule),
    /// Disarm a rule.
    DisarmRule(DisarmRule),
    /// Make a live rule, armed or not.
    CreateLiveRule(CreateLiveRule),
    /// Replace a live rule.
    UpdateLiveRule(UpdateLiveRule),
    /// Take a live rule out of its environment.
    DeleteLiveRule(DeleteLiveRule),
    /// Replace a rule of the workspace.
    UpdateRule(UpdateDefinition<Rule>),
}

impl Change {
    /// What the operation changes.
    pub fn subject(&self) -> Subject {
        match self {
            Change::ArmRule(ArmRule { environment, rule, .. })
            | Change::DisarmRule(DisarmRule { environment, rule, .. })
            | Change::DeleteLiveRule(DeleteLiveRule { environment, rule, .. }) => Subject::rule(environment, rule),
            Change::CreateLiveRule(CreateLiveRule { environment, rule, .. })
            | Change::UpdateLiveRule(UpdateLiveRule { environment, rule, .. }) => {
                Subject::rule(environment, rule.name())
            }
            Change::UpdateRule(update) => Subject::Definition(DefinitionRef::Rule(update.definition.name().clone())),
        }
    }

    /// The same operation, expecting `expects` of its subject. A create expects nothing, and stays as it is.
    fn expecting(self, expects: Expects) -> Self {
        let expects = Some(expects);
        match self {
            Change::ArmRule(arm) => Change::ArmRule(ArmRule { expects, ..arm }),
            Change::DisarmRule(disarm) => Change::DisarmRule(DisarmRule { expects, ..disarm }),
            Change::UpdateLiveRule(update) => Change::UpdateLiveRule(UpdateLiveRule { expects, ..update }),
            Change::DeleteLiveRule(delete) => Change::DeleteLiveRule(DeleteLiveRule { expects, ..delete }),
            Change::UpdateRule(update) => Change::UpdateRule(UpdateDefinition { expects, ..update }),
            create @ Change::CreateLiveRule(_) => create,
        }
    }

    /// The same operation, expecting nothing yet: the history says what it expects when it sends it.
    fn expecting_nothing(self) -> Self {
        match self {
            Change::ArmRule(arm) => Change::ArmRule(ArmRule { expects: None, ..arm }),
            Change::DisarmRule(disarm) => Change::DisarmRule(DisarmRule {
                expects: None,
                ..disarm
            }),
            Change::UpdateLiveRule(update) => Change::UpdateLiveRule(UpdateLiveRule {
                expects: None,
                ..update
            }),
            Change::DeleteLiveRule(delete) => Change::DeleteLiveRule(DeleteLiveRule {
                expects: None,
                ..delete
            }),
            Change::UpdateRule(update) => Change::UpdateRule(UpdateDefinition {
                expects: None,
                ..update
            }),
            create @ Change::CreateLiveRule(_) => create,
        }
    }

    /// What the operation did, by name, once it applied as the `cause` of a request.
    /// An undo can arm a fault, so the UI says so every time: *Armed slow-order-producers again*.
    fn done(&self, cause: RequestCause) -> String {
        let subject = self.subject();
        match (self, cause) {
            (Change::ArmRule(_), _) => format!("Armed {subject} again"),
            (Change::DisarmRule(_), _) => format!("Disarmed {subject} again"),
            (Change::CreateLiveRule(create), _) if create.armed => format!("Armed the live rule {subject} again"),
            (Change::CreateLiveRule(_), _) => format!("Made the live rule {subject} again"),
            (Change::DeleteLiveRule(_), RequestCause::Undo) => format!("Deleted the live rule {subject}"),
            (Change::DeleteLiveRule(_), RequestCause::Redo) => format!("Deleted the live rule {subject} again"),
            (Change::UpdateLiveRule(_) | Change::UpdateRule(_), RequestCause::Undo) => format!("Set {subject} back"),
            (Change::UpdateLiveRule(_) | Change::UpdateRule(_), RequestCause::Redo) => {
                format!("Changed {subject} again")
            }
        }
    }
}

/// One step of the history: one gesture of the tab that changed something, what takes it back, and what makes it
/// again.
///
/// A step is made from what the tab sent and what the control plane answered to it, never from what the tab expects a
/// press or a turn to do. The inverse restores a state, and never replays a gesture.
///
/// | The tab | Takes it back | Makes it again |
/// |---|---|---|
/// | [Arms](Self::armed) a rule | Disarm it. | Arm it. |
/// | [Disarms](Self::disarmed) a rule | Arm it. | Disarm it. |
/// | [Makes](Self::created) a live rule, in ⌘K armed | Delete it. | Make it again, armed when it was. |
/// | [Deletes](Self::deleted) a live rule | Make it again, armed when it was armed. | Delete it. |
/// | [Updates](Self::updated_live) a live rule | Update it to the rule before. | Update it to the rule after. |
/// | [Updates](Self::updated_rule) a rule of the workspace | Update it to the rule before. | Update it to the rule after. |
/// | [Presses](Self::pressed) a pad that arms a rule | Disarm it, or delete it when the press made it. | Arm it, or make it armed. |
/// | [Presses](Self::pressed) a pad that disarms a rule | Arm it. | Disarm it. |
/// | [Holds](History::held) a pad that makes a rule, and lets go | Delete it. | Make it again, disarmed. |
/// | [Turns](Self::turned) an encoder, once or one turn after another | Update the live rule to the one before the first turn. | Update it to the one after the last turn. |
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    back: Change,
    again: Change,
    /// The revision that the change left.
    left: Revision,
    /// The turn that made the step, when a turn made it.
    turn: Option<Turn>,
}

/// What a turn found, which decides whether its step holds the rule before it, and whether it joins the turn before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Turn {
    /// The encoder that turned.
    encoder: Encoder,
    /// The revision of the rule that the turn acted on, as its answer says.
    found: Option<Revision>,
    /// The revision of the rule that the tab showed when someone turned.
    shown: Revision,
}

impl Step {
    /// A step of `back` and `again`, which change one subject.
    fn new(back: Change, again: Change, left: Revision) -> Self {
        Self {
            back: back.expecting_nothing(),
            again: again.expecting_nothing(),
            left,
            turn: None,
        }
    }

    /// The tab armed a rule, and the control plane answered.
    pub fn armed(arm: ArmRule, answer: Revised) -> Self {
        let disarm = DisarmRule {
            environment: arm.environment.clone(),
            rule: arm.rule.clone(),
            expects: None,
        };
        Self::new(Change::DisarmRule(disarm), Change::ArmRule(arm), answer.revision)
    }

    /// The tab disarmed a rule, and the control plane answered.
    pub fn disarmed(disarm: DisarmRule, answer: Revised) -> Self {
        let arm = ArmRule {
            environment: disarm.environment.clone(),
            rule: disarm.rule.clone(),
            expects: None,
        };
        Self::new(Change::ArmRule(arm), Change::DisarmRule(disarm), answer.revision)
    }

    /// The tab made a live rule, and the control plane answered. ⌘K makes it armed, in one operation.
    pub fn created(create: CreateLiveRule, answer: Revised) -> Self {
        let delete = DeleteLiveRule {
            environment: create.environment.clone(),
            rule: create.rule.name().clone(),
            expects: None,
        };
        Self::new(
            Change::DeleteLiveRule(delete),
            Change::CreateLiveRule(create),
            answer.revision,
        )
    }

    /// The tab deleted the live rule that was `was`, and the control plane answered.
    ///
    /// The delete expected the revision of `was`, so a success says that the tab held the rule as it was.
    pub fn deleted(delete: DeleteLiveRule, was: &RuleStatus, answer: Revised) -> Self {
        let create = CreateLiveRule {
            environment: delete.environment.clone(),
            rule: was.rule.clone(),
            armed: was.state.is_armed(),
        };
        Self::new(
            Change::CreateLiveRule(create),
            Change::DeleteLiveRule(delete),
            answer.revision,
        )
    }

    /// The tab replaced the live rule `was`, and the control plane answered.
    ///
    /// The update expected the revision of `was`, so a success says that `was` is the rule before.
    pub fn updated_live(update: UpdateLiveRule, was: Rule, answer: Revised) -> Self {
        let back = UpdateLiveRule {
            environment: update.environment.clone(),
            rule: was,
            expects: None,
        };
        Self::new(
            Change::UpdateLiveRule(back),
            Change::UpdateLiveRule(update),
            answer.revision,
        )
    }

    /// The tab replaced the rule of the workspace `was`, and the control plane answered.
    ///
    /// The update expected the revision of `was`, so a success says that `was` is the rule before.
    pub fn updated_rule(update: UpdateDefinition<Rule>, was: Rule, answer: Revised) -> Self {
        let back = UpdateDefinition {
            expects: None,
            definition: was,
        };
        Self::new(Change::UpdateRule(back), Change::UpdateRule(update), answer.revision)
    }

    /// A press in latch of a pad of `environment` did what `effect` says.
    ///
    /// Returns `None` when the press changed nothing: it found the state it wants, and leaves nothing to undo.
    pub fn pressed(environment: &EnvironmentName, effect: &IntentEffect) -> Option<Self> {
        let status = effect.rule.as_ref().filter(|_| effect.changed)?;
        let rule = status.rule.name().clone();
        let environment = environment.clone();
        let arm = ArmRule {
            environment: environment.clone(),
            rule: rule.clone(),
            expects: None,
        };
        let disarm = DisarmRule {
            environment: environment.clone(),
            rule: rule.clone(),
            expects: None,
        };
        let (back, again) = if effect.made {
            let delete = DeleteLiveRule {
                environment: environment.clone(),
                rule,
                expects: None,
            };
            let create = CreateLiveRule {
                environment,
                rule: status.rule.clone(),
                armed: true,
            };
            (Change::DeleteLiveRule(delete), Change::CreateLiveRule(create))
        } else if status.state.is_armed() {
            (Change::DisarmRule(disarm), Change::ArmRule(arm))
        } else {
            (Change::ArmRule(arm), Change::DisarmRule(disarm))
        };
        Some(Self::new(back, again, status.revision))
    }

    /// A turn of `encoder` changed the live rule of `environment` that the tab showed as `was`, as `effect` says.
    ///
    /// The turns of one encoder on one rule, one after another, are one step: the history keeps the rule before the
    /// first of them. A turn expects nothing, so the answer says which revision of the rule it found:
    /// the [history](History::record) keeps the step only when that is the rule the tab showed,
    /// and joins it to the turn before only when it found what that turn left.
    /// Returns `None` when the turn changed nothing, because the parameter stood at its bound.
    pub fn turned(
        environment: &EnvironmentName,
        encoder: Encoder,
        was: &RuleStatus,
        effect: &IntentEffect,
    ) -> Option<Self> {
        let status = effect.rule.as_ref().filter(|_| effect.changed)?;
        let back = UpdateLiveRule {
            environment: environment.clone(),
            rule: was.rule.clone(),
            expects: None,
        };
        let again = UpdateLiveRule {
            environment: environment.clone(),
            rule: status.rule.clone(),
            expects: None,
        };
        Some(Self {
            turn: Some(Turn {
                encoder,
                found: effect.found,
                shown: was.revision,
            }),
            ..Self::new(
                Change::UpdateLiveRule(back),
                Change::UpdateLiveRule(again),
                status.revision,
            )
        })
    }

    /// What the step changed.
    pub fn subject(&self) -> Subject {
        self.again.subject()
    }

    /// What takes the step back, expecting nothing yet.
    pub fn back(&self) -> &Change {
        &self.back
    }

    /// What makes the step again, expecting nothing yet.
    pub fn again(&self) -> &Change {
        &self.again
    }
}

/// What came of an undo or a redo that the history sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It applied, and left `revision`.
    Applied { revision: Revision },
    /// Someone else changed the subject since the tab did: another tab, `gmq`, a step, a lifetime or a reload.
    /// It changed nothing.
    Changed,
    /// The process started again since the tab read it. It changed nothing.
    OtherRun,
    /// The control plane refused it for another reason, and it changed nothing: the environment does not run, the rule
    /// is gone, its name is taken, or it no longer fits the workspace. `reason` says which.
    Refused { reason: String },
    /// No answer came, so it may have applied or not. `reason` says what happened.
    Unanswered { reason: String },
}

impl From<RuleCommandError> for Outcome {
    fn from(error: RuleCommandError) -> Self {
        match error {
            RuleCommandError::Changed { .. } => Outcome::Changed,
            RuleCommandError::OtherRun { .. } => Outcome::OtherRun,
            refused => Outcome::Refused {
                reason: refused.to_string(),
            },
        }
    }
}

impl From<CreateLiveRuleError> for Outcome {
    fn from(error: CreateLiveRuleError) -> Self {
        Outcome::Refused {
            reason: error.to_string(),
        }
    }
}

impl From<UpdateLiveRuleError> for Outcome {
    fn from(error: UpdateLiveRuleError) -> Self {
        match error {
            UpdateLiveRuleError::Changed { .. } => Outcome::Changed,
            UpdateLiveRuleError::OtherRun { .. } => Outcome::OtherRun,
            refused => Outcome::Refused {
                reason: refused.to_string(),
            },
        }
    }
}

impl From<DeleteLiveRuleError> for Outcome {
    fn from(error: DeleteLiveRuleError) -> Self {
        match error {
            DeleteLiveRuleError::Changed { .. } => Outcome::Changed,
            DeleteLiveRuleError::OtherRun { .. } => Outcome::OtherRun,
            refused => Outcome::Refused {
                reason: refused.to_string(),
            },
        }
    }
}

impl From<UpdateDefinitionError> for Outcome {
    fn from(error: UpdateDefinitionError) -> Self {
        match error {
            UpdateDefinitionError::Changed { .. } => Outcome::Changed,
            UpdateDefinitionError::OtherRun { .. } => Outcome::OtherRun,
            refused => Outcome::Refused {
                reason: refused.to_string(),
            },
        }
    }
}

/// What the UI says about an undo or a redo once its answer came.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// What happened, by name.
    pub text: String,
    /// Whether it changed nothing.
    pub refused: bool,
}

/// An undo or a redo on its way, with the step it takes.
#[derive(Debug, Clone, PartialEq)]
struct Underway {
    step: Step,
    cause: RequestCause,
    /// Whether the tab made a new change since it sent the undo: the step then has nothing to redo.
    overtaken: bool,
}

/// The undo history of one tab, in memory: the changes that the tab made, oldest first, and the ones it took back.
///
/// ⌘Z takes back the last step, and ⇧⌘Z makes the last step that was taken back again.
/// The engine keeps no history: an undo is an ordinary operation, the inverse of the change, sent with the cause
/// [`undo`](RequestCause::Undo).
///
/// - **It holds the changes of this tab alone.** A change of another tab, of `gmq`, of a scenario or of a reload
///   never enters it. The tab [records](Self::record) a step once the control plane answered its change.
/// - **An undo expects what the tab left.** It names the run and the revision that the tab left on its subject last:
///   its change, or the undo or redo of a later step of the same subject.
///   When someone else changed the subject since, the control plane refuses with `changed`, and the step goes.
/// - **It holds as many steps as [`undo_depth`](crate::UiConfiguration::undo_depth) says.**
///   The oldest step goes when a new one would be one too many.
/// - **A new change ends what could be redone.**
/// - **It ends with the run.** A snapshot of another run, or an undo refused with `other-run`, drops every step.
/// - **A step goes with its environment.** When an environment stops, its steps go.
/// - **One undo or redo at a time.** While one is on its way, ⌘Z and ⇧⌘Z do nothing.
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_ui::history::{Change, History, Outcome, Step};
///
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().expect("a run ID");
/// let mut history = History::new(NonZeroUsize::new(100).expect("100 is no zero"));
/// history.apply(&StreamMessage::Snapshot(ProcessState {
///     run,
///     definitions: Definitions::default(),
///     workspace: WorkspaceStatus::default(),
///     environments: Vec::new(),
/// }));
///
/// // The tab armed a rule, and the arm left revision 8.
/// let arm = ArmRule {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     expects: Some(Expects { run, revision: Revision::new(7) }),
/// };
/// history.record(Step::armed(arm.clone(), Revised { revision: Revision::new(8) }));
///
/// // ⌘Z disarms it, expecting what the arm left.
/// let disarm = DisarmRule {
///     environment: arm.environment.clone(),
///     rule: arm.rule.clone(),
///     expects: Some(Expects { run, revision: Revision::new(8) }),
/// };
/// assert_eq!(history.undo(), Some(Change::DisarmRule(disarm)));
///
/// let report = history.settled(Outcome::Applied { revision: Revision::new(9) }).expect("an undo on its way");
/// assert_eq!(report.text, "Disarmed slow-order-producers again");
///
/// // ⇧⌘Z arms it again, expecting what the undo left.
/// let again = ArmRule { expects: Some(Expects { run, revision: Revision::new(9) }), ..arm };
/// assert_eq!(history.redo(), Some(Change::ArmRule(again)));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct History {
    depth: NonZeroUsize,
    /// The run of the process that the steps belong to, once a snapshot named it.
    run: Option<RunId>,
    /// The steps to undo, the oldest first.
    to_undo: VecDeque<Step>,
    /// The steps to redo, the last one taken back last.
    to_redo: Vec<Step>,
    /// The revision that the tab left last on each subject of a step.
    left: BTreeMap<Subject, Revision>,
    underway: Option<Underway>,
    /// The subject and the encoder that the next turn joins, while the last thing the tab did was that turn.
    turning: Option<(Subject, Encoder)>,
    /// The holds whose press made the live rule of its pad, until their release answers.
    making: BTreeSet<Hold>,
}

impl History {
    /// An empty history that keeps at most `depth` steps.
    pub fn new(depth: NonZeroUsize) -> Self {
        Self {
            depth,
            run: None,
            to_undo: VecDeque::new(),
            to_redo: Vec::new(),
            left: BTreeMap::new(),
            underway: None,
            turning: None,
            making: BTreeSet::new(),
        }
    }

    /// Whether ⌘Z has a step to take back now.
    pub fn can_undo(&self) -> bool {
        self.underway.is_none() && self.run.is_some() && !self.to_undo.is_empty()
    }

    /// Whether ⇧⌘Z has a step to make again now.
    pub fn can_redo(&self) -> bool {
        self.underway.is_none() && self.run.is_some() && !self.to_redo.is_empty()
    }

    /// Follows one message of the live stream: a snapshot of another run drops every step, and an environment that
    /// stops takes its steps with it. Every other message changes nothing here.
    pub fn apply(&mut self, message: &StreamMessage) {
        match message {
            StreamMessage::Snapshot(state) => {
                if self.run.is_some_and(|run| run != state.run) {
                    self.forget();
                }
                self.run = Some(state.run);
            }
            StreamMessage::EnvironmentStopped { environment } => {
                let stays = |step: &Step| step.subject().environment() != Some(environment);
                self.to_undo.retain(stays);
                self.to_redo.retain(stays);
                if self
                    .turning
                    .as_ref()
                    .is_some_and(|(subject, _)| subject.environment() == Some(environment))
                {
                    self.turning = None;
                }
                self.prune();
            }
            _ => {}
        }
    }

    /// The tab made a change, and the control plane answered: `step` takes it back.
    ///
    /// A turn that follows a turn of the same encoder on the same rule joins its step, when it found the rule as that
    /// turn left it. Any other turn makes a step of its own, when it found the rule that the tab showed.
    /// A turn that found neither leaves no step: someone else changed the rule just before it, and setting the rule
    /// back would take their change too. An earlier step of the rule then expects a revision that is gone,
    /// so its undo changes nothing either.
    pub fn record(&mut self, step: Step) {
        let (subject, left) = (step.subject(), step.left);
        self.ended_redo();
        let joins = step.turn.is_some_and(|turn| {
            self.turning.as_ref() == Some(&(subject.clone(), turn.encoder))
                && turn.found.is_some()
                && turn.found == self.left.get(&subject).copied()
        });
        match self.to_undo.back_mut() {
            Some(last) if joins => last.again = step.again,
            _ if step.turn.is_some_and(|turn| turn.found != Some(turn.shown)) => {
                self.turning = None;
                return;
            }
            _ => {
                self.turning = step.turn.map(|turn| (subject.clone(), turn.encoder));
                self.push(step);
            }
        }
        self.left_on(&subject, left);
    }

    /// The tab made a change that leaves no step, such as keeping a live rule, and it left `revision` on `subject`.
    ///
    /// It ends what could be redone, and an undo of an earlier step of `subject` expects it.
    pub fn changed(&mut self, subject: &Subject, revision: Revision) {
        self.left_on(subject, revision);
        self.turning = None;
        self.ended_redo();
    }

    /// A press in hold did what `effect` says.
    ///
    /// A hold leaves a step only when its press made the live rule of the pad, and the step waits for the
    /// [release](Self::released): the press and its release are one step, which deletes the rule.
    pub fn held(&mut self, effect: &IntentEffect) {
        if effect.made
            && let Some(hold) = effect.hold
        {
            self.making.insert(hold);
        }
    }

    /// The release of `hold` in `environment` did what `effect` says.
    ///
    /// When the press of the hold made the rule, and the release disarmed it, the two are one step.
    /// A release that changed nothing leaves no step: someone else disarmed the rule since,
    /// or still holds its pad.
    pub fn released(&mut self, environment: &EnvironmentName, hold: Hold, effect: &IntentEffect) {
        if !self.making.remove(&hold) {
            return;
        }
        let Some(status) = effect.rule.as_ref().filter(|_| effect.changed) else {
            return;
        };
        let delete = DeleteLiveRule {
            environment: environment.clone(),
            rule: status.rule.name().clone(),
            expects: None,
        };
        let create = CreateLiveRule {
            environment: environment.clone(),
            rule: status.rule.clone(),
            armed: false,
        };
        self.record(Step::new(
            Change::DeleteLiveRule(delete),
            Change::CreateLiveRule(create),
            status.revision,
        ));
    }

    /// Every hold of the tab ended with its presence, and no release of them answers any more.
    pub fn holds_ended(&mut self) {
        self.making.clear();
    }

    /// ⌘Z: the operation that takes the last step back, expecting what the tab left on its subject.
    ///
    /// `None` when there is nothing to undo, or while an undo or a redo is on its way.
    /// The answer goes to [`settled`](Self::settled).
    pub fn undo(&mut self) -> Option<Change> {
        if !self.can_undo() {
            return None;
        }
        let step = self.to_undo.pop_back()?;
        Some(self.send(step, RequestCause::Undo))
    }

    /// ⇧⌘Z: the operation that makes the last step that was taken back again, expecting what its undo left.
    ///
    /// `None` when there is nothing to redo, or while an undo or a redo is on its way.
    /// The answer goes to [`settled`](Self::settled).
    pub fn redo(&mut self) -> Option<Change> {
        if !self.can_redo() {
            return None;
        }
        let step = self.to_redo.pop()?;
        Some(self.send(step, RequestCause::Redo))
    }

    /// The answer to the undo or the redo on its way came: what the UI says about it, or `None` when none was on its
    /// way.
    ///
    /// | Outcome | The step |
    /// |---|---|
    /// | Applied | Moves: an undo can be redone, and a redo undone. |
    /// | Changed, Refused | Goes. The steps before it stay, and the next ⌘Z takes the next one. |
    /// | Other run | Goes, with every other step. |
    /// | Unanswered | Stays where it was, so a ⌘Z or ⇧⌘Z can send it again. A second one that finds it applied is refused. |
    pub fn settled(&mut self, outcome: Outcome) -> Option<Report> {
        let Underway { step, cause, overtaken } = self.underway.take()?;
        let change = match cause {
            RequestCause::Undo => &step.back,
            RequestCause::Redo => &step.again,
        };
        let subject = change.subject();
        let (nothing, what) = match cause {
            RequestCause::Undo => ("nothing was undone", "undo"),
            RequestCause::Redo => ("nothing was redone", "redo"),
        };
        let report = match outcome {
            Outcome::Applied { revision } => {
                let text = change.done(cause);
                self.left_on(&subject, revision);
                match cause {
                    RequestCause::Undo if !overtaken => self.to_redo.push(step),
                    RequestCause::Undo => {}
                    RequestCause::Redo => self.push(step),
                }
                Report { text, refused: false }
            }
            Outcome::Changed => Report {
                text: format!("{subject} changed since, so {nothing}"),
                refused: true,
            },
            Outcome::OtherRun => {
                self.forget();
                Report {
                    text: format!("The process started again since, so {nothing}, and the history of this tab is gone"),
                    refused: true,
                }
            }
            Outcome::Refused { reason } => Report {
                text: format!("The {what} was refused, so {nothing}: {reason}"),
                refused: true,
            },
            Outcome::Unanswered { reason } => {
                match cause {
                    RequestCause::Undo if !overtaken => self.to_undo.push_back(step),
                    RequestCause::Undo => {}
                    RequestCause::Redo => self.to_redo.push(step),
                }
                Report {
                    text: format!("The {what} got no answer, and may have applied or not: {reason}"),
                    refused: true,
                }
            }
        };
        self.prune();
        Some(report)
    }

    /// Sends `step` back or again: it is on its way until its answer [settles](Self::settled) it.
    fn send(&mut self, step: Step, cause: RequestCause) -> Change {
        self.turning = None;
        let subject = step.subject();
        let revision = self.left.get(&subject).copied().unwrap_or(step.left);
        let change = match cause {
            RequestCause::Undo => step.back.clone(),
            RequestCause::Redo => step.again.clone(),
        };
        self.underway = Some(Underway {
            step,
            cause,
            overtaken: false,
        });
        match self.run {
            Some(run) => change.expecting(Expects { run, revision }),
            None => change,
        }
    }

    /// Adds `step` as the last step to undo, and lets the oldest go when the history holds too many.
    fn push(&mut self, step: Step) {
        self.to_undo.push_back(step);
        while self.to_undo.len() > self.depth.get() {
            self.to_undo.pop_front();
        }
        self.prune();
    }

    /// The tab left `revision` on `subject`, and on every subject that a change of `subject` changes too.
    fn left_on(&mut self, subject: &Subject, revision: Revision) {
        for (held, at) in &mut self.left {
            if subject.covers(held) {
                *at = revision;
            }
        }
        self.left.insert(subject.clone(), revision);
    }

    /// A new change of the tab: nothing can be redone, and an undo on its way has nothing to redo either.
    fn ended_redo(&mut self) {
        self.to_redo.clear();
        if let Some(underway) = &mut self.underway
            && underway.cause == RequestCause::Undo
        {
            underway.overtaken = true;
        }
        self.prune();
    }

    /// Drops every step: they belong to a run that has ended.
    fn forget(&mut self) {
        self.to_undo.clear();
        self.to_redo.clear();
        self.left.clear();
        self.underway = None;
        self.turning = None;
        self.making.clear();
    }

    /// Keeps the revisions of the subjects that a step still changes, and of none other.
    fn prune(&mut self) {
        let subjects: BTreeSet<Subject> = self
            .to_undo
            .iter()
            .chain(&self.to_redo)
            .chain(self.underway.as_ref().map(|underway| &underway.step))
            .map(Step::subject)
            .collect();
        self.left.retain(|subject, _| subjects.contains(subject));
    }
}
