//! The undo history of a tab: ⌘Z takes back the last change that the tab made, ⇧⌘Z makes it again, and an undo
//! that would overwrite what someone else changed is refused.
//!
//! The tests play each tab as the UI does: a [`Live`] applies what the live stream sends, and its [`History`]
//! records the answers to the changes of the tab. What the control plane answers is a plain value.
//! No browser and no server take part.
//!
//! [`History`]: gaslightmq_ui::history::History

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::Endpoint;
use gaslightmq_control_api::incident::IncidentSupport;
use gaslightmq_control_api::kernel::BrokerId;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::SelectorName;
use gaslightmq_control_api::surface::{Encoder, Hold};
use gaslightmq_control_api::trigger::{Chance, Trigger, TriggerParts};
use gaslightmq_ui::history::{Change, Outcome, Report, Step, Subject};
use gaslightmq_ui::{Live, UiConfiguration};

/// The run of the process, as its snapshot names it.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// The run of the process after it started again.
const NEXT_RUN: &str = "9f86d081884c7d659a2feaa0c55ad015";

/// The revision that the snapshot tells beside the rule.
const READ: Revision = Revision::new(7);

/// The environment of the tests, as the workspace and the control plane describe it.
struct OrdersSim;

impl OrdersSim {
    fn name() -> EnvironmentName {
        "orders-sim".parse().unwrap()
    }

    /// A proxy in front of a real cluster, with a port pinned for its one broker.
    fn definition() -> Environment {
        let brokers = BrokerListeners::pinned([(BrokerId::from(1), "0.0.0.0:19092".parse().unwrap())]).unwrap();
        let listen = Listeners::new("0.0.0.0:9092".parse().unwrap(), brokers, "localhost".parse().unwrap()).unwrap();
        Environment::new(
            Self::name(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listen),
                upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
            },
        )
    }

    /// The snapshot of a process of `run` in which the environment runs with `rules`.
    fn snapshot(run: &str, rules: Vec<RuleStatus>) -> StreamMessage {
        StreamMessage::Snapshot(ProcessState {
            run: run.parse().unwrap(),
            definitions: Definitions {
                environments: vec![DefinitionStatus {
                    definition: Self::definition(),
                    revision: READ,
                }],
                ..Definitions::default()
            },
            environments: vec![RunningEnvironment {
                environment: Self::name(),
                backend: BackendStatus {
                    kind: BackendKind::Upstream,
                    endpoints: BTreeMap::from([(Endpoint::Bootstrap, Some("127.0.0.1:9092".parse().unwrap()))]),
                    incidents: IncidentSupport::none(),
                },
                rules,
                sessions: Vec::new(),
            }],
        })
    }
}

/// The rule `slow-order-producers` of the specs: the selector `order-producers`, and the preset `delay-1s`.
fn slow_order_producers() -> Rule {
    Rule::new(
        rule_name(),
        "order-producers".parse::<SelectorName>().unwrap(),
        "delay-1s".parse::<PresetName>().unwrap(),
    )
}

fn rule_name() -> RuleName {
    "slow-order-producers".parse().unwrap()
}

/// What a change of the rule tells every tab: the rule in the state and at the revision it has now.
fn rule_changed(state: RuleState, revision: Revision, live: bool) -> StreamMessage {
    StreamMessage::RuleChanged {
        environment: OrdersSim::name(),
        rule: RuleStatus {
            rule: slow_order_producers(),
            live,
            state,
            revision,
        },
    }
}

/// The rule as a tab shows it.
fn shown(tab: &Live) -> &RuleStatus {
    gaslightmq_ui::rule_status(tab.shown_running().unwrap(), &rule_name()).unwrap()
}

/// The rule of the tests in the environment of the tests.
fn the_rule() -> Subject {
    Subject::rule(&OrdersSim::name(), &rule_name())
}

/// What a change of the tests expects: revision `revision` of `run`.
fn seen(run: &str, revision: u64) -> Expects {
    Expects {
        run: run.parse().unwrap(),
        revision: Revision::new(revision),
    }
}

/// A tab of the UI with its live stream open, which read `snapshot`.
fn tab_that_read(snapshot: StreamMessage) -> Live {
    let mut tab = Live::new(&UiConfiguration::default());
    tab.opened();
    tab.apply(snapshot);
    tab
}

/// What a change answers: the revision that it left.
fn left(revision: u64) -> Revised {
    Revised {
        revision: Revision::new(revision),
    }
}

/// What the control plane answers to an undo or a redo that applied.
fn applied(revision: u64) -> Outcome {
    Outcome::Applied {
        revision: Revision::new(revision),
    }
}

/// What a pad intent did to the rule of its pad: the rule as it is now, whether the intent made it, and whether it
/// changed it.
fn effect(state: RuleState, revision: u64, made: bool, changed: bool) -> IntentEffect {
    IntentEffect {
        rule: Some(RuleStatus {
            rule: slow_order_producers(),
            live: true,
            state,
            revision: Revision::new(revision),
        }),
        made,
        changed,
        hold: None,
    }
}

fn arm(expects: Option<Expects>) -> ArmRule {
    ArmRule {
        environment: OrdersSim::name(),
        rule: rule_name(),
        expects,
    }
}

fn disarm(expects: Option<Expects>) -> DisarmRule {
    DisarmRule {
        environment: OrdersSim::name(),
        rule: rule_name(),
        expects,
    }
}

fn delete(expects: Option<Expects>) -> DeleteLiveRule {
    DeleteLiveRule {
        environment: OrdersSim::name(),
        rule: rule_name(),
        expects,
    }
}

fn disarmed_rule() -> RuleStatus {
    RuleStatus {
        rule: slow_order_producers(),
        live: false,
        state: RuleState::Disarmed,
        revision: READ,
    }
}

#[test]
fn an_undo_after_another_tab_changed_the_rule_is_refused_and_changes_nothing() {
    let snapshot = OrdersSim::snapshot(RUN, vec![disarmed_rule()]);
    let mut first = tab_that_read(snapshot.clone());
    let mut second = tab_that_read(snapshot);

    // The first tab arms the rule, and the arm leaves revision 8, which both tabs read.
    let arm = ArmRule {
        environment: OrdersSim::name(),
        rule: rule_name(),
        expects: first.expects(&the_rule()),
    };
    assert_eq!(arm.expects, Some(seen(RUN, 7)), "a change expects what the tab shows");
    first.history_mut().record(Step::armed(
        arm,
        Revised {
            revision: Revision::new(8),
        },
    ));
    first.apply(rule_changed(RuleState::Armed, Revision::new(8), false));
    second.apply(rule_changed(RuleState::Armed, Revision::new(8), false));

    // The second tab disarms it again, and the disarm leaves revision 9.
    let disarm = DisarmRule {
        environment: OrdersSim::name(),
        rule: rule_name(),
        expects: second.expects(&the_rule()),
    };
    second.history_mut().record(Step::disarmed(
        disarm,
        Revised {
            revision: Revision::new(9),
        },
    ));
    first.apply(rule_changed(RuleState::Disarmed, Revision::new(9), false));
    second.apply(rule_changed(RuleState::Disarmed, Revision::new(9), false));

    // ⌘Z in the first tab takes back its own arm, and expects what that arm left.
    let undo = first.history_mut().undo().unwrap();
    assert_eq!(
        undo,
        Change::DisarmRule(DisarmRule {
            environment: OrdersSim::name(),
            rule: rule_name(),
            expects: Some(seen(RUN, 8)),
        }),
    );

    // The rule has revision 9 now, so the control plane refuses, and changes nothing.
    let refused = RuleCommandError::Changed {
        environment: OrdersSim::name(),
        rule: rule_name(),
        revision: Revision::new(9),
    };
    let report = first.history_mut().settled(Outcome::from(refused)).unwrap();

    assert_eq!(
        report,
        Report {
            text: "slow-order-producers changed since, so nothing was undone".to_owned(),
            refused: true,
        },
    );
    assert!(!first.history().can_undo(), "the step that did not fit is gone");
    assert!(!first.history().can_redo(), "a refused undo leaves nothing to redo");
    assert_eq!(
        shown(&first).state,
        RuleState::Disarmed,
        "the rule stays as the second tab left it"
    );
    assert_eq!(shown(&first).revision, Revision::new(9));
    assert!(second.history().can_undo(), "the second tab keeps its own step");
}

#[test]
fn an_undo_says_what_it_did_and_a_redo_expects_what_the_undo_left() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut()
        .record(Step::disarmed(disarm(Some(seen(RUN, 7))), left(8)));

    assert_eq!(tab.history_mut().undo(), Some(Change::ArmRule(arm(Some(seen(RUN, 8))))));
    let report = tab.history_mut().settled(applied(9)).unwrap();
    assert_eq!(
        report.text, "Armed slow-order-producers again",
        "an undo that arms a fault says so"
    );
    assert!(!report.refused);

    assert_eq!(
        tab.history_mut().redo(),
        Some(Change::DisarmRule(disarm(Some(seen(RUN, 9)))))
    );
    tab.history_mut().settled(applied(10)).unwrap();
    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::ArmRule(arm(Some(seen(RUN, 10)))))
    );
}

#[test]
fn two_changes_of_one_rule_undo_one_after_the_other() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    let sometimes = slow_order_producers().with_trigger(Trigger::from(
        TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
    ));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    let update = UpdateLiveRule {
        environment: OrdersSim::name(),
        rule: sometimes,
        expects: Some(seen(RUN, 8)),
    };
    tab.history_mut()
        .record(Step::updated_live(update, slow_order_producers(), left(9)));

    // The undo of the update sets the rule back to what it was before.
    let back = UpdateLiveRule {
        environment: OrdersSim::name(),
        rule: slow_order_producers(),
        expects: Some(seen(RUN, 9)),
    };
    assert_eq!(tab.history_mut().undo(), Some(Change::UpdateLiveRule(back)));
    tab.history_mut().settled(applied(10)).unwrap();

    // The undo of the arm expects what the undo of the update left, which the tab did itself.
    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::DisarmRule(disarm(Some(seen(RUN, 10)))))
    );
}

#[test]
fn a_rule_that_comes_back_is_made_again_armed_when_it_was_armed() {
    let armed = RuleStatus {
        live: true,
        state: RuleState::Armed,
        ..disarmed_rule()
    };
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![armed.clone()]));
    tab.history_mut()
        .record(Step::deleted(delete(Some(seen(RUN, 7))), &armed, left(8)));

    let create = CreateLiveRule {
        environment: OrdersSim::name(),
        rule: slow_order_producers(),
        armed: true,
    };
    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::CreateLiveRule(create)),
        "a create expects nothing"
    );
    let report = tab.history_mut().settled(applied(9)).unwrap();
    assert_eq!(report.text, "Armed the live rule slow-order-producers again");

    // The rule that came back is a new one: the redo deletes it, expecting what the create left.
    assert_eq!(
        tab.history_mut().redo(),
        Some(Change::DeleteLiveRule(delete(Some(seen(RUN, 9)))))
    );
}

#[test]
fn a_press_that_changed_nothing_leaves_no_step() {
    let found_armed = effect(RuleState::Armed, 7, false, false);

    assert_eq!(Step::pressed(&OrdersSim::name(), &found_armed), None);
}

#[test]
fn a_press_that_made_the_rule_of_its_pad_is_undone_by_deleting_it() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, Vec::new()));
    let made = effect(RuleState::Armed, 8, true, true);
    tab.history_mut()
        .record(Step::pressed(&OrdersSim::name(), &made).unwrap());

    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::DeleteLiveRule(delete(Some(seen(RUN, 8)))))
    );
    tab.history_mut().settled(applied(9)).unwrap();

    // The redo makes the rule of the pad again, armed, as the press left it.
    let create = CreateLiveRule {
        environment: OrdersSim::name(),
        rule: slow_order_producers(),
        armed: true,
    };
    assert_eq!(tab.history_mut().redo(), Some(Change::CreateLiveRule(create)));
}

#[test]
fn a_hold_leaves_a_step_only_when_its_press_made_the_rule() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, Vec::new()));
    let (making, arming) = (Hold::new(1), Hold::new(2));

    // A hold of a pad whose rule exists arms it while held, and its release disarms it: nothing is left to undo.
    tab.history_mut().held(&IntentEffect {
        hold: Some(arming),
        ..effect(RuleState::Armed, 8, false, true)
    });
    tab.history_mut()
        .released(&OrdersSim::name(), arming, &effect(RuleState::Disarmed, 9, false, true));
    assert!(!tab.history().can_undo());

    // A hold that made the rule is one step with its release, and its undo deletes the rule.
    tab.history_mut().held(&IntentEffect {
        hold: Some(making),
        ..effect(RuleState::Armed, 10, true, true)
    });
    assert!(!tab.history().can_undo(), "the step waits for the release");
    tab.history_mut().released(
        &OrdersSim::name(),
        making,
        &effect(RuleState::Disarmed, 11, false, true),
    );
    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::DeleteLiveRule(delete(Some(seen(RUN, 11)))))
    );
}

#[test]
fn turns_of_one_encoder_are_one_step_back_to_the_rule_before_the_first() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, Vec::new()));
    let with_chance = |percent| {
        slow_order_producers().with_trigger(Trigger::from(
            TriggerParts::new().with_chance(Chance::from_percent(percent).unwrap()),
        ))
    };
    let turned_to = |percent, revision| IntentEffect {
        rule: Some(RuleStatus {
            rule: with_chance(percent),
            live: true,
            state: RuleState::Armed,
            revision: Revision::new(revision),
        }),
        made: false,
        changed: true,
        hold: None,
    };
    let environment = OrdersSim::name();
    for (was, now, revision) in [(100, 90, 8), (90, 80, 9), (80, 70, 10)] {
        let step = Step::turned(
            &environment,
            Encoder::Probability,
            &with_chance(was),
            &turned_to(now, revision),
        );
        tab.history_mut().record(step.unwrap());
    }
    // A turn of another encoder is a step of its own.
    let delay = Step::turned(&environment, Encoder::Delay, &with_chance(70), &turned_to(70, 11));
    tab.history_mut().record(delay.unwrap());

    tab.history_mut().undo().unwrap();
    tab.history_mut().settled(applied(12)).unwrap();
    let back = UpdateLiveRule {
        environment: OrdersSim::name(),
        rule: with_chance(100),
        expects: Some(seen(RUN, 12)),
    };
    assert_eq!(tab.history_mut().undo(), Some(Change::UpdateLiveRule(back)));
    tab.history_mut().settled(applied(13)).unwrap();
    assert!(!tab.history().can_undo(), "the three turns were one step");
}

#[test]
fn a_new_change_ends_what_could_be_redone() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    tab.history_mut().undo().unwrap();
    tab.history_mut().settled(applied(9)).unwrap();
    assert!(tab.history().can_redo());

    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 9))), left(10)));

    assert!(!tab.history().can_redo());
}

#[test]
fn the_history_holds_as_many_steps_as_its_depth_says() {
    let mut configuration = UiConfiguration::default();
    configuration.undo_depth = NonZeroUsize::new(2).unwrap();
    let mut tab = Live::new(&configuration);
    tab.opened();
    tab.apply(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    tab.history_mut()
        .record(Step::disarmed(disarm(Some(seen(RUN, 8))), left(9)));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 9))), left(10)));

    for revision in [11, 12] {
        tab.history_mut().undo().unwrap();
        tab.history_mut().settled(applied(revision)).unwrap();
    }

    assert!(!tab.history().can_undo(), "the oldest step went when the third came");
}

#[test]
fn the_history_lasts_across_a_stream_that_opens_again_and_ends_with_the_run() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));

    tab.lost("the connection dropped".to_owned());
    tab.opened();
    tab.apply(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    assert!(tab.history().can_undo(), "a stream of the same run keeps the steps");

    tab.lost("the process stopped".to_owned());
    tab.opened();
    tab.apply(OrdersSim::snapshot(NEXT_RUN, vec![disarmed_rule()]));
    assert!(
        !tab.history().can_undo(),
        "nothing of an earlier run fits a process that started again"
    );
}

#[test]
fn an_undo_refused_with_another_run_drops_every_step() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    tab.history_mut()
        .record(Step::disarmed(disarm(Some(seen(RUN, 8))), left(9)));

    tab.history_mut().undo().unwrap();
    let refused = RuleCommandError::OtherRun {
        run: NEXT_RUN.parse().unwrap(),
    };
    let report = tab.history_mut().settled(Outcome::from(refused)).unwrap();

    assert!(report.refused);
    assert!(!tab.history().can_undo());
}

#[test]
fn a_step_goes_with_its_environment() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    let sometimes = slow_order_producers().with_trigger(Trigger::from(
        TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
    ));
    let update = UpdateDefinition {
        expects: Some(seen(RUN, 7)),
        definition: sometimes,
    };
    tab.history_mut()
        .record(Step::updated_rule(update, slow_order_producers(), left(8)));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 8))), left(9)));

    tab.apply(StreamMessage::EnvironmentStopped {
        environment: OrdersSim::name(),
    });

    // The arm went with the environment, and the change of the rule of the workspace stays.
    let back = UpdateDefinition {
        expects: Some(seen(RUN, 8)),
        definition: slow_order_producers(),
    };
    assert_eq!(tab.history_mut().undo(), Some(Change::UpdateRule(back)));
}

#[test]
fn a_change_of_a_rule_of_the_workspace_is_a_change_of_it_in_every_environment() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    let sometimes = slow_order_producers().with_trigger(Trigger::from(
        TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
    ));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    let update = UpdateDefinition {
        expects: Some(seen(RUN, 7)),
        definition: sometimes,
    };
    tab.history_mut()
        .record(Step::updated_rule(update, slow_order_producers(), left(9)));

    tab.history_mut().undo().unwrap();
    tab.history_mut().settled(applied(10)).unwrap();

    // Setting the definition back left revision 10 on the rule in the environment too.
    assert_eq!(
        tab.history_mut().undo(),
        Some(Change::DisarmRule(disarm(Some(seen(RUN, 10)))))
    );
}

#[test]
fn an_undo_that_got_no_answer_stays_and_one_goes_out_at_a_time() {
    let mut tab = tab_that_read(OrdersSim::snapshot(RUN, vec![disarmed_rule()]));
    tab.history_mut().record(Step::armed(arm(Some(seen(RUN, 7))), left(8)));
    tab.history_mut()
        .record(Step::disarmed(disarm(Some(seen(RUN, 8))), left(9)));

    let undo = tab.history_mut().undo().unwrap();
    assert_eq!(
        tab.history_mut().undo(),
        None,
        "a second ⌘Z waits for the answer to the first"
    );
    let unanswered = Outcome::Unanswered {
        reason: "the connection dropped".to_owned(),
    };
    assert!(tab.history_mut().settled(unanswered).unwrap().refused);

    assert_eq!(
        tab.history_mut().undo(),
        Some(undo),
        "the step that may not have applied goes out again"
    );
}
