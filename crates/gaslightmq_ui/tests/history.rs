//! The undo history of a tab: ⌘Z takes back the last change that the tab made, ⇧⌘Z makes it again, and an undo
//! that would overwrite what someone else changed is refused.
//!
//! A tab is a [`Live`], which applies what the live stream sends and keeps the [`History`] of the tab.
//! The history records the answers to the changes of the tab. What the control plane answers is a plain value.
//! No browser and no server take part.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::incident::IncidentSupport;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::SelectorName;
use gaslightmq_control_api::surface::{Encoder, Hold};
use gaslightmq_control_api::trigger::{Chance, Trigger, TriggerParts};
use gaslightmq_ui::history::{Change, History, Outcome, Report, Step, Subject};
use gaslightmq_ui::{Live, UiConfiguration, rule_status};

/// The revision that the start of the environment took: the last change of its scenario while none is active.
const STARTED: Revision = Revision::new(1);

/// The environment that the tabs show.
const ENVIRONMENT: &str = "orders-sim";

/// The rule that the tabs change: the delay of 1 s on the order producers.
const RULE: &str = "slow-order-producers";

/// The selector of the rule.
const SELECTOR: &str = "order-producers";

/// The preset of the rule.
const PRESET: &str = "delay-1s";

/// The run of the process, as its snapshot names it.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// The run of the process after it started again.
const NEXT_RUN: &str = "9f86d081884c7d659a2feaa0c55ad015";

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one story of two tabs, from the snapshot they read to the undo that is refused"
)]
fn an_undo_after_another_tab_changed_the_rule_is_refused_and_changes_nothing() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    const LEFT_BY_THE_OTHER_TAB: Revision = Revision::new(9);
    let environment: EnvironmentName = ENVIRONMENT.parse().unwrap();
    let rule: RuleName = RULE.parse().unwrap();
    let run: RunId = RUN.parse().unwrap();
    let disarmed = RuleStatus {
        rule: Rule::new(
            rule.clone(),
            SELECTOR.parse::<SelectorName>().unwrap(),
            PRESET.parse::<PresetName>().unwrap(),
        ),
        live: false,
        state: RuleState::Disarmed,
        revision: SHOWN,
    };
    let snapshot = StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: vec![RunningEnvironment {
            environment: environment.clone(),
            backend: BackendStatus {
                kind: BackendKind::Upstream,
                endpoints: BTreeMap::new(),
                incidents: IncidentSupport::none(),
            },
            rules: vec![disarmed.clone()],
            sessions: Vec::new(),
            // No scenario is active, and the start of the environment was the last change of its scenario.
            scenario: ScenarioStatus {
                active: None,
                revision: STARTED,
            },
            recording: None,
            capturing: false,
        }],
        workspace: WorkspaceStatus::default(),
    });
    let configuration = UiConfiguration::default();
    let mut first = Live::new(&configuration);
    first.opened();
    first.apply(snapshot.clone());
    let mut second = Live::new(&configuration);
    second.opened();
    second.apply(snapshot);
    let the_rule = Subject::rule(&environment, &rule);

    // The first tab arms the rule, expecting what it shows, and both tabs read what the arm left.
    let arm = ArmRule {
        environment: environment.clone(),
        rule: rule.clone(),
        expects: first.expects(&the_rule),
    };
    assert_eq!(arm.expects, Some(Expects { run, revision: SHOWN }));
    first.history_mut().record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));
    let armed = StreamMessage::RuleChanged {
        environment: environment.clone(),
        rule: RuleStatus {
            state: RuleState::Armed,
            revision: LEFT_BY_THE_ARM,
            ..disarmed.clone()
        },
    };
    first.apply(armed.clone());
    second.apply(armed);

    // The second tab disarms it again, expecting what it shows: what the arm left.
    let disarm = DisarmRule {
        environment: environment.clone(),
        rule: rule.clone(),
        expects: second.expects(&the_rule),
    };
    assert_eq!(
        disarm.expects,
        Some(Expects {
            run,
            revision: LEFT_BY_THE_ARM,
        })
    );
    second.history_mut().record(Step::disarmed(
        disarm,
        Revised {
            revision: LEFT_BY_THE_OTHER_TAB,
        },
    ));
    let disarmed_again = StreamMessage::RuleChanged {
        environment: environment.clone(),
        rule: RuleStatus {
            revision: LEFT_BY_THE_OTHER_TAB,
            ..disarmed
        },
    };
    first.apply(disarmed_again.clone());
    second.apply(disarmed_again);

    // ⌘Z in the first tab takes back its own arm, and expects what that arm left.
    let undo = first.history_mut().undo().unwrap();
    assert_eq!(
        undo,
        Change::DisarmRule(DisarmRule {
            environment: environment.clone(),
            rule: rule.clone(),
            expects: Some(Expects {
                run,
                revision: LEFT_BY_THE_ARM,
            }),
        }),
    );

    // The other tab changed the rule since, so the control plane refuses, and changes nothing.
    let refused = RuleCommandError::Changed {
        environment,
        rule: rule.clone(),
        revision: LEFT_BY_THE_OTHER_TAB,
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
    let shown = rule_status(first.shown_running().unwrap(), &rule).unwrap();
    assert_eq!(
        (shown.state, shown.revision),
        (RuleState::Disarmed, LEFT_BY_THE_OTHER_TAB),
        "the rule stays as the other tab left it"
    );
    assert!(second.history().can_undo(), "the other tab keeps its own step");
}

#[test]
fn an_undo_says_what_it_did_and_a_redo_expects_what_the_undo_left() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_DISARM: Revision = Revision::new(8);
    const LEFT_BY_THE_UNDO: Revision = Revision::new(9);
    const LEFT_BY_THE_REDO: Revision = Revision::new(10);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let disarm = DisarmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::disarmed(
        disarm.clone(),
        Revised {
            revision: LEFT_BY_THE_DISARM,
        },
    ));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_DISARM,
        }),
    };

    assert_eq!(history.undo(), Some(Change::ArmRule(arm.clone())));
    let report = history
        .settled(Outcome::Applied {
            revision: LEFT_BY_THE_UNDO,
        })
        .unwrap();
    assert_eq!(
        report,
        Report {
            text: "Armed slow-order-producers again".to_owned(),
            refused: false,
        },
        "an undo that arms a fault says so"
    );

    let redo = DisarmRule {
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UNDO,
        }),
        ..disarm
    };
    assert_eq!(history.redo(), Some(Change::DisarmRule(redo)));
    history.settled(Outcome::Applied {
        revision: LEFT_BY_THE_REDO,
    });
    let undo_again = ArmRule {
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_REDO,
        }),
        ..arm
    };
    assert_eq!(history.undo(), Some(Change::ArmRule(undo_again)));
}

#[test]
fn two_changes_of_one_rule_undo_one_after_the_other() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    const LEFT_BY_THE_UPDATE: Revision = Revision::new(9);
    const LEFT_BY_ITS_UNDO: Revision = Revision::new(10);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let always = Rule::new(
        RULE.parse().unwrap(),
        SELECTOR.parse::<SelectorName>().unwrap(),
        PRESET.parse::<PresetName>().unwrap(),
    );
    let sometimes = always.clone().with_trigger(Trigger::from(
        TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
    ));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));
    let update = UpdateLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: sometimes,
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_ARM,
        }),
    };
    history.record(Step::updated_live(
        update,
        always.clone(),
        Revised {
            revision: LEFT_BY_THE_UPDATE,
        },
    ));

    // The undo of the update sets the rule back to what it was before.
    let back = UpdateLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: always,
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UPDATE,
        }),
    };
    assert_eq!(history.undo(), Some(Change::UpdateLiveRule(back)));
    history.settled(Outcome::Applied {
        revision: LEFT_BY_ITS_UNDO,
    });

    // The undo of the arm expects what the undo of the update left: the tab did that itself.
    let disarm = DisarmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_ITS_UNDO,
        }),
    };
    assert_eq!(history.undo(), Some(Change::DisarmRule(disarm)));
}

#[test]
fn a_rule_that_comes_back_is_made_again_armed_when_it_was_armed() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_DELETE: Revision = Revision::new(8);
    const LEFT_BY_THE_CREATE: Revision = Revision::new(9);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let definition = Rule::new(
        RULE.parse().unwrap(),
        SELECTOR.parse::<SelectorName>().unwrap(),
        PRESET.parse::<PresetName>().unwrap(),
    );
    let armed = RuleStatus {
        rule: definition.clone(),
        live: true,
        state: RuleState::Armed,
        revision: SHOWN,
    };
    let delete = DeleteLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::deleted(
        delete.clone(),
        &armed,
        Revised {
            revision: LEFT_BY_THE_DELETE,
        },
    ));

    let create = CreateLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: definition,
        armed: true,
    };
    assert_eq!(
        history.undo(),
        Some(Change::CreateLiveRule(create)),
        "a create expects nothing: it refuses a name that is taken"
    );
    let report = history
        .settled(Outcome::Applied {
            revision: LEFT_BY_THE_CREATE,
        })
        .unwrap();
    assert_eq!(report.text, "Armed the live rule slow-order-producers again");

    // The rule that came back is a new one: the redo deletes it, expecting what the create left.
    let redo = DeleteLiveRule {
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_CREATE,
        }),
        ..delete
    };
    assert_eq!(history.redo(), Some(Change::DeleteLiveRule(redo)));
}

#[test]
fn a_press_that_changed_nothing_leaves_no_step() {
    const SHOWN: Revision = Revision::new(7);
    // Someone pressed the pad to arm its rule, and found it armed already.
    let found_armed = IntentEffect {
        rule: Some(RuleStatus {
            rule: Rule::new(
                RULE.parse().unwrap(),
                SELECTOR.parse::<SelectorName>().unwrap(),
                PRESET.parse::<PresetName>().unwrap(),
            ),
            live: true,
            state: RuleState::Armed,
            revision: SHOWN,
        }),
        found: Some(SHOWN),
        made: false,
        changed: false,
        hold: None,
    };

    assert_eq!(Step::pressed(&ENVIRONMENT.parse().unwrap(), &found_armed), None);
}

#[test]
fn a_press_that_made_the_rule_of_its_pad_is_undone_by_deleting_it() {
    const LEFT_BY_THE_PRESS: Revision = Revision::new(8);
    const LEFT_BY_THE_UNDO: Revision = Revision::new(9);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let made = Rule::new(
        RULE.parse().unwrap(),
        SELECTOR.parse::<SelectorName>().unwrap(),
        PRESET.parse::<PresetName>().unwrap(),
    );
    // The pad pointed at no rule, so the press found none, made the live rule of the pad and armed it.
    let pressed = IntentEffect {
        rule: Some(RuleStatus {
            rule: made.clone(),
            live: true,
            state: RuleState::Armed,
            revision: LEFT_BY_THE_PRESS,
        }),
        found: None,
        made: true,
        changed: true,
        hold: None,
    };
    history.record(Step::pressed(&ENVIRONMENT.parse().unwrap(), &pressed).unwrap());

    let delete = DeleteLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_PRESS,
        }),
    };
    assert_eq!(history.undo(), Some(Change::DeleteLiveRule(delete)));
    history.settled(Outcome::Applied {
        revision: LEFT_BY_THE_UNDO,
    });

    // The redo makes the rule of the pad again, armed, as the press left it.
    let create = CreateLiveRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: made,
        armed: true,
    };
    assert_eq!(history.redo(), Some(Change::CreateLiveRule(create)));
}

#[test]
fn a_hold_leaves_a_step_only_when_its_press_made_the_rule() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_FIRST_PRESS: Revision = Revision::new(8);
    const LEFT_BY_THE_FIRST_RELEASE: Revision = Revision::new(9);
    const LEFT_BY_THE_SECOND_PRESS: Revision = Revision::new(10);
    const LEFT_BY_THE_SECOND_RELEASE: Revision = Revision::new(11);
    let run: RunId = RUN.parse().unwrap();
    let environment: EnvironmentName = ENVIRONMENT.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let armed = RuleStatus {
        rule: Rule::new(
            RULE.parse().unwrap(),
            SELECTOR.parse::<SelectorName>().unwrap(),
            PRESET.parse::<PresetName>().unwrap(),
        ),
        live: true,
        state: RuleState::Armed,
        revision: LEFT_BY_THE_FIRST_PRESS,
    };
    let (arming, making) = (Hold::new(1), Hold::new(2));

    // A hold of a pad whose rule exists arms it while held, and its release disarms it: nothing is left to undo.
    history.held(&IntentEffect {
        rule: Some(armed.clone()),
        found: Some(SHOWN),
        made: false,
        changed: true,
        hold: Some(arming),
    });
    history.released(
        &environment,
        arming,
        &IntentEffect {
            rule: Some(RuleStatus {
                state: RuleState::Disarmed,
                revision: LEFT_BY_THE_FIRST_RELEASE,
                ..armed.clone()
            }),
            found: Some(LEFT_BY_THE_FIRST_PRESS),
            made: false,
            changed: true,
            hold: None,
        },
    );
    assert!(!history.can_undo());

    // A hold that made the rule is one step with its release, and its undo deletes the rule.
    history.held(&IntentEffect {
        rule: Some(RuleStatus {
            revision: LEFT_BY_THE_SECOND_PRESS,
            ..armed.clone()
        }),
        found: None,
        made: true,
        changed: true,
        hold: Some(making),
    });
    assert!(!history.can_undo(), "the step waits for the release");
    history.released(
        &environment,
        making,
        &IntentEffect {
            rule: Some(RuleStatus {
                state: RuleState::Disarmed,
                revision: LEFT_BY_THE_SECOND_RELEASE,
                ..armed
            }),
            found: Some(LEFT_BY_THE_SECOND_PRESS),
            made: false,
            changed: true,
            hold: None,
        },
    );
    let delete = DeleteLiveRule {
        environment,
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_SECOND_RELEASE,
        }),
    };
    assert_eq!(history.undo(), Some(Change::DeleteLiveRule(delete)));
}

#[test]
fn turns_of_one_encoder_are_one_step_back_to_the_rule_before_the_first() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_FIRST_TURN: Revision = Revision::new(8);
    const LEFT_BY_THE_SECOND_TURN: Revision = Revision::new(9);
    const LEFT_BY_THE_DELAY: Revision = Revision::new(10);
    const LEFT_BY_THE_UNDO_OF_THE_DELAY: Revision = Revision::new(11);
    const LEFT_BY_THE_UNDO_OF_THE_TURNS: Revision = Revision::new(12);
    let run: RunId = RUN.parse().unwrap();
    let environment: EnvironmentName = ENVIRONMENT.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let always = RuleStatus {
        rule: Rule::new(
            RULE.parse().unwrap(),
            SELECTOR.parse::<SelectorName>().unwrap(),
            PRESET.parse::<PresetName>().unwrap(),
        ),
        live: true,
        state: RuleState::Armed,
        revision: SHOWN,
    };
    let at_90_percent = RuleStatus {
        rule: always.rule.clone().with_trigger(Trigger::from(
            TriggerParts::new().with_chance(Chance::from_percent(90).unwrap()),
        )),
        revision: LEFT_BY_THE_FIRST_TURN,
        ..always.clone()
    };
    let at_80_percent = RuleStatus {
        rule: always.rule.clone().with_trigger(Trigger::from(
            TriggerParts::new().with_chance(Chance::from_percent(80).unwrap()),
        )),
        revision: LEFT_BY_THE_SECOND_TURN,
        ..always.clone()
    };

    // Two turns of the probability. The second found what the first left, while the tab still showed the rule
    // before the first.
    let first = IntentEffect {
        rule: Some(at_90_percent),
        found: Some(SHOWN),
        made: false,
        changed: true,
        hold: None,
    };
    history.record(Step::turned(&environment, Encoder::Probability, &always, &first).unwrap());
    let second = IntentEffect {
        rule: Some(at_80_percent.clone()),
        found: Some(LEFT_BY_THE_FIRST_TURN),
        made: false,
        changed: true,
        hold: None,
    };
    history.record(Step::turned(&environment, Encoder::Probability, &always, &second).unwrap());
    // A turn of another encoder is a step of its own.
    let delay = IntentEffect {
        rule: Some(RuleStatus {
            revision: LEFT_BY_THE_DELAY,
            ..at_80_percent.clone()
        }),
        found: Some(LEFT_BY_THE_SECOND_TURN),
        made: false,
        changed: true,
        hold: None,
    };
    history.record(Step::turned(&environment, Encoder::Delay, &at_80_percent, &delay).unwrap());

    history.undo().unwrap();
    history.settled(Outcome::Applied {
        revision: LEFT_BY_THE_UNDO_OF_THE_DELAY,
    });
    let back = UpdateLiveRule {
        environment,
        rule: always.rule,
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UNDO_OF_THE_DELAY,
        }),
    };
    assert_eq!(history.undo(), Some(Change::UpdateLiveRule(back)));
    history.settled(Outcome::Applied {
        revision: LEFT_BY_THE_UNDO_OF_THE_TURNS,
    });
    assert!(!history.can_undo(), "the two turns of the probability were one step");
}

#[test]
fn a_turn_that_found_another_rule_than_the_tab_showed_leaves_no_step() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_OTHER_TAB: Revision = Revision::new(8);
    const LEFT_BY_THE_TURN: Revision = Revision::new(9);
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run: RUN.parse().unwrap(),
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let shown = RuleStatus {
        rule: Rule::new(
            RULE.parse().unwrap(),
            SELECTOR.parse::<SelectorName>().unwrap(),
            PRESET.parse::<PresetName>().unwrap(),
        ),
        live: true,
        state: RuleState::Armed,
        revision: SHOWN,
    };
    // Another tab changed the rule a moment before the turn, and the stream had not told this tab yet.
    let turned = IntentEffect {
        rule: Some(RuleStatus {
            revision: LEFT_BY_THE_TURN,
            ..shown.clone()
        }),
        found: Some(LEFT_BY_THE_OTHER_TAB),
        made: false,
        changed: true,
        hold: None,
    };

    history.record(Step::turned(&ENVIRONMENT.parse().unwrap(), Encoder::Delay, &shown, &turned).unwrap());

    assert!(
        !history.can_undo(),
        "setting the rule back to what the tab showed would take the change of the other tab too"
    );
}

#[test]
fn a_new_change_ends_what_could_be_redone() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    const LEFT_BY_THE_UNDO: Revision = Revision::new(9);
    const LEFT_BY_THE_NEW_ARM: Revision = Revision::new(10);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::armed(
        arm.clone(),
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));
    history.undo().unwrap();
    history.settled(Outcome::Applied {
        revision: LEFT_BY_THE_UNDO,
    });
    assert!(history.can_redo());

    let arm_again = ArmRule {
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UNDO,
        }),
        ..arm
    };
    history.record(Step::armed(
        arm_again,
        Revised {
            revision: LEFT_BY_THE_NEW_ARM,
        },
    ));

    assert!(!history.can_redo());
}

#[test]
fn the_history_holds_as_many_steps_as_its_depth_says() {
    const SHOWN: Revision = Revision::new(7);
    let run: RunId = RUN.parse().unwrap();
    let mut configuration = UiConfiguration::default();
    configuration.undo_depth = NonZeroUsize::new(2).unwrap();
    let mut history = History::new(configuration.undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    // The tab arms three rules, one after the other, and each arm takes the next revision.
    let mut left = SHOWN;
    for rule in ["slow-order-producers", "slow-order-consumers", "slow-payments"] {
        let arm = ArmRule {
            environment: ENVIRONMENT.parse().unwrap(),
            rule: rule.parse().unwrap(),
            expects: Some(Expects { run, revision: SHOWN }),
        };
        left = Revision::new(left.number() + 1);
        history.record(Step::armed(arm, Revised { revision: left }));
    }

    let mut undone = Vec::new();
    while let Some(Change::DisarmRule(disarm)) = history.undo() {
        undone.push(disarm.rule.to_string());
        left = Revision::new(left.number() + 1);
        history.settled(Outcome::Applied { revision: left });
    }

    assert_eq!(
        undone,
        ["slow-payments", "slow-order-consumers"],
        "the oldest step went when the third came"
    );
}

#[test]
fn the_history_lasts_across_a_stream_that_opens_again_and_ends_with_the_run() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    let run: RunId = RUN.parse().unwrap();
    let mut tab = Live::new(&UiConfiguration::default());
    tab.opened();
    tab.apply(StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    tab.history_mut().record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));

    tab.lost("the connection dropped".to_owned());
    tab.opened();
    tab.apply(StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    assert!(tab.history().can_undo(), "a stream of the same run keeps the steps");

    tab.lost("the process stopped".to_owned());
    tab.opened();
    tab.apply(StreamMessage::Snapshot(ProcessState {
        run: NEXT_RUN.parse().unwrap(),
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    assert!(
        !tab.history().can_undo(),
        "nothing of an earlier run fits a process that started again"
    );
}

#[test]
fn an_undo_refused_with_another_run_drops_every_step() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    const LEFT_BY_THE_DISARM: Revision = Revision::new(9);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));
    let disarm = DisarmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_ARM,
        }),
    };
    history.record(Step::disarmed(
        disarm,
        Revised {
            revision: LEFT_BY_THE_DISARM,
        },
    ));

    history.undo().unwrap();
    let refused = RuleCommandError::OtherRun {
        run: NEXT_RUN.parse().unwrap(),
    };
    let report = history.settled(Outcome::from(refused)).unwrap();

    assert!(report.refused);
    assert!(
        !history.can_undo(),
        "no step of an earlier run arms a fault in a process that started again"
    );
}

#[test]
fn a_step_goes_with_its_environment_and_a_change_of_the_workspace_stays() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_UPDATE: Revision = Revision::new(8);
    const LEFT_BY_THE_ARM: Revision = Revision::new(9);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let always = Rule::new(
        RULE.parse().unwrap(),
        SELECTOR.parse::<SelectorName>().unwrap(),
        PRESET.parse::<PresetName>().unwrap(),
    );
    let update = UpdateDefinition {
        expects: Some(Expects { run, revision: SHOWN }),
        definition: always.clone().with_trigger(Trigger::from(
            TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
        )),
    };
    history.record(Step::updated_rule(
        update,
        always.clone(),
        Revised {
            revision: LEFT_BY_THE_UPDATE,
        },
    ));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UPDATE,
        }),
    };
    history.record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));

    history.apply(&StreamMessage::EnvironmentStopped {
        environment: ENVIRONMENT.parse().unwrap(),
    });

    // The arm went with the environment, and the change of the rule of the workspace stays.
    let back = UpdateDefinition {
        expects: Some(Expects {
            run,
            revision: LEFT_BY_THE_UPDATE,
        }),
        definition: always,
    };
    assert_eq!(history.undo(), Some(Change::UpdateRule(back)));
}

#[test]
fn a_change_of_a_rule_of_the_workspace_is_a_change_of_it_in_every_environment() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    const LEFT_BY_THE_UPDATE: Revision = Revision::new(9);
    const LEFT_BY_ITS_UNDO: Revision = Revision::new(10);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let always = Rule::new(
        RULE.parse().unwrap(),
        SELECTOR.parse::<SelectorName>().unwrap(),
        PRESET.parse::<PresetName>().unwrap(),
    );
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));
    // The definition has a revision of its own, which the arm in the environment did not change.
    let update = UpdateDefinition {
        expects: Some(Expects { run, revision: SHOWN }),
        definition: always.clone().with_trigger(Trigger::from(
            TriggerParts::new().with_chance(Chance::from_percent(40).unwrap()),
        )),
    };
    history.record(Step::updated_rule(
        update,
        always,
        Revised {
            revision: LEFT_BY_THE_UPDATE,
        },
    ));

    history.undo().unwrap();
    history.settled(Outcome::Applied {
        revision: LEFT_BY_ITS_UNDO,
    });

    // Setting the definition back left its revision on the rule in the environment too.
    let disarm = DisarmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects {
            run,
            revision: LEFT_BY_ITS_UNDO,
        }),
    };
    assert_eq!(history.undo(), Some(Change::DisarmRule(disarm)));
}

#[test]
fn an_undo_that_got_no_answer_stays_and_one_goes_out_at_a_time() {
    const SHOWN: Revision = Revision::new(7);
    const LEFT_BY_THE_ARM: Revision = Revision::new(8);
    let run: RunId = RUN.parse().unwrap();
    let mut history = History::new(UiConfiguration::default().undo_depth);
    history.apply(&StreamMessage::Snapshot(ProcessState {
        run,
        definitions: Definitions::default(),
        environments: Vec::new(),
        workspace: WorkspaceStatus::default(),
    }));
    let arm = ArmRule {
        environment: ENVIRONMENT.parse().unwrap(),
        rule: RULE.parse().unwrap(),
        expects: Some(Expects { run, revision: SHOWN }),
    };
    history.record(Step::armed(
        arm,
        Revised {
            revision: LEFT_BY_THE_ARM,
        },
    ));

    let undo = history.undo().unwrap();
    assert_eq!(history.undo(), None, "a second ⌘Z waits for the answer to the first");
    let unanswered = Outcome::Unanswered {
        reason: "the connection dropped".to_owned(),
    };
    assert!(history.settled(unanswered).unwrap().refused);

    assert_eq!(
        history.undo(),
        Some(undo),
        "the undo that may not have applied goes out again"
    );
}
