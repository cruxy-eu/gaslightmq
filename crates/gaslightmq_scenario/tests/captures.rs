//! A capture: what someone played live, read from the journal, as the steps of a Sequence.

use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;

/// When someone arms a live rule, counted from the start of the capture.
const SLOW_ARMED_AFTER: Duration = Duration::from_secs(2);

/// When someone disarms the rule of the workspace, counted from the start of the capture.
const REFUSE_DISARMED_AFTER: Duration = Duration::from_secs(5);

/// When someone stops the capture, counted from its start.
const STOPPED_AFTER: Duration = Duration::from_millis(6500);

/// The revision that arming the live rule left.
const SLOW_ARMED: Revision = Revision::new(7);

/// The revision that disarming the rule of the workspace left.
const REFUSE_DISARMED: Revision = Revision::new(8);

/// The revision that activating a scenario left, which the rules it armed share.
const ACTIVATED: Revision = Revision::new(4);

/// The revision that deactivating the scenario left, which the rules it disarmed share.
const DEACTIVATED: Revision = Revision::new(5);

/// The revision of a reload, which every rule that it disarmed shares.
const RELOADED: Revision = Revision::new(9);

/// The seed of a scenario that a test saves.
const SEED: Seed = Seed::new(7);

#[test]
fn every_change_of_the_live_set_is_a_step_that_ends_on_the_time_until_the_next_change() {
    let slow = Rule::new(
        "slow-produce".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_secs(1)),
    );
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let started = Instant::ZERO;
    // The workspace rule was armed by hand before the capture started.
    let mut capture = Capture::start(started, [CapturedRule::from(refuse.clone())]);

    // Someone arms a live rule at a pad, and then disarms the rule of the workspace in the UI.
    let armed = Change::RuleArmed {
        rule: slow.name().as_str().parse().unwrap(),
    };
    let armed = journal.record(started + SLOW_ARMED_AFTER, armed, ChangeSurface::Pad, Some(SLOW_ARMED));
    capture.read(&armed, |_| Some(slow.clone()));
    let disarmed = Change::RuleDisarmed {
        rule: refuse.as_str().parse().unwrap(),
    };
    let at = started + REFUSE_DISARMED_AFTER;
    let disarmed = journal.record(at, disarmed, ChangeSurface::Ui, Some(REFUSE_DISARMED));
    capture.read(&disarmed, |_| None);
    let stopped_at = started + STOPPED_AFTER;
    let captured = capture.stop(stopped_at);

    assert_eq!(
        captured.steps(),
        [
            Step::new("step-1".parse().unwrap())
                .arming([refuse.clone()])
                .ending_on(Until::After(TimeSpan::from_duration(SLOW_ARMED_AFTER))),
            // The rule armed most recently comes first, so it wins a conflict as it did in the live set.
            Step::new("step-2".parse().unwrap())
                .with_title("Armed slow-produce".parse().unwrap())
                .arming([slow.name().clone(), refuse.clone()])
                .ending_on(Until::After(TimeSpan::from_duration(disarmed.at - armed.at))),
            Step::new("step-3".parse().unwrap())
                .with_title("Disarmed refuse-with-not-leader".parse().unwrap())
                .arming([slow.name().clone()])
                .ending_on(Until::After(TimeSpan::from_duration(stopped_at - disarmed.at))),
        ]
    );
    // The live rule is captured as it was armed. The rule of the workspace stays a name.
    assert_eq!(captured.rules(), [slow]);
}

#[test]
fn what_a_scenario_arms_and_disarms_is_no_change_of_the_live_set() {
    let jitter: RecordedName = "background-jitter".parse().unwrap();
    let scenario: RecordedName = "leader-election".parse().unwrap();
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let mut capture = Capture::start(Instant::ZERO, []);
    let activated_at = Instant::ZERO + SLOW_ARMED_AFTER;
    let deactivated_at = Instant::ZERO + REFUSE_DISARMED_AFTER;

    // Someone activates a scenario, which arms its rule with the revision of the activation.
    let activated = Change::ScenarioActivated {
        scenario: scenario.clone(),
        step: None,
    };
    for change in [activated, Change::RuleArmed { rule: jitter.clone() }] {
        let entry = journal.record(activated_at, change, ChangeSurface::Ui, Some(ACTIVATED));
        capture.read(&entry, |_| None);
    }
    // The deactivation disarms it again.
    let deactivated = Change::ScenarioDeactivated { scenario, step: None };
    for change in [Change::RuleDisarmed { rule: jitter }, deactivated] {
        let entry = journal.record(deactivated_at, change, ChangeSurface::Ui, Some(DEACTIVATED));
        capture.read(&entry, |_| None);
    }
    let captured = capture.stop(deactivated_at);

    // The capture holds one step, the live set as it found it: empty, for the whole capture.
    let found =
        Step::new("step-1".parse().unwrap()).ending_on(Until::After(TimeSpan::from_duration(REFUSE_DISARMED_AFTER)));
    assert_eq!(captured.steps(), [found]);
}

#[test]
fn a_captured_rule_keeps_the_parameters_it_had_when_it_was_armed() {
    let sometimes = TriggerParts::new().with_chance("40%".parse().unwrap());
    let flaky = Rule::new(
        "flaky-produce".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_trigger(sometimes);
    let recorded: RecordedName = flaky.name().as_str().parse().unwrap();
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let mut capture = Capture::start(Instant::ZERO, []);
    let armed_at = Instant::ZERO + SLOW_ARMED_AFTER;
    let turned_at = Instant::ZERO + REFUSE_DISARMED_AFTER;

    let armed = Change::RuleArmed { rule: recorded.clone() };
    let entry = journal.record(armed_at, armed, ChangeSurface::Pad, Some(SLOW_ARMED));
    capture.read(&entry, |_| Some(flaky.clone()));
    // A turn of the encoder makes the rule fire more often. The capture ignores it.
    let turned = Change::ParameterChanged {
        rule: recorded,
        parameter: Parameter::Chance,
        old: "40%".to_owned(),
        new: "70%".to_owned(),
    };
    let entry = journal.record(turned_at, turned, ChangeSurface::Pad, Some(REFUSE_DISARMED));
    capture.read(&entry, |_| None);
    let captured = capture.stop(turned_at);

    assert_eq!(captured.steps()[1].arm(), [flaky.name().clone()]);
    assert_eq!(captured.rules(), [flaky]);
}

#[test]
fn the_entries_of_one_revision_are_one_change() {
    let names = ["slow-produce", "flaky-fetch"].map(|name| name.parse::<RecordedName>().unwrap());
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let found = names
        .clone()
        .map(|name| CapturedRule::from(name.as_str().parse::<RuleName>().unwrap()));
    let mut capture = Capture::start(Instant::ZERO, found);
    let reloaded_at = Instant::ZERO + REFUSE_DISARMED_AFTER;

    // A reload takes both rules of the workspace away: two entries, with the revision of the reload.
    for rule in names {
        let disarmed = Change::RuleDisarmed { rule };
        let entry = journal.record(reloaded_at, disarmed, ChangeSurface::File, Some(RELOADED));
        capture.read(&entry, |_| None);
    }
    let captured = capture.stop(reloaded_at);

    assert_eq!(captured.steps().len(), 2);
    assert_eq!(
        captured.steps()[1].title().map(StepTitle::as_str),
        Some("Disarmed slow-produce, flaky-fetch")
    );
}

#[test]
fn the_steps_of_a_capture_make_a_sequence_scenario() {
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let capture = Capture::start(Instant::ZERO, [CapturedRule::from(refuse.clone())]);
    let captured = capture.stop(Instant::ZERO + STOPPED_AFTER);

    let scenario = captured.scenario("refused-produce".parse().unwrap(), SEED);

    assert_eq!(scenario.mode().name(), "sequence");
    assert_eq!(scenario.seed(), SEED);
    assert_eq!(scenario.rule_references(), [&refuse]);
    assert_eq!(scenario.steps(), captured.steps());
}
