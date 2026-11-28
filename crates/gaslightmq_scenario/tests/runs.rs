//! A scenario that runs: the order in which its rules win a conflict, and the conditions that end its steps.

use gaslightmq_call::CallId;
use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;

#[test]
fn a_chain_keeps_its_rules_in_its_order_and_a_live_scenario_counts_its_last_rule_as_armed_last() {
    let blackhole = Rule::new(
        "blackhole-half".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let refuse = Rule::new(
        "refuse-half".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let workspace = RuleSet::new([blackhole.clone(), refuse.clone()]).unwrap();
    let rules = [
        ScenarioRule::from(blackhole.name().clone()),
        ScenarioRule::from(refuse.name().clone()),
    ];
    let resolved = |mode: Mode| {
        let scenario = Scenario::new("halves".parse().unwrap(), mode, Seed::new(42), rules.clone()).unwrap();
        scenario
            .resolve(&workspace, &SelectorSet::default(), &PresetSet::default())
            .unwrap()
    };

    let chain = ScenarioRun::start(resolved(Mode::Chain), Instant::ZERO, 0);
    let live = ScenarioRun::start(resolved(Mode::Live), Instant::ZERO, 0);

    let in_order = |run: &ScenarioRun| {
        run.armed_in_order()
            .iter()
            .map(|rule| rule.name().clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(in_order(&chain), [blackhole.name().clone(), refuse.name().clone()]);
    assert_eq!(in_order(&live), [refuse.name().clone(), blackhole.name().clone()]);
    // Neither has a step, so neither ends a step by itself.
    assert_eq!(chain.position().step, None);
    assert!(!chain.step_is_over(Instant::ZERO));
}

#[test]
fn a_sequence_arms_the_rules_of_its_step_above_its_own_and_disarms_them_when_the_step_ends() {
    let jitter = Rule::new(
        "background-jitter".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let refuse = Rule::new(
        "refuse-with-not-leader".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let workspace = RuleSet::new([jitter.clone(), refuse.clone()]).unwrap();
    let churn = Step::new("leader-churn".parse().unwrap()).arming([refuse.name().clone()]);
    let after = Step::new("back-to-normal".parse().unwrap());
    let scenario = Scenario::new(
        "churn".parse().unwrap(),
        Mode::Sequence(vec![churn, after]),
        Seed::new(7),
        [ScenarioRule::from(jitter.name().clone())],
    )
    .unwrap();
    let resolved = scenario.resolve(&workspace, &SelectorSet::default(), &PresetSet::default());
    let mut run = ScenarioRun::start(resolved.unwrap(), Instant::ZERO, 0);
    let in_order = |run: &ScenarioRun| {
        run.armed_in_order()
            .iter()
            .map(|rule| rule.name().clone())
            .collect::<Vec<_>>()
    };

    assert_eq!(in_order(&run), [refuse.name().clone(), jitter.name().clone()]);
    let advanced = run.advance(Instant::ZERO, 0).unwrap();

    assert_eq!(advanced.ended, "leader-churn");
    assert_eq!(advanced.started.unwrap(), "back-to-normal");
    assert_eq!(in_order(&run), [jitter.name().clone()]);
    // After the last step no step runs, and the rules of the scenario stay armed.
    let last = run.advance(Instant::ZERO, 0).unwrap();
    assert_eq!(last.started, None);
    assert_eq!(in_order(&run), [jitter.name().clone()]);
    assert_eq!(run.advance(Instant::ZERO, 0), None);
}

#[test]
fn a_step_counts_the_calls_that_arrived_during_it_and_that_its_selector_matches() {
    let two = Count::new(2).unwrap();
    let produce = Conditions::from(Condition::is(ApiKey::PRODUCE));
    let warm_up = Step::new("warm-up".parse().unwrap()).ending_on(Until::Calls {
        count: two,
        selector: produce.into(),
    });
    let scenario = Scenario::new("warm".parse().unwrap(), Mode::Sequence(vec![warm_up]), Seed::new(7), []).unwrap();
    let resolved = scenario.resolve(&RuleSet::default(), &SelectorSet::default(), &PresetSet::default());
    // Three calls had arrived when the scenario started.
    let calls_before = 3;
    let mut run = ScenarioRun::start(resolved.unwrap(), Instant::ZERO, calls_before);
    let (a_produce, a_fetch) = (Fields::new().with(ApiKey::PRODUCE), Fields::new().with(ApiKey::FETCH));
    let earlier = CallId::new(calls_before).unwrap();

    // A call that arrived before the step belongs to the step before it, and a Fetch is no Produce.
    run.call_recorded(earlier, &a_produce);
    run.call_recorded(earlier.next(), &a_fetch);
    run.call_recorded(earlier.next().next(), &a_produce);
    assert!(!run.step_is_over(Instant::ZERO));

    run.call_recorded(earlier.next().next().next(), &a_produce);
    assert!(run.step_is_over(Instant::ZERO));
}

#[test]
fn a_step_that_ends_on_firings_hands_out_a_budget_of_that_many_and_ends_once_it_is_spent() {
    let refuse = Rule::new(
        "refuse-with-not-leader".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let three = Count::new(3).unwrap();
    let churn = Step::new("leader-churn".parse().unwrap())
        .arming([refuse.name().clone()])
        .ending_on(Until::Firings {
            count: three,
            rule: refuse.name().clone(),
        });
    let scenario = Scenario::new("churn".parse().unwrap(), Mode::Sequence(vec![churn]), Seed::new(7), []).unwrap();
    let workspace = RuleSet::new([refuse.clone()]).unwrap();
    let resolved = scenario.resolve(&workspace, &SelectorSet::default(), &PresetSet::default());
    let run = ScenarioRun::start(resolved.unwrap(), Instant::ZERO, 0);

    let (rule, budget) = run.step_budget().unwrap();
    assert_eq!(rule, refuse.name());
    assert_eq!(budget.limit(), three);
    for _ in 0..three.value() {
        assert!(!run.step_is_over(Instant::ZERO));
        budget.take().unwrap();
    }
    assert!(run.step_is_over(Instant::ZERO));
}

#[test]
fn a_step_that_ends_after_a_time_ends_on_the_clock_from_its_start() {
    let twenty_seconds = TimeSpan::from_secs(20);
    let slow = Step::new("slow-recovery".parse().unwrap()).ending_on(Until::After(twenty_seconds));
    let scenario = Scenario::new("slow".parse().unwrap(), Mode::Sequence(vec![slow]), Seed::new(7), []).unwrap();
    let resolved = scenario.resolve(&RuleSet::default(), &SelectorSet::default(), &PresetSet::default());
    let started = Instant::ZERO + TimeSpan::from_secs(5).duration();
    let run = ScenarioRun::start(resolved.unwrap(), started, 0);

    let ends_at = started + twenty_seconds.duration();
    assert_eq!(run.ends_at(), Some(ends_at));
    assert!(!run.step_is_over(started));
    assert!(run.step_is_over(ends_at));
}

#[test]
fn a_scenario_needs_the_incidents_of_its_rules_and_of_the_rules_of_its_steps() {
    let partition: TopicPartition = "orders.eu/3".parse().unwrap();
    let leader_moves = Rule::new(
        "leader-moves-away".parse().unwrap(),
        Conditions::new(),
        gaslightmq_incident::IncidentDefinition::LeaderMoves {
            partition: Some(partition),
            to: None,
        },
    );
    let move_it = Step::new("move".parse().unwrap()).arming([leader_moves.name().clone()]);
    let scenario = Scenario::new(
        "election".parse().unwrap(),
        Mode::Sequence(vec![move_it]),
        Seed::new(42),
        [],
    )
    .unwrap();
    let workspace = RuleSet::new([leader_moves]).unwrap();

    let resolved = scenario
        .resolve(&workspace, &SelectorSet::default(), &PresetSet::default())
        .unwrap();

    let kinds: Vec<_> = resolved
        .requirements()
        .iter()
        .map(|requirement| requirement.kind())
        .collect();
    assert_eq!(kinds, [gaslightmq_incident::IncidentKind::LeaderMoves]);
}

#[test]
fn a_scenario_that_names_what_the_workspace_does_not_have_does_not_resolve() {
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let missing: SelectorName = "order-producers".parse().unwrap();
    let churn = Step::new("leader-churn".parse().unwrap()).ending_on(Until::Calls {
        count: Count::ONE,
        selector: missing.into(),
    });
    let named = Scenario::new(
        "churn".parse().unwrap(),
        Mode::Chain,
        Seed::new(7),
        [ScenarioRule::from(refuse)],
    );
    let counting = Scenario::new("warm".parse().unwrap(), Mode::Sequence(vec![churn]), Seed::new(7), []);
    let nothing = (RuleSet::default(), SelectorSet::default(), PresetSet::default());

    let unnamed = named.unwrap().resolve(&nothing.0, &nothing.1, &nothing.2).unwrap_err();
    let uncounted = counting
        .unwrap()
        .resolve(&nothing.0, &nothing.1, &nothing.2)
        .unwrap_err();

    assert_eq!(
        unnamed.to_string(),
        "scenario \"churn\" names rule \"refuse-with-not-leader\", which the workspace does not have"
    );
    assert_eq!(
        uncounted.to_string(),
        "the calls that end step \"leader-churn\" of scenario \"warm\" have no selector: \
         no selector is named \"order-producers\""
    );
}
