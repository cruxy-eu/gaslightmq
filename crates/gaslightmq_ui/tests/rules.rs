//! What the Rules view offers for a rule, and which operation each choice sends: a live rule changes in its
//! environment, and a rule of the workspace changes in the workspace.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::kernel::ClientId;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::{Scope, SelectorName};
use gaslightmq_control_api::trigger::{Chance, Count, Lifetime, Trigger, TriggerParts};
use gaslightmq_ui::rules::{
    RuleCommand, RuleOperation, RuleTab, activity, by_scope, commands, operation, update, with_chance,
};

/// The rule `slow-order-producers` of the specs: the selector `order-producers`, and the preset `delay-1s`.
fn slow_order_producers() -> Rule {
    Rule::new(
        "slow-order-producers".parse().unwrap(),
        "order-producers".parse::<SelectorName>().unwrap(),
        "delay-1s".parse::<PresetName>().unwrap(),
    )
}

#[test]
fn an_armed_rule_disarms_and_fires_and_a_disarmed_one_arms() {
    let armed = RuleStatus {
        rule: slow_order_producers(),
        live: false,
        state: RuleState::Armed,
    };
    let disarmed = RuleStatus {
        state: RuleState::Disarmed,
        ..armed.clone()
    };
    let draft = RuleStatus {
        state: RuleState::Draft,
        ..armed.clone()
    };

    assert_eq!(commands(&armed), [RuleCommand::Disarm, RuleCommand::Fire]);
    assert_eq!(commands(&disarmed), [RuleCommand::Arm]);
    assert_eq!(commands(&draft), [], "a draft is never evaluated");
}

#[test]
fn a_live_rule_can_be_kept_or_deleted_and_a_rule_of_the_workspace_cannot() {
    let live = RuleStatus {
        rule: slow_order_producers(),
        live: true,
        state: RuleState::Disarmed,
    };
    let environment: EnvironmentName = "orders-sim".parse().unwrap();

    assert_eq!(
        commands(&live),
        [RuleCommand::Arm, RuleCommand::Keep, RuleCommand::Delete]
    );
    assert_eq!(
        operation(&environment, &live, RuleCommand::Keep),
        RuleOperation::Keep(KeepRule {
            environment,
            rule: "slow-order-producers".parse().unwrap(),
        })
    );
}

#[test]
fn a_change_of_a_live_rule_stays_in_its_environment_and_a_rule_of_the_workspace_changes_in_the_workspace() {
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let workspace = RuleStatus {
        rule: slow_order_producers(),
        live: false,
        state: RuleState::Armed,
    };
    let live = RuleStatus {
        live: true,
        ..workspace.clone()
    };
    let sometimes = with_chance(&slow_order_producers(), Some(Chance::from_percent(25).unwrap()));

    assert_eq!(
        update(&environment, &live, sometimes.clone()),
        RuleOperation::UpdateLive(UpdateLiveRule {
            environment: environment.clone(),
            rule: sometimes.clone(),
        })
    );
    assert_eq!(
        update(&environment, &workspace, sometimes.clone()),
        RuleOperation::UpdateWorkspace(UpdateDefinition { definition: sometimes })
    );
}

#[test]
fn a_new_chance_keeps_the_other_parts_of_the_trigger_and_a_rule_by_hand_stays_by_hand() {
    let every_tenth = TriggerParts::new().with_every(Count::new(10).unwrap());
    let rule = slow_order_producers().with_trigger(every_tenth);
    let by_hand = slow_order_producers().with_trigger(Trigger::Manual);

    let sometimes = with_chance(&rule, Some(Chance::from_percent(40).unwrap()));

    assert_eq!(
        sometimes.trigger(),
        Trigger::from(every_tenth.with_chance(Chance::from_percent(40).unwrap()))
    );
    assert_eq!(
        with_chance(&by_hand, Some(Chance::from_percent(40).unwrap())).trigger(),
        Trigger::Manual
    );
    assert_eq!(with_chance(&sometimes, None).trigger(), Trigger::from(every_tenth));
    assert_eq!(sometimes.lifetime(), Lifetime::UntilDisarmed);
}

#[test]
fn the_armed_tab_lists_the_armed_rules_by_scope_and_every_client_comes_first() {
    let ingest = Scope::client(ClientId::new("order-ingest").unwrap());
    let for_ingest = RuleStatus {
        rule: Rule::new(
            "slow-ingest".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        )
        .scoped(ingest.clone()),
        live: true,
        state: RuleState::Armed,
    };
    let for_everyone = RuleStatus {
        rule: slow_order_producers(),
        live: false,
        state: RuleState::Armed,
    };
    let resting = RuleStatus {
        rule: Rule::new(
            "resting".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        ),
        live: false,
        state: RuleState::Disarmed,
    };

    let groups = by_scope(&[for_ingest.clone(), for_everyone.clone(), resting], RuleTab::Armed);

    let scopes: Vec<&Scope> = groups.iter().map(|group| &group.scope).collect();
    assert_eq!(scopes, [&Scope::all(), &ingest]);
    assert_eq!(groups[0].rules, [for_everyone]);
    assert_eq!(groups[1].rules, [for_ingest]);
}

#[test]
fn the_activity_of_a_rule_is_what_it_counted_in_each_interval() {
    let name: RuleName = "slow-order-producers".parse().unwrap();
    let counted = |matches: u64, firings: u64| Stats {
        rules: BTreeMap::from([(
            name.clone(),
            RuleStats {
                matches,
                firings,
                left: None,
            },
        )]),
        ..Stats::default()
    };
    let history = [counted(10, 4), counted(25, 9), counted(25, 9)];

    let intervals: Vec<(u64, u64)> = activity(&history, &name)
        .into_iter()
        .map(|interval| (interval.matches, interval.firings))
        .collect();

    assert_eq!(intervals, [(15, 5), (0, 0)]);
}
