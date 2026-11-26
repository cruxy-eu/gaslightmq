//! Scenarios: what they name exists, what they name stays, and the rules they own share no name.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::OrderDesk;

#[test]
fn a_scenario_names_rules_and_selectors_that_exist() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let slow_order_producers: RuleName = "slow-order-producers".parse()?;
    let order_producers: SelectorName = "order-producers".parse()?;
    let warm_up = Step::new("warm-up".parse()?).ending_on(Until::Calls {
        count: Count::new(100)?,
        selector: order_producers.clone().into(),
    });
    let slow_down = Step::new("slow-down".parse()?).arming([slow_order_producers.clone()]);
    let scenario = Scenario::new(
        "producers-slow-down".parse()?,
        Mode::Sequence(vec![warm_up, slow_down]),
        Seed::new(7),
        [],
    )?;
    let name: ScenarioName = scenario.name().clone();

    block_on(async {
        // Nothing that the scenario names exists yet.
        let early = workspace.create(scenario.clone()).await.unwrap_err();
        assert_eq!(
            early,
            CreateError::DoesNotFit {
                name: name.clone().into(),
                errors: vec![
                    WorkspaceError::UnknownReference {
                        definition: name.clone().into(),
                        reference: slow_order_producers.clone().into(),
                    },
                    WorkspaceError::UnknownReference {
                        definition: name.clone().into(),
                        reference: order_producers.clone().into(),
                    },
                ],
            }
        );
        assert_eq!(
            early.to_string(),
            "scenario \"producers-slow-down\" does not fit the workspace: \
             scenario \"producers-slow-down\" refers to rule \"slow-order-producers\", which does not exist; \
             scenario \"producers-slow-down\" refers to selector \"order-producers\", which does not exist"
        );

        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        let revision = workspace.create(scenario.clone()).await?;

        // A scenario has its revision like every other definition, and its store holds it as it was written.
        assert_eq!(workspace.revision(name.clone()), revision);
        assert_eq!(workspace.stores().scenarios().get(&name).await?, Some(scenario));
        workspace.validate().await?;
        Ok(())
    })
}

#[test]
fn a_rule_or_a_selector_that_a_scenario_names_cannot_be_deleted() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let slow_order_producers: RuleName = "slow-order-producers".parse()?;
    let order_producers: SelectorName = "order-producers".parse()?;
    let warm_up = Step::new("warm-up".parse()?).ending_on(Until::Calls {
        count: Count::new(100)?,
        selector: order_producers.clone().into(),
    });
    let scenario = Scenario::new(
        "producers-slow-down".parse()?,
        Mode::Sequence(vec![warm_up]),
        Seed::new(7),
        [slow_order_producers.clone().into()],
    )?;
    let name: ScenarioName = scenario.name().clone();

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        workspace.create(scenario).await?;

        let rule_in_use = workspace.delete(slow_order_producers.clone(), None).await.unwrap_err();
        let selector_in_use = workspace.delete(order_producers.clone(), None).await.unwrap_err();

        assert_eq!(
            rule_in_use,
            DeleteError::InUse {
                name: slow_order_producers.clone().into(),
                users: vec![name.clone().into()],
            }
        );
        assert_eq!(
            rule_in_use.to_string(),
            "rule \"slow-order-producers\" is used by scenario \"producers-slow-down\""
        );
        // The rule uses the selector too, and the error names both.
        assert_eq!(
            selector_in_use.to_string(),
            "selector \"order-producers\" is used by rule \"slow-order-producers\" \
             and scenario \"producers-slow-down\""
        );

        // Nothing refers to a scenario, so it goes, and what it named can go after it.
        workspace.delete(name.clone(), None).await?;
        workspace.delete(slow_order_producers, None).await?;
        assert_eq!(workspace.stores().scenarios().get(&name).await?, None);
        Ok(())
    })
}

#[test]
fn a_preset_that_a_rule_of_a_scenario_uses_cannot_be_deleted() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole: PresetName = "blackhole".parse()?;
    let owned = Rule::new(
        "blackhole-order-producers".parse()?,
        "order-producers".parse::<SelectorName>()?,
        blackhole.clone(),
    );
    let scenario = Scenario::new("black-friday".parse()?, Mode::Chain, Seed::new(42), [owned.into()])?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace.create(scenario).await?;

        let error = workspace.delete(blackhole, None).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" is used by scenario \"black-friday\""
        );
        Ok(())
    })
}

#[test]
fn a_rule_that_a_scenario_owns_shares_its_name_with_no_other_rule() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole_metadata = OrderDesk::blackhole_metadata();
    let owning = |scenario: &str| {
        Scenario::new(
            scenario.parse().expect("a scenario name"),
            Mode::Chain,
            Seed::new(42),
            [blackhole_metadata.clone().into()],
        )
        .expect("a scenario")
    };
    let rule: DefinitionName = blackhole_metadata.name().clone().into();
    let first: ScenarioName = "quiet-metadata".parse()?;
    let second: ScenarioName = "metadata-outage".parse()?;

    block_on(async {
        workspace.create(owning(first.as_str())).await?;

        // The workspace cannot take a rule of the name, because a scenario holds a rule of it already.
        let of_the_workspace = workspace.create(blackhole_metadata.clone()).await.unwrap_err();
        assert_eq!(
            of_the_workspace,
            CreateError::DoesNotFit {
                name: rule.clone(),
                errors: vec![WorkspaceError::SharedRuleName {
                    rule: blackhole_metadata.name().clone(),
                    holders: vec![rule.clone(), first.clone().into()],
                }],
            }
        );
        assert_eq!(
            of_the_workspace.to_string(),
            "rule \"blackhole-metadata\" does not fit the workspace: \
             rule \"blackhole-metadata\" and scenario \"quiet-metadata\" each hold a rule named \"blackhole-metadata\""
        );

        // Neither can another scenario.
        let of_another_scenario = workspace.create(owning(second.as_str())).await.unwrap_err();
        assert_eq!(
            of_another_scenario.to_string(),
            "scenario \"metadata-outage\" does not fit the workspace: \
             scenario \"metadata-outage\" and scenario \"quiet-metadata\" each hold a rule named \"blackhole-metadata\""
        );
        assert_eq!(workspace.stores().rules().list().await?, []);
        assert_eq!(workspace.stores().scenarios().get(&second).await?, None);
        Ok(())
    })
}

#[test]
fn a_rule_that_a_scenario_owns_can_fire() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule: RuleName = "blackhole-what-worked".parse()?;
    let owned = Rule::new(
        rule.clone(),
        "answered".parse::<SelectorName>()?,
        "blackhole".parse::<PresetName>()?,
    );
    let scenario = Scenario::new("black-friday".parse()?, Mode::Chain, Seed::new(42), [owned.into()])?;
    let name: ScenarioName = scenario.name().clone();

    block_on(async {
        workspace.create(OrderDesk::answered()).await?;
        workspace.create(OrderDesk::blackhole()).await?;

        // The rule only knows which calls it means once the backend answered, and a silence acts before that.
        let error = workspace.create(scenario).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::DoesNotFit {
                name: name.clone().into(),
                errors: vec![WorkspaceError::InvalidOwnedRule {
                    scenario: name.clone(),
                    error: RuleError::LieBeforeTruth {
                        rule,
                        lie: LieKind::Silence,
                    },
                }],
            }
        );
        assert_eq!(
            error.to_string(),
            "scenario \"black-friday\" does not fit the workspace: \
             scenario \"black-friday\": rule \"blackhole-what-worked\" tests a response field, \
             and its silence acts before forwarding, when the truth is not known yet. Give the lie `stage: after`"
        );
        assert_eq!(workspace.stores().scenarios().list().await?, []);
        Ok(())
    })
}
