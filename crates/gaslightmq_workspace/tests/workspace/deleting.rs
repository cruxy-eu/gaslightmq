//! Deleting: a definition that another one uses stays, and the error says who uses it.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::{Context, OrderDesk, ReadOnly, ReadOnlyPresets, UnreadableRules};

#[test]
fn a_selector_that_a_rule_uses_cannot_be_deleted() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let order_producers: SelectorName = "order-producers".parse()?;
    let slow_order_producers: RuleName = "slow-order-producers".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;

        let error = workspace.delete(order_producers.clone(), None).await.unwrap_err();

        assert_eq!(
            error,
            DeleteError::InUse {
                name: order_producers.clone().into(),
                users: vec![slow_order_producers.clone().into()],
            }
        );
        assert_eq!(
            error.to_string(),
            "selector \"order-producers\" is used by rule \"slow-order-producers\""
        );
        // The selector stays, so the rule still names something.
        assert!(workspace.stores().selectors().get(&order_producers).await?.is_some());

        // Without the rule, nothing uses the selector.
        workspace.delete(slow_order_producers, None).await?;
        workspace.delete(order_producers.clone(), None).await?;
        assert_eq!(workspace.stores().selectors().get(&order_producers).await?, None);
        Ok(())
    })
}

#[test]
fn the_error_names_everyone_who_uses_a_selector() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let order_producers: SelectorName = "order-producers".parse()?;
    let from_ingest: SelectorName = "order-producers-from-ingest".parse()?;
    let slow_order_producers: RuleName = "slow-order-producers".parse()?;
    let order_desk: LayoutName = "order-desk".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        workspace.create(OrderDesk::layout()).await?;

        let error = workspace.delete(order_producers.clone(), None).await.unwrap_err();

        // A selector includes it, a rule uses it, and a layout has a row for it.
        assert_eq!(
            error,
            DeleteError::InUse {
                name: order_producers.into(),
                users: vec![from_ingest.into(), slow_order_producers.into(), order_desk.into()],
            }
        );
        assert_eq!(
            error.to_string(),
            "selector \"order-producers\" is used by selector \"order-producers-from-ingest\", \
             rule \"slow-order-producers\" and layout \"order-desk\""
        );
        Ok(())
    })
}

#[test]
fn a_preset_that_a_rule_or_a_layout_uses_cannot_be_deleted() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let delay: PresetName = "delay-1s".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        workspace.create(OrderDesk::layout()).await?;

        let of_the_delay = workspace.delete(delay.clone(), None).await.unwrap_err();
        let of_the_blackhole = workspace.delete(blackhole.clone(), None).await.unwrap_err();

        assert_eq!(
            of_the_delay.to_string(),
            "preset \"delay-1s\" is used by rule \"slow-order-producers\" and layout \"order-desk\""
        );
        // No rule names the preset `blackhole`.
        assert_eq!(
            of_the_blackhole.to_string(),
            "preset \"blackhole\" is used by layout \"order-desk\""
        );
        assert_eq!(workspace.stores().presets().list().await?.len(), 2);
        Ok(())
    })
}

#[test]
fn a_definition_that_only_shares_its_name_with_another_kind_is_not_in_use() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let selector: SelectorName = "blackhole".parse()?;
    let preset: PresetName = "blackhole".parse()?;
    let rule: RuleName = "blackhole".parse()?;

    block_on(async {
        workspace
            .create(Selector::new(selector.clone(), Conditions::new()))
            .await?;
        workspace.create(OrderDesk::blackhole()).await?;
        // The rule names the preset `blackhole`, and no selector.
        workspace
            .create(Rule::new(rule, Conditions::new(), preset.clone()))
            .await?;

        workspace.delete(selector, None).await?;

        let error = workspace.delete(preset, None).await.unwrap_err();
        assert_eq!(error.to_string(), "preset \"blackhole\" is used by rule \"blackhole\"");
        Ok(())
    })
}

#[test]
fn rules_layouts_and_environments_are_deleted_whenever_someone_asks() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        workspace.create(OrderDesk::layout()).await?;
        workspace.create(OrderDesk::environment()).await?;

        // Nothing refers to a rule, a layout or an environment by name.
        workspace
            .delete(OrderDesk::slow_order_producers().name().clone(), None)
            .await?;
        workspace.delete(OrderDesk::layout().name().clone(), None).await?;
        workspace.delete(OrderDesk::environment().name().clone(), None).await?;

        assert_eq!(workspace.stores().rules().list().await?, []);
        assert_eq!(workspace.stores().layouts().list().await?, []);
        assert_eq!(workspace.stores().environments().list().await?, []);
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn delete_needs_a_name_that_exists() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let order_producers: SelectorName = "order-producers".parse()?;

    block_on(async {
        // A rule has the name, and the name of a selector says that a selector is meant.
        let rule = Rule::new("order-producers".parse()?, Conditions::new(), LieDefinition::Silence);
        workspace.create(rule).await?;

        let error = workspace.delete(order_producers.clone(), None).await.unwrap_err();

        assert_eq!(
            error,
            DeleteError::NotFound {
                name: order_producers.into()
            }
        );
        assert_eq!(error.to_string(), "selector \"order-producers\" does not exist");
        assert_eq!(workspace.stores().rules().list().await?.len(), 1);
        Ok(())
    })
}

#[test]
fn a_store_that_lets_go_of_nothing_is_why_a_definition_was_not_deleted() -> anyhow::Result<()> {
    let presets = MemoryStore::new();
    block_on(presets.create(OrderDesk::blackhole()))?;
    let mut workspace = Workspace::new(Context::over(ReadOnlyPresets::holding(presets)));
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        let error = workspace.delete(blackhole.clone(), None).await.unwrap_err();

        assert_eq!(
            error,
            DeleteError::StoreFailed {
                name: blackhole.clone().into(),
                reason: ReadOnly::<Preset>::REASON.to_owned(),
            }
        );
        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" was not deleted: the directory is read-only"
        );
        assert!(workspace.stores().presets().get(&blackhole).await?.is_some());
        Ok(())
    })
}

#[test]
fn a_store_that_cannot_read_is_why_nobody_knows_who_uses_a_definition() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(Context::over(UnreadableRules::new()));
    let order_producers: SelectorName = "order-producers".parse()?;

    block_on(async {
        workspace
            .stores()
            .selectors()
            .create(OrderDesk::order_producers())
            .await?;

        // A rule may use the selector, and the store of the rules does not say.
        let error = workspace.delete(order_producers.clone(), None).await.unwrap_err();

        assert_eq!(
            error,
            DeleteError::StoreFailed {
                name: order_producers.clone().into(),
                reason: "the rule store could not be read: permission denied".to_owned(),
            }
        );
        assert_eq!(
            error.to_string(),
            "selector \"order-producers\" was not deleted: the rule store could not be read: permission denied"
        );
        assert!(workspace.stores().selectors().get(&order_producers).await?.is_some());
        Ok(())
    })
}
