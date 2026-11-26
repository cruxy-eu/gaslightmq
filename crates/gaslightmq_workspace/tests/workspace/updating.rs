//! Updating: a definition changes in everything but its name, and still fits the others.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::{Context, OrderDesk, ReadOnly, ReadOnlyPresets};

#[test]
fn an_update_changes_everything_but_the_name() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::blackhole()).await?;

        // The preset becomes a hang-up with a title.
        let change = Preset::new(blackhole.clone(), HangUpLie::new()).with_title("Hang up".parse()?);
        workspace.update(change.clone(), None).await?;

        assert_eq!(workspace.stores().presets().get(&blackhole).await?, Some(change));
        Ok(())
    })
}

#[test]
fn update_needs_a_name_that_exists() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        let error = workspace.update(OrderDesk::blackhole(), None).await.unwrap_err();

        assert_eq!(error, UpdateError::NotFound { name: blackhole.into() });
        assert_eq!(error.to_string(), "preset \"blackhole\" does not exist");
        // Updating never creates a definition.
        assert_eq!(workspace.stores().presets().list().await?, []);
        Ok(())
    })
}

#[test]
fn a_definition_that_someone_put_into_its_store_is_updated_like_any_other() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        // Someone put the preset into the store by hand, as a file that someone wrote.
        workspace.stores().presets().create(OrderDesk::blackhole()).await?;

        let change = OrderDesk::blackhole().with_title("Blackhole".parse()?);
        workspace.update(change.clone(), None).await?;

        assert_eq!(workspace.stores().presets().get(&blackhole).await?, Some(change));
        Ok(())
    })
}

#[test]
fn an_update_names_what_exists() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule: RuleName = "blackhole-metadata".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::blackhole_metadata()).await?;

        // The rule changes its inline silence for a preset that nobody created.
        let change = OrderDesk::blackhole_metadata().with_action(blackhole.clone());
        let error = workspace.update(change, None).await.unwrap_err();

        assert_eq!(
            error,
            UpdateError::DoesNotFit {
                name: rule.clone().into(),
                errors: vec![WorkspaceError::UnknownReference {
                    definition: rule.clone().into(),
                    reference: blackhole.into(),
                }],
            }
        );
        // The rule is as it was.
        assert_eq!(
            workspace.stores().rules().get(&rule).await?,
            Some(OrderDesk::blackhole_metadata())
        );
        Ok(())
    })
}

#[test]
fn a_selector_does_not_change_into_one_that_a_rule_can_never_fire_with() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let name: RuleName = "blackhole-order-producers".parse()?;
    let order_producers: SelectorName = "order-producers".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace
            .create(Rule::new(name.clone(), order_producers.clone(), blackhole))
            .await?;

        // The selector would test a response field, and the silence of the rule acts before the truth is known.
        let change = OrderDesk::order_producers().with_conditions(OrderDesk::answered_conditions());
        let error = workspace.update(change, None).await.unwrap_err();

        // Nothing is wrong with the selector itself. The error is the rule that uses it.
        assert_eq!(
            error,
            UpdateError::DoesNotFit {
                name: order_producers.clone().into(),
                errors: vec![WorkspaceError::InvalidRule {
                    error: RuleError::LieBeforeTruth {
                        rule: name,
                        lie: LieKind::Silence,
                    },
                }],
            }
        );
        assert_eq!(
            workspace.stores().selectors().get(&order_producers).await?,
            Some(OrderDesk::order_producers())
        );
        Ok(())
    })
}

#[test]
fn a_selector_does_not_change_into_one_that_includes_itself() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let order_producers: SelectorName = "order-producers".parse()?;
    let from_ingest: SelectorName = "order-producers-from-ingest".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::order_producers_from_ingest()).await?;

        // `order-producers-from-ingest` includes `order-producers`, which would include it back.
        let error = workspace
            .update(OrderDesk::order_producers().including(from_ingest.clone()), None)
            .await
            .unwrap_err();

        assert_eq!(
            error,
            UpdateError::DoesNotFit {
                name: order_producers.clone().into(),
                errors: vec![WorkspaceError::InvalidSelectors {
                    error: SelectorSetError::IncludeCycle {
                        path: vec![order_producers.clone(), from_ingest, order_producers],
                    },
                }],
            }
        );
        assert_eq!(
            error.to_string(),
            "selector \"order-producers\" does not fit the workspace: selectors include each other in a circle: \
             \"order-producers\" includes \"order-producers-from-ingest\" includes \"order-producers\""
        );
        Ok(())
    })
}

#[test]
fn what_was_wrong_before_does_not_stand_in_the_way_of_another_change() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule: RuleName = "slow-order-producers".parse()?;

    block_on(async {
        // Someone put a rule into its store by hand. It names a selector and a preset that do not exist.
        workspace
            .stores()
            .rules()
            .create(OrderDesk::slow_order_producers())
            .await?;
        assert!(workspace.validate().await.is_err());

        // Other definitions still come and change: they break nothing.
        workspace.create(OrderDesk::blackhole()).await?;
        workspace
            .update(OrderDesk::blackhole().with_title("Blackhole".parse()?), None)
            .await?;

        // The rule itself does not change while it names nothing.
        let error = workspace
            .update(OrderDesk::slow_order_producers(), None)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, UpdateError::DoesNotFit { name, errors } if *name == rule.clone().into() && errors.len() == 2),
            "{error}"
        );

        // Once what it names exists, it does, and the workspace is valid.
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.update(OrderDesk::slow_order_producers(), None).await?;
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn a_store_that_keeps_no_change_is_why_a_definition_was_not_updated() -> anyhow::Result<()> {
    let presets = MemoryStore::new();
    block_on(presets.create(OrderDesk::blackhole()))?;
    let mut workspace = Workspace::new(Context::over(ReadOnlyPresets::holding(presets)));
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        let change = OrderDesk::blackhole().with_title("Blackhole".parse()?);
        let error = workspace.update(change, None).await.unwrap_err();

        assert_eq!(
            error,
            UpdateError::StoreFailed {
                name: blackhole.clone().into(),
                reason: ReadOnly::<Preset>::REASON.to_owned(),
            }
        );
        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" was not updated: the directory is read-only"
        );
        assert_eq!(
            workspace.stores().presets().get(&blackhole).await?,
            Some(OrderDesk::blackhole())
        );
        Ok(())
    })
}
