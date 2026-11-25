//! Creating: a definition gets a UUID, its name is free, and everything it names exists.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_definition_memory::{KnownUuids, MemoryContext, MemoryStore};
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::{Context, OrderDesk, ReadOnly, ReadOnlyPresets, UnreadableRules};

#[test]
fn a_definition_gets_a_uuid_when_it_is_created() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let written = OrderDesk::order_producers();
    assert_eq!(written.uuid(), None);

    block_on(async {
        let created = workspace.create(written.clone()).await?;

        // The definition is as it was written, and it has the first UUID of the source.
        assert_eq!(created.uuid().map(String::from), Some(KnownUuids::nth(1)));
        assert_eq!(created.clone().without_uuid(), written);
        // Its store holds it with that UUID.
        assert_eq!(workspace.stores().selectors().list().await?, [created]);
        Ok(())
    })
}

#[test]
fn every_kind_of_definition_gets_a_uuid_of_its_own() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        let order_producers = workspace.create(OrderDesk::order_producers()).await?;
        let from_ingest = workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        let delay = workspace.create(OrderDesk::delay_1s()).await?;
        let blackhole = workspace.create(OrderDesk::blackhole()).await?;
        let rule = workspace.create(OrderDesk::slow_order_producers()).await?;
        let layout = workspace.create(OrderDesk::layout()).await?;
        let environment = workspace.create(OrderDesk::environment()).await?;

        let uuids = [
            order_producers.uuid().map(String::from),
            from_ingest.uuid().map(String::from),
            delay.uuid().map(String::from),
            blackhole.uuid().map(String::from),
            rule.uuid().map(String::from),
            layout.uuid().map(String::from),
            environment.uuid().map(String::from),
        ];
        let made_in_order = (1..=7).map(|nth| Some(KnownUuids::nth(nth)));
        assert!(uuids.into_iter().eq(made_in_order));

        // Each definition is in the store of its kind.
        assert_eq!(workspace.stores().presets().list().await?, [blackhole, delay]);
        assert_eq!(workspace.stores().rules().list().await?, [rule]);
        assert_eq!(workspace.stores().layouts().list().await?, [layout]);
        assert_eq!(workspace.stores().environments().list().await?, [environment]);
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn a_copy_of_a_definition_gets_a_new_uuid() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        let original = workspace.create(OrderDesk::blackhole_metadata()).await?;

        // Someone copies the rule under another name. The UUID comes along with the copy.
        let copy = Rule::new(
            "blackhole-metadata-again".parse()?,
            original.selector().clone(),
            original.action().clone(),
        );
        let copy = original.uuid().map_or(copy.clone(), |uuid| copy.with_uuid(uuid));
        assert_eq!(copy.uuid(), original.uuid());

        let created = workspace.create(copy).await?;

        assert_eq!(created.uuid().map(String::from), Some(KnownUuids::nth(2)));
        assert_ne!(created.uuid(), original.uuid());
        Ok(())
    })
}

#[test]
fn create_refuses_a_name_that_is_taken() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        let first = workspace.create(OrderDesk::blackhole()).await?;

        // Another preset wants the name: a hang-up instead of a silence.
        let other = Preset::new(blackhole.clone(), HangUpLie::new());
        let error = workspace.create(other).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::NameTaken {
                name: blackhole.clone().into()
            }
        );
        assert_eq!(error.to_string(), "preset \"blackhole\" exists already");
        // Nothing overwrote the preset that was there.
        assert_eq!(workspace.stores().presets().get(&blackhole).await?, Some(first));
        Ok(())
    })
}

#[test]
fn two_kinds_can_share_a_name() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let selector: SelectorName = "metadata".parse()?;
    let preset: PresetName = "metadata".parse()?;
    let rule: RuleName = "metadata".parse()?;

    block_on(async {
        workspace
            .create(Selector::new(selector.clone(), Conditions::new()))
            .await?;
        workspace
            .create(Preset::new(preset.clone(), LieDefinition::Silence))
            .await?;

        // Every reference says which kind it means, so the rule finds its selector and its preset.
        let created = workspace
            .create(Rule::new(rule, selector.clone(), preset.clone()))
            .await?;

        assert_eq!(created.selector().name(), Some(&selector));
        assert_eq!(created.action().preset(), Some(&preset));
        Ok(())
    })
}

#[test]
fn a_rule_names_a_selector_and_a_preset_that_exist() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule: RuleName = "slow-order-producers".parse()?;
    let selector: SelectorName = "order-producers".parse()?;
    let preset: PresetName = "delay-1s".parse()?;

    block_on(async {
        // Nobody created the selector and the preset that the rule names.
        let error = workspace.create(OrderDesk::slow_order_producers()).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::DoesNotFit {
                name: rule.clone().into(),
                errors: vec![
                    WorkspaceError::UnknownReference {
                        definition: rule.clone().into(),
                        reference: selector.into(),
                    },
                    WorkspaceError::UnknownReference {
                        definition: rule.clone().into(),
                        reference: preset.into(),
                    },
                ],
            }
        );
        assert_eq!(
            error.to_string(),
            "rule \"slow-order-producers\" does not fit the workspace: \
             rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist; \
             rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist"
        );
        assert_eq!(workspace.stores().rules().list().await?, []);

        // With both of them, the same rule fits.
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        workspace.create(OrderDesk::slow_order_producers()).await?;
        Ok(())
    })
}

#[test]
fn a_selector_includes_a_selector_that_exists() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        let error = workspace
            .create(OrderDesk::order_producers_from_ingest())
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "selector \"order-producers-from-ingest\" does not fit the workspace: \
             selector \"order-producers-from-ingest\" refers to selector \"order-producers\", which does not exist"
        );

        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        Ok(())
    })
}

#[test]
fn a_layout_has_rows_and_columns_that_exist() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let layout: LayoutName = "order-desk".parse()?;
    let missing_row: SelectorName = "order-producers-from-ingest".parse()?;
    let missing_column: PresetName = "blackhole".parse()?;

    block_on(async {
        // One of the two rows and one of the two columns exist.
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;

        let error = workspace.create(OrderDesk::layout()).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::DoesNotFit {
                name: layout.clone().into(),
                errors: vec![
                    WorkspaceError::UnknownReference {
                        definition: layout.clone().into(),
                        reference: missing_row.into(),
                    },
                    WorkspaceError::UnknownReference {
                        definition: layout.clone().into(),
                        reference: missing_column.into(),
                    },
                ],
            }
        );

        workspace.create(OrderDesk::order_producers_from_ingest()).await?;
        workspace.create(OrderDesk::blackhole()).await?;
        workspace.create(OrderDesk::layout()).await?;
        Ok(())
    })
}

#[test]
fn a_rule_that_can_never_fire_is_not_created() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let name: RuleName = "blackhole-what-worked".parse()?;
    let answered: SelectorName = "answered".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::answered()).await?;
        workspace.create(OrderDesk::blackhole()).await?;

        // The rule only knows which calls it means once the backend answered, and a silence acts before that.
        let rule = Rule::new(name.clone(), answered, blackhole);
        let error = workspace.create(rule).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::DoesNotFit {
                name: name.clone().into(),
                errors: vec![WorkspaceError::InvalidRule {
                    error: RuleError::LieBeforeTruth {
                        rule: name.clone(),
                        lie: LieKind::Silence,
                    },
                }],
            }
        );
        assert_eq!(
            error.to_string(),
            "rule \"blackhole-what-worked\" does not fit the workspace: \
             rule \"blackhole-what-worked\" tests a response field, and its silence acts before forwarding, \
             when the truth is not known yet. Give the lie `stage: after`"
        );
        assert_eq!(workspace.stores().rules().list().await?, []);
        Ok(())
    })
}

#[test]
fn a_uuid_that_another_definition_has_is_not_given_twice() -> anyhow::Result<()> {
    // This source repeats itself: every seed is the first one.
    let stuck = || UuidSeed::new(KnownUuids::CREATED, KnownUuids::RANDOM);
    let mut workspace = Workspace::new(MemoryContext::with_uuids(stuck));
    let order_producers: SelectorName = "order-producers".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;

        // The preset would get the UUID of the selector. A UUID is there once, whatever the kinds are.
        let error = workspace.create(OrderDesk::blackhole()).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::DoesNotFit {
                name: blackhole.clone().into(),
                errors: vec![WorkspaceError::SharedUuid {
                    definition: blackhole.into(),
                    holder: order_producers.into(),
                }],
            }
        );
        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" does not fit the workspace: \
             preset \"blackhole\" has the UUID of selector \"order-producers\""
        );
        assert_eq!(workspace.stores().presets().list().await?, []);
        Ok(())
    })
}

#[test]
fn a_store_that_keeps_nothing_new_is_why_a_definition_was_not_created() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(Context::over(ReadOnlyPresets::holding(MemoryStore::new())));
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        let error = workspace.create(OrderDesk::blackhole()).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::StoreFailed {
                name: blackhole.into(),
                reason: ReadOnly::<Preset>::REASON.to_owned(),
            }
        );
        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" was not created: the directory is read-only"
        );

        // The other stores take what they are given.
        workspace.create(OrderDesk::order_producers()).await?;
        Ok(())
    })
}

#[test]
fn a_store_that_cannot_read_is_why_no_definition_of_any_kind_is_created() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(Context::over(UnreadableRules::new()));
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        // Nobody knows which rules there are, so nobody knows whether a definition fits.
        let error = workspace.create(OrderDesk::blackhole()).await.unwrap_err();

        assert_eq!(
            error,
            CreateError::StoreFailed {
                name: blackhole.into(),
                reason: "the rule store could not be read: permission denied".to_owned(),
            }
        );
        assert_eq!(
            error.to_string(),
            "preset \"blackhole\" was not created: the rule store could not be read: permission denied"
        );
        assert_eq!(workspace.stores().presets().list().await?, []);
        Ok(())
    })
}

#[test]
fn a_change_is_a_future_that_another_thread_can_drive() {
    fn assert_send<T: Send>(_: &T) {}

    /// A host holds its workspace over whichever context it is handed,
    /// and hands every request to a task of its runtime, which may run on any thread.
    fn every_change_is_send<C: WorkspaceContext>(workspace: &mut Workspace<C>) {
        assert_send(&workspace.create(OrderDesk::blackhole()));
        assert_send(&workspace.update(OrderDesk::blackhole()));
        assert_send(&workspace.delete(OrderDesk::blackhole().name().clone()));
        assert_send(&workspace.validate());
    }

    every_change_is_send(&mut Workspace::new(MemoryContext::new()));
}
