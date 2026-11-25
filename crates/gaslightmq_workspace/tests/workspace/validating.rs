//! Validating: what the stores hold fits together, or the error says everything that does not.

use futures::executor::block_on;
use gaslightmq_definition_store_api::prelude::*;
use gaslightmq_definition_store_memory::{KnownUuids, MemoryContext};
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::{Context, OrderDesk, UnreadableRules};

#[test]
fn a_workspace_without_a_definition_is_valid() {
    let workspace = Workspace::new(MemoryContext::new());

    assert_eq!(block_on(workspace.validate()), Ok(()));
}

#[test]
fn definitions_that_someone_wrote_by_hand_are_valid_when_they_fit_each_other() -> anyhow::Result<()> {
    let workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        // The stores get the order desk as it was written, in an order in which nothing is there before it is named.
        let stores = workspace.stores();
        stores.layouts().create(OrderDesk::layout()).await?;
        stores.rules().create(OrderDesk::slow_order_producers()).await?;
        stores.rules().create(OrderDesk::blackhole_metadata()).await?;
        stores.presets().create(OrderDesk::blackhole()).await?;
        stores.presets().create(OrderDesk::delay_1s()).await?;
        stores
            .selectors()
            .create(OrderDesk::order_producers_from_ingest())
            .await?;
        stores.selectors().create(OrderDesk::order_producers()).await?;
        stores.environments().create(OrderDesk::environment()).await?;

        // No definition has a UUID yet, and that is no error.
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn no_two_definitions_share_a_uuid_whatever_their_kinds_are() -> anyhow::Result<()> {
    let workspace = Workspace::new(MemoryContext::new());
    let metadata: RuleName = "blackhole-metadata".parse()?;
    let fetches: RuleName = "blackhole-fetches".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;
    let (created, random) = (KnownUuids::CREATED, KnownUuids::RANDOM);

    block_on(async {
        // Someone copied a rule and gave the copy another name. The UUID came along.
        let original = OrderDesk::blackhole_metadata().with_uuid(RuleUuid::new(created, random));
        let copy = Rule::new(fetches.clone(), original.selector().clone(), original.action().clone())
            .with_uuid(RuleUuid::new(created, random));
        // A preset has the same UUID too.
        let preset = OrderDesk::blackhole().with_uuid(PresetUuid::new(created, random));
        workspace.stores().rules().create(original).await?;
        workspace.stores().rules().create(copy).await?;
        workspace.stores().presets().create(preset).await?;

        let error = workspace.validate().await.unwrap_err();

        // Nobody knows which one is the copy. The first in the order of kinds and names holds the UUID.
        assert_eq!(
            error,
            ValidationError::Invalid {
                errors: vec![
                    WorkspaceError::SharedUuid {
                        definition: fetches.into(),
                        holder: blackhole.clone().into(),
                    },
                    WorkspaceError::SharedUuid {
                        definition: metadata.into(),
                        holder: blackhole.into(),
                    },
                ],
            }
        );
        assert_eq!(
            error.to_string(),
            "the definitions do not fit each other: \
             rule \"blackhole-fetches\" has the UUID of preset \"blackhole\"; \
             rule \"blackhole-metadata\" has the UUID of preset \"blackhole\""
        );
        Ok(())
    })
}

#[test]
fn every_reference_names_a_definition_that_exists() -> anyhow::Result<()> {
    let workspace = Workspace::new(MemoryContext::new());
    let layout = PadLayout::new(
        "order-desk".parse()?,
        ["order-producers".parse()?],
        ["delay-1s".parse()?],
        [Layer::all_clients()],
    )?;

    block_on(async {
        // Nothing defines the selector `order-producers` and the preset `delay-1s`, and three definitions name them.
        workspace
            .stores()
            .selectors()
            .create(OrderDesk::order_producers_from_ingest())
            .await?;
        workspace
            .stores()
            .rules()
            .create(OrderDesk::slow_order_producers())
            .await?;
        workspace.stores().layouts().create(layout).await?;

        let ValidationError::Invalid { errors } = workspace.validate().await.unwrap_err() else {
            panic!("every store reads");
        };

        // The error holds every reference that names nothing, in the order of the kinds.
        let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
        assert_eq!(
            messages,
            [
                "selector \"order-producers-from-ingest\" refers to selector \"order-producers\", \
                 which does not exist",
                "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
                "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist",
                "layout \"order-desk\" refers to selector \"order-producers\", which does not exist",
                "layout \"order-desk\" refers to preset \"delay-1s\", which does not exist",
            ]
        );
        Ok(())
    })
}

#[test]
fn selectors_that_include_each_other_in_a_circle_are_not_valid() -> anyhow::Result<()> {
    let workspace = Workspace::new(MemoryContext::new());
    let order_producers: SelectorName = "order-producers".parse()?;
    let from_ingest: SelectorName = "order-producers-from-ingest".parse()?;

    block_on(async {
        let back = OrderDesk::order_producers().including(from_ingest.clone());
        workspace.stores().selectors().create(back).await?;
        workspace
            .stores()
            .selectors()
            .create(OrderDesk::order_producers_from_ingest())
            .await?;
        // A rule uses one of them. Its selector is what is wrong, and the rule is not named for it.
        workspace.stores().presets().create(OrderDesk::delay_1s()).await?;
        workspace
            .stores()
            .rules()
            .create(OrderDesk::slow_order_producers())
            .await?;

        let error = workspace.validate().await.unwrap_err();

        assert_eq!(
            error,
            ValidationError::Invalid {
                errors: vec![WorkspaceError::InvalidSelectors {
                    error: SelectorSetError::IncludeCycle {
                        path: vec![order_producers.clone(), from_ingest, order_producers],
                    },
                }],
            }
        );
        assert_eq!(
            error.to_string(),
            "the definitions do not fit each other: selectors include each other in a circle: \
             \"order-producers\" includes \"order-producers-from-ingest\" includes \"order-producers\""
        );
        Ok(())
    })
}

#[test]
fn a_rule_is_checked_with_the_selector_and_the_preset_that_it_names() -> anyhow::Result<()> {
    let workspace = Workspace::new(MemoryContext::new());
    let name: RuleName = "blackhole-what-worked".parse()?;
    let answered: SelectorName = "answered".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    block_on(async {
        workspace.stores().selectors().create(OrderDesk::answered()).await?;
        workspace.stores().presets().create(OrderDesk::blackhole()).await?;
        // The preset is a silence, which acts before forwarding, and the selector tests what the backend answered.
        let rule = Rule::new(name.clone(), answered, blackhole);
        workspace.stores().rules().create(rule).await?;

        let error = workspace.validate().await.unwrap_err();

        assert_eq!(
            error,
            ValidationError::Invalid {
                errors: vec![WorkspaceError::InvalidRule {
                    error: RuleError::LieBeforeTruth {
                        rule: name,
                        lie: LieKind::Silence,
                    },
                }],
            }
        );
        Ok(())
    })
}

#[test]
fn an_environment_names_a_world_that_nothing_checks_yet() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        // No store holds worlds yet, so the world of an environment is a name and no reference.
        let environment = workspace.create(OrderDesk::environment()).await?;

        assert_eq!(
            environment.world().map(ToString::to_string).as_deref(),
            Some("orders-sim")
        );
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn a_store_that_cannot_read_is_why_nobody_knows_whether_the_definitions_fit() {
    let workspace = Workspace::new(Context::over(UnreadableRules::new()));

    let error = block_on(workspace.validate()).unwrap_err();

    assert_eq!(
        error,
        ValidationError::StoreFailed {
            reason: "the rule store could not be read: permission denied".to_owned(),
        }
    );
    assert_eq!(
        error.to_string(),
        "the definitions were not validated: the rule store could not be read: permission denied"
    );
}
