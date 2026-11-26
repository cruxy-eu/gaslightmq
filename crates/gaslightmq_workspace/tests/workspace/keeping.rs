//! Keeping: a live rule that was made outside the workspace keeps the UUID it got when it was made.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::OrderDesk;

#[test]
fn a_live_rule_keeps_its_uuid_when_it_is_kept() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let live = OrderDesk::slow_order_producers();

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        // A pad makes a live rule, which lives in an environment and gets its UUID from the workspace.
        let issued = workspace.live_rule_uuid();
        let uuid = issued.uuid();
        assert_eq!(String::from(uuid), KnownUuids::nth(3));
        assert_eq!(workspace.uuid::<Rule>(live.name()), None);
        assert_eq!(workspace.stores().rules().list().await?, []);

        workspace.keep(live.clone(), issued).await?;

        assert_eq!(workspace.uuid::<Rule>(live.name()), Some(uuid));
        assert_eq!(
            workspace.stores().rules().list().await?,
            [OrderDesk::slow_order_producers()]
        );

        // An update of the kept rule keeps that UUID too.
        let change = live.with_title("Slow ingest".parse()?);
        assert_eq!(workspace.update(change).await?, uuid);
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn a_kept_rule_needs_a_free_name_and_stays_live_with_its_uuid_when_it_has_none() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule = OrderDesk::slow_order_producers();

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        let uuid = workspace.create(rule.clone()).await?;

        let live = workspace.live_rule_uuid();
        let live_uuid = live.uuid();
        let Err(taken) = workspace.keep(rule.clone(), live).await else {
            anyhow::bail!("a live rule was kept under the name of a rule of the workspace");
        };

        assert_eq!(
            taken.error,
            CreateError::NameTaken {
                name: rule.name().clone().into()
            }
        );
        assert_eq!(taken.to_string(), "rule \"slow-order-producers\" exists already");
        // The live rule gets its UUID back, and stays live with it.
        assert_eq!(taken.uuid.uuid(), live_uuid);
        // The rule that was there keeps its UUID.
        assert_eq!(workspace.uuid::<Rule>(rule.name()), Some(uuid));
        assert_eq!(workspace.stores().rules().list().await?, [rule]);
        Ok(())
    })
}
