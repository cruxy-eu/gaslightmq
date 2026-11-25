//! Keeping: a definition that was made outside the workspace keeps the UUID it was stamped with.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::OrderDesk;

#[test]
fn a_stamped_definition_keeps_its_uuid_when_it_is_kept() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        // A pad makes a live rule, which lives in an environment and gets its UUID from the workspace.
        let live = workspace.stamp(OrderDesk::slow_order_producers());
        assert_eq!(live.uuid().map(String::from), Some(KnownUuids::nth(3)));
        assert_eq!(workspace.stores().rules().list().await?, []);

        let kept = workspace.keep(live.clone()).await?;

        assert_eq!(kept, live);
        assert_eq!(workspace.stores().rules().list().await?, [live]);
        assert_eq!(workspace.validate().await, Ok(()));
        Ok(())
    })
}

#[test]
fn a_kept_definition_needs_a_free_name_and_a_uuid_of_its_own() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());

    block_on(async {
        workspace.create(OrderDesk::order_producers()).await?;
        workspace.create(OrderDesk::delay_1s()).await?;
        let rule = workspace.create(OrderDesk::slow_order_producers()).await?;

        let taken = workspace.keep(OrderDesk::slow_order_producers()).await;
        // Another rule under the UUID of the one that is there.
        let copy = Rule::new(
            "slow-order-producers-again".parse()?,
            rule.selector().clone(),
            rule.action().clone(),
        );
        let copy = rule.uuid().map_or(copy.clone(), |uuid| copy.with_uuid(uuid));
        let shared = workspace.keep(copy).await;

        assert_eq!(
            taken,
            Err(CreateError::NameTaken {
                name: rule.name().clone().into()
            })
        );
        let Err(CreateError::DoesNotFit { errors, .. }) = shared else {
            anyhow::bail!("a rule was kept under the UUID of another: {shared:?}");
        };
        assert!(
            matches!(errors.as_slice(), [WorkspaceError::SharedUuid { .. }]),
            "{errors:?}"
        );
        assert_eq!(workspace.stores().rules().list().await?, [rule]);
        Ok(())
    })
}
