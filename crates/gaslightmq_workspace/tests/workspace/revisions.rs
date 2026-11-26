//! Revisions: every change takes the next number, and a change can name the revision it expects.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_workspace::prelude::*;

use crate::support::OrderDesk;

#[test]
fn a_definition_that_nobody_changed_since_the_load_has_the_revision_of_the_load() -> anyhow::Result<()> {
    let context = MemoryContext::new();
    // The stores hold a preset before the workspace loads, as the files of a workspace do.
    block_on(context.stores().presets().create(OrderDesk::blackhole()))?;

    let workspace = Workspace::new(context);

    assert_eq!(workspace.loaded(), RevisionCounter::FIRST);
    assert_eq!(
        workspace.revision(OrderDesk::blackhole().name().clone()),
        workspace.loaded()
    );
    Ok(())
}

#[test]
fn every_change_takes_the_next_number_whatever_it_changes() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();
    let delay = OrderDesk::delay_1s();

    block_on(async {
        let created = workspace.create(blackhole.clone()).await?;
        let created_next = workspace.create(delay.clone()).await?;
        let updated = workspace
            .update(blackhole.clone().with_title("Blackhole".parse()?), None)
            .await?;
        let deleted = workspace.delete(delay.name().clone(), None).await?;

        // A higher revision is a later change, across the definitions.
        assert!(workspace.loaded() < created, "{created}");
        assert!(created < created_next, "{created} and then {created_next}");
        assert!(created_next < updated, "{created_next} and then {updated}");
        assert!(updated < deleted, "{updated} and then {deleted}");
        // Each definition has the revision of its own last change.
        assert_eq!(workspace.revision(blackhole.name().clone()), updated);
        Ok(())
    })
}

#[test]
fn an_update_that_expects_the_revision_of_the_definition_changes_it() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        let seen = workspace.create(blackhole.clone()).await?;

        let change = blackhole.clone().with_title("Blackhole".parse()?);
        let updated = workspace.update(change.clone(), Some(seen)).await?;

        assert!(updated > seen, "{seen} and then {updated}");
        assert_eq!(workspace.revision(blackhole.name().clone()), updated);
        assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, Some(change));
        Ok(())
    })
}

#[test]
fn an_update_that_expects_another_revision_changes_nothing() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        // Two people read the preset at the same revision.
        let seen = workspace.create(blackhole.clone()).await?;
        // The first one gives it a title.
        let titled = blackhole.clone().with_title("Blackhole".parse()?);
        let now = workspace.update(titled.clone(), Some(seen)).await?;

        // The second one changes what they saw, which is no longer what the workspace holds.
        let late = blackhole.clone().with_title("Silence".parse()?);
        let error = workspace.update(late, Some(seen)).await.unwrap_err();

        assert_eq!(
            error,
            UpdateError::Changed {
                name: blackhole.name().clone().into(),
                revision: now,
            }
        );
        assert_eq!(
            error.to_string(),
            format!("preset \"blackhole\" changed since: it has revision {now} now")
        );
        // The preset is as the first one left it, at the revision of that change.
        assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, Some(titled));
        assert_eq!(workspace.revision(blackhole.name().clone()), now);
        Ok(())
    })
}

#[test]
fn a_delete_that_expects_another_revision_removes_nothing() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        let seen = workspace.create(blackhole.clone()).await?;
        let titled = blackhole.clone().with_title("Blackhole".parse()?);
        let now = workspace.update(titled.clone(), None).await?;

        let error = workspace
            .delete(blackhole.name().clone(), Some(seen))
            .await
            .unwrap_err();

        assert_eq!(
            error,
            DeleteError::Changed {
                name: blackhole.name().clone().into(),
                revision: now,
            }
        );
        assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, Some(titled));

        // Whoever saw the title deletes it.
        workspace.delete(blackhole.name().clone(), Some(now)).await?;
        assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, None);
        Ok(())
    })
}

#[test]
fn a_definition_that_comes_back_under_its_name_has_a_higher_revision_than_the_one_that_went() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        let gone = workspace.create(blackhole.clone()).await?;
        let deleted = workspace.delete(blackhole.name().clone(), Some(gone)).await?;
        // Someone makes a preset under the same name. It is another preset.
        let back = workspace.create(blackhole.clone()).await?;

        assert!(gone < deleted && deleted < back, "{gone}, {deleted} and then {back}");
        // A change that still expects the preset that went changes nothing.
        let late = blackhole.clone().with_title("Blackhole".parse()?);
        assert_eq!(
            workspace.update(late, Some(gone)).await,
            Err(UpdateError::Changed {
                name: blackhole.name().clone().into(),
                revision: back,
            })
        );
        assert_eq!(
            workspace.delete(blackhole.name().clone(), Some(gone)).await,
            Err(DeleteError::Changed {
                name: blackhole.name().clone().into(),
                revision: back,
            })
        );
        Ok(())
    })
}

#[test]
fn a_change_that_expects_a_revision_of_a_definition_that_went_finds_no_definition() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        let gone = workspace.create(blackhole.clone()).await?;
        workspace.delete(blackhole.name().clone(), None).await?;

        assert_eq!(
            workspace.update(blackhole.clone(), Some(gone)).await,
            Err(UpdateError::NotFound {
                name: blackhole.name().clone().into()
            })
        );
        assert_eq!(
            workspace.delete(blackhole.name().clone(), Some(gone)).await,
            Err(DeleteError::NotFound {
                name: blackhole.name().clone().into()
            })
        );
        Ok(())
    })
}

#[test]
fn without_a_revision_the_last_change_wins() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        workspace.create(blackhole.clone()).await?;
        workspace
            .update(blackhole.clone().with_title("Blackhole".parse()?), None)
            .await?;

        // Whoever names no revision changes the preset as it is, whoever changed it before.
        let last = blackhole.clone().with_title("Silence".parse()?);
        workspace.update(last.clone(), None).await?;

        assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, Some(last));
        Ok(())
    })
}

#[test]
fn a_change_that_fails_leaves_the_revision_of_the_definition() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    let rule = OrderDesk::blackhole_metadata();
    let unknown: PresetName = "blackhole".parse()?;

    block_on(async {
        let seen = workspace.create(rule.clone()).await?;

        // The rule changes its inline silence for a preset that nobody created.
        let refused = workspace.update(rule.clone().with_action(unknown), Some(seen)).await;

        assert!(matches!(refused, Err(UpdateError::DoesNotFit { .. })), "{refused:?}");
        // What its sender saw is still what the workspace holds.
        assert_eq!(workspace.revision(rule.name().clone()), seen);
        Ok(())
    })
}

#[test]
fn a_change_outside_the_workspace_takes_its_number_from_the_same_counter() -> anyhow::Result<()> {
    let mut workspace = Workspace::new(MemoryContext::new());
    // Whoever holds live state takes the numbers of its changes from the counter of the workspace.
    let counter = workspace.counter().clone();
    let blackhole = OrderDesk::blackhole();

    block_on(async {
        let created = workspace.create(blackhole.clone()).await?;
        let armed_somewhere = counter.next_revision();
        let updated = workspace
            .update(blackhole.clone().with_title("Blackhole".parse()?), Some(created))
            .await?;

        assert!(
            created < armed_somewhere && armed_somewhere < updated,
            "{created}, {armed_somewhere} and then {updated}"
        );
        Ok(())
    })
}
