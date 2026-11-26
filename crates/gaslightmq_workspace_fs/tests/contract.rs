//! The stores of files do what the store trait of each kind asks, as the stores in memory do.
//!
//! The check asks for a new store for each behaviour, and each one gets a directory of its own that does not exist
//! yet: a store of a new directory holds nothing, and makes the directory with its first write.

use std::sync::atomic::{AtomicUsize, Ordering};

use gaslightmq_environment_conformance::assert_environment_store;
use gaslightmq_rule_conformance::{assert_preset_store, assert_rule_store};
use gaslightmq_selector_conformance::assert_selector_store;
use gaslightmq_surface_conformance::assert_layout_store;
use gaslightmq_workspace_fs::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_store_of_selectors_in_files_keeps_the_contract_and_says_who_includes_a_selector() -> anyhow::Result<()> {
    let workspaces = tempfile::tempdir()?;
    let made = AtomicUsize::new(0);
    let empty = || {
        let workspace = workspaces.path().join(made.fetch_add(1, Ordering::Relaxed).to_string());
        FileStore::new(workspace, Access::ReadWrite)
    };

    assert_selector_store(empty).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_of_presets_in_files_keeps_the_contract() -> anyhow::Result<()> {
    let workspaces = tempfile::tempdir()?;
    let made = AtomicUsize::new(0);
    let empty = || {
        let workspace = workspaces.path().join(made.fetch_add(1, Ordering::Relaxed).to_string());
        FileStore::new(workspace, Access::ReadWrite)
    };

    assert_preset_store(empty).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_of_rules_in_files_keeps_the_contract_and_says_who_uses_a_selector_or_a_preset() -> anyhow::Result<()> {
    let workspaces = tempfile::tempdir()?;
    let made = AtomicUsize::new(0);
    let empty = || {
        let workspace = workspaces.path().join(made.fetch_add(1, Ordering::Relaxed).to_string());
        FileStore::new(workspace, Access::ReadWrite)
    };

    assert_rule_store(empty).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_of_pad_layouts_in_files_keeps_the_contract_and_says_who_has_a_row_or_a_column() -> anyhow::Result<()> {
    let workspaces = tempfile::tempdir()?;
    let made = AtomicUsize::new(0);
    let empty = || {
        let workspace = workspaces.path().join(made.fetch_add(1, Ordering::Relaxed).to_string());
        FileStore::new(workspace, Access::ReadWrite)
    };

    assert_layout_store(empty).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_of_environments_in_files_keeps_the_contract() -> anyhow::Result<()> {
    let workspaces = tempfile::tempdir()?;
    let made = AtomicUsize::new(0);
    let empty = || {
        let workspace = workspaces.path().join(made.fetch_add(1, Ordering::Relaxed).to_string());
        FileStore::new(workspace, Access::ReadWrite)
    };

    assert_environment_store(empty).await;
    Ok(())
}
