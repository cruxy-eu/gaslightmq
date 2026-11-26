//! A store of files never writes over an edit it has not read, and a read-only workspace is never written.

use std::fs;

use gaslightmq_definition_api::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

const SELECTORS: &str = "\
- name: order-fetches
  match:
    api: Fetch

- name: order-producers
  match:
    api: Produce
";

/// What someone saved in an editor while the engine ran.
const EDITED_BY_HAND: &str = "\
- name: order-fetches
  match:
    api: Fetch
    topic: orders   # only the orders

- name: order-producers
  match:
    api: Produce
";

#[tokio::test(flavor = "multi_thread")]
async fn a_write_over_an_edit_by_hand_is_refused_and_changes_nothing() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let file = workspace.path().join("selectors.yaml");
    fs::write(&file, SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    fs::write(&file, EDITED_BY_HAND)?;

    let producers: SelectorName = "order-producers".parse()?;
    let held = stores.selectors().get(&producers).await?.expect("the selector");
    let titled = held.clone().with_title("Order producers".parse()?);
    let refused = stores.selectors().update(titled).await;

    assert_eq!(
        refused,
        Err(StoreChangeError::ChangedOutside {
            reason: "selectors.yaml changed since the store read it".to_owned()
        })
    );
    // The edit by hand is in the file, and the store holds what it read.
    assert_eq!(fs::read_to_string(&file)?, EDITED_BY_HAND);
    assert_eq!(stores.selectors().get(&producers).await?, Some(held));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_check_is_per_file() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    fs::write(workspace.path().join("selectors.yaml"), EDITED_BY_HAND)?;

    // An edit of `selectors.yaml` holds up no other file.
    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);
    stores.presets().create(blackhole).await?;

    assert_eq!(
        fs::read_to_string(workspace.path().join("presets.yaml"))?,
        "- name: blackhole\n  lie: silence\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_is_gone_counts_as_changed() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    fs::remove_file(workspace.path().join("selectors.yaml"))?;

    let refused = stores.selectors().delete(&"order-fetches".parse()?).await;

    assert_eq!(
        refused,
        Err(StoreChangeError::ChangedOutside {
            reason: "selectors.yaml is gone since the store read it".to_owned()
        })
    );
    assert!(!workspace.path().join("selectors.yaml").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_appeared_counts_as_changed() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;

    let metadata = Selector::new(
        "metadata".parse()?,
        Conditions::new().with(Condition::is(ApiKey::METADATA)),
    );
    let refused = stores.selectors().create(metadata).await;

    assert_eq!(
        refused,
        Err(StoreCreateError::ChangedOutside {
            reason: "selectors.yaml exists now, and did not when the store read its files".to_owned()
        })
    );
    assert_eq!(fs::read_to_string(workspace.path().join("selectors.yaml"))?, SELECTORS);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_workspace_reads_and_is_never_written() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadOnly).await?;

    let fetches: SelectorName = "order-fetches".parse()?;
    let held = stores.selectors().get(&fetches).await?.expect("the selector reads");
    let refused = stores
        .selectors()
        .update(held.with_title("Order fetches".parse()?))
        .await;
    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);
    let not_created = stores.presets().create(blackhole).await;

    assert_eq!(
        refused,
        Err(StoreChangeError::Failed {
            reason: "the workspace is read-only".to_owned()
        })
    );
    assert_eq!(
        not_created,
        Err(StoreCreateError::Failed {
            reason: "the workspace is read-only".to_owned()
        })
    );
    assert_eq!(fs::read_to_string(workspace.path().join("selectors.yaml"))?, SELECTORS);
    assert!(!workspace.path().join("presets.yaml").exists());

    // Reading again only reads, so it works on a read-only workspace too.
    fs::write(workspace.path().join("selectors.yaml"), EDITED_BY_HAND)?;
    assert_eq!(stores.selectors().read_again().await?.map(|read| read.len()), Some(2));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_in_flow_style_is_not_edited() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let flow = "[{name: order-fetches, match: {api: Fetch}}]\n";
    fs::write(workspace.path().join("selectors.yaml"), flow)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    let refused = stores.selectors().delete(&"order-fetches".parse()?).await;

    assert_eq!(
        refused,
        Err(StoreChangeError::Failed {
            reason: "selectors.yaml holds its selectors in a list in flow style, `[…]`, which a write does not edit"
                .to_owned()
        })
    );
    assert_eq!(fs::read_to_string(workspace.path().join("selectors.yaml"))?, flow);
    Ok(())
}
