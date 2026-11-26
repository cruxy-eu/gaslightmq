//! A workspace over the files takes in an edit by hand when someone reloads it, as one change or not at all.

use std::fs;

use gaslightmq_definition_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
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

const PRESETS: &str = "\
- name: blackhole
  lie: silence

- name: hang-up
  lie: hang-up
";

/// What someone saved while the engine ran: one selector changed, one preset gone, and a file of rules is new.
const SELECTORS_EDITED: &str = "\
- name: order-fetches
  match:
    api: Fetch
    topic: orders

- name: order-producers
  match:
    api: Produce
";
const PRESETS_EDITED: &str = "\
- name: blackhole
  lie: silence
";
const RULES_NEW: &str = "\
- name: blackhole-order-fetches
  selector: order-fetches
  action: blackhole
";

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_takes_in_what_changed_by_name_and_keeps_the_revision_of_the_rest() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));

    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS_EDITED)?;
    fs::create_dir(directory.path().join("rules"))?;
    fs::write(directory.path().join("rules").join("fetches.yaml"), RULES_NEW)?;

    let fetches: SelectorName = "order-fetches".parse()?;
    let producers: SelectorName = "order-producers".parse()?;
    let hang_up: PresetName = "hang-up".parse()?;
    let blackhole_fetches: RuleName = "blackhole-order-fetches".parse()?;
    let reload = workspace.reload().await?;
    assert_eq!(
        reload.changes(),
        [
            DefinitionChange::Updated(fetches.clone().into()),
            DefinitionChange::Deleted(hang_up.clone().into()),
            DefinitionChange::Created(blackhole_fetches.clone().into()),
        ]
    );
    let reloaded = reload.take().await;

    assert_eq!(workspace.revision(fetches.clone()), reloaded);
    assert_eq!(workspace.revision(blackhole_fetches.clone()), reloaded);
    assert_eq!(workspace.revision(producers), workspace.loaded());
    assert!(workspace.stores().presets().get(&hang_up).await?.is_none());
    assert!(workspace.stores().rules().get(&blackhole_fetches).await?.is_some());

    // The store holds what is in the file now, so a write goes through again.
    let edited = workspace
        .stores()
        .selectors()
        .get(&fetches)
        .await?
        .expect("the selector");
    workspace
        .update(edited.with_title("Order fetches".parse()?), None)
        .await?;
    Ok(())
}

/// A scenario that someone added while the engine ran, which names the new rule.
const SCENARIO_NEW: &str = "\
name: fetches-vanish
mode: chain
seed: 42
rules:
  - blackhole-order-fetches
";

#[tokio::test(flavor = "multi_thread")]
async fn a_new_scenario_comes_in_with_a_reload_once_the_rule_it_names_is_there_too() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    let vanish: ScenarioName = "fetches-vanish".parse()?;
    let blackhole_fetches: RuleName = "blackhole-order-fetches".parse()?;

    fs::create_dir(directory.path().join("scenarios"))?;
    fs::write(
        directory.path().join("scenarios").join("fetches-vanish.yaml"),
        SCENARIO_NEW,
    )?;
    let error = workspace.reload().await.expect_err("a rule that does not exist");
    assert_eq!(
        error.to_string(),
        "the definitions that the stores read again do not fit each other: \
         scenario \"fetches-vanish\" refers to rule \"blackhole-order-fetches\", which does not exist"
    );
    assert!(workspace.stores().scenarios().get(&vanish).await?.is_none());

    fs::create_dir(directory.path().join("rules"))?;
    fs::write(directory.path().join("rules").join("fetches.yaml"), RULES_NEW)?;
    let reload = workspace.reload().await?;
    assert_eq!(
        reload.changes(),
        [
            DefinitionChange::Created(blackhole_fetches.into()),
            DefinitionChange::Created(vanish.clone().into()),
        ]
    );
    let reloaded = reload.take().await;

    assert_eq!(workspace.revision(vanish.clone()), reloaded);
    assert!(workspace.stores().scenarios().get(&vanish).await?.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_that_does_not_fit_changes_nothing() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));

    // The new rule names a preset that nobody defined.
    fs::create_dir(directory.path().join("rules"))?;
    fs::write(
        directory.path().join("rules").join("fetches.yaml"),
        RULES_NEW.replace("action: blackhole", "action: delay-1s"),
    )?;

    let error = workspace.reload().await.expect_err("a preset that does not exist");

    assert_eq!(
        error.to_string(),
        "the definitions that the stores read again do not fit each other: \
         rule \"blackhole-order-fetches\" refers to preset \"delay-1s\", which does not exist"
    );
    assert_eq!(workspace.stores().rules().list().await?, Vec::<Rule>::new());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_of_a_file_that_does_not_read_names_the_file_and_the_key() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));

    fs::write(
        directory.path().join("selectors.yaml"),
        "- name: order-fetches\n  armed: true\n  match:\n    api: Fetch\n",
    )?;

    let error = workspace.reload().await.expect_err("armed is no key of a selector");

    let ReloadError::StoreFailed { reason } = error else {
        anyhow::bail!("the store could not read again, and the reload says {error}");
    };
    assert!(
        reason.starts_with(
            "the selector store could not be read again: selectors.yaml: [0].armed: unknown field `armed`"
        ),
        "{reason}"
    );
    assert_eq!(workspace.stores().selectors().list().await?.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_that_nobody_takes_changes_nothing() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    fs::write(directory.path().join("presets.yaml"), PRESETS_EDITED)?;

    let reload = workspace.reload().await?;
    assert_eq!(reload.changes().len(), 1);
    drop(reload);

    assert_eq!(workspace.stores().presets().list().await?.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_holds_what_it_read_again_only_once_it_takes_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let presets = stores.presets();
    fs::write(directory.path().join("presets.yaml"), PRESETS_EDITED)?;

    let read = presets.read_again().await?.expect("a store of files reads again");
    assert_eq!(read.len(), 1);
    assert_eq!(presets.list().await?.len(), 2);

    presets.take_reading().await;
    assert_eq!(presets.list().await?, read);
    Ok(())
}
