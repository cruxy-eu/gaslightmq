//! A workspace that writes on save holds every change in memory, and one save writes every file that a change
//! touched, or none of them when someone edited one since.

use std::fs;

use gaslightmq_definition_api::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// The selectors of an order desk, as someone wrote them.
const SELECTORS: &str = "\
- name: order-fetches
  match:
    api: Fetch
    topic: orders

- name: order-producers
  match:
    api: Produce
";

const PRESETS: &str = "\
- name: blackhole
  lie: silence
";

#[tokio::test(flavor = "multi_thread")]
async fn a_change_waits_in_memory_until_one_save_writes_every_file_it_touched() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    assert_eq!(workspace.write_back(), WriteBack::OnSave);
    let fetches: SelectorName = "order-fetches".parse()?;
    let every_fetch = Selector::new(fetches.clone(), Conditions::new().with(Condition::is(ApiKey::FETCH)));
    let blackhole_fetches = Rule::new(
        "blackhole-order-fetches".parse()?,
        fetches.clone(),
        "blackhole".parse::<PresetName>()?,
    );

    // Someone edits a selector and writes a new rule in the UI. The workspace holds both, and no file changed.
    let updated = workspace.update(every_fetch.clone(), None).await?;
    workspace.create(blackhole_fetches.clone()).await?;

    assert_eq!(workspace.stores().selectors().get(&fetches).await?, Some(every_fetch));
    assert_eq!(fs::read_to_string(directory.path().join("selectors.yaml"))?, SELECTORS);
    assert!(!directory.path().join("rules").exists());
    let unsaved = [
        DefinitionName::from(fetches.clone()),
        DefinitionName::from(blackhole_fetches.name().clone()),
    ];
    assert_eq!(workspace.unsaved().await, unsaved);

    let saved = workspace.save().await?;

    assert_eq!(saved, unsaved);
    assert_eq!(
        fs::read_to_string(directory.path().join("selectors.yaml"))?,
        SELECTORS.replace("    api: Fetch\n    topic: orders\n", "    api: Fetch\n")
    );
    assert_eq!(
        fs::read_to_string(directory.path().join("rules").join("blackhole-order-fetches.yaml"))?,
        "- name: blackhole-order-fetches\n  selector: order-fetches\n  action: blackhole\n"
    );
    assert_eq!(workspace.unsaved().await, Vec::<DefinitionName>::new());
    // A save changes no definition, so the selector keeps the revision of its update.
    assert_eq!(workspace.revision(fetches), updated);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_save_writes_no_file_when_someone_edited_one_of_them_since() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    let producers: SelectorName = "order-producers".parse()?;
    let titled = Selector::new(
        producers.clone(),
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    )
    .with_title("Order producers".parse()?);
    workspace.update(titled, None).await?;
    let blackhole: PresetName = "blackhole".parse()?;
    workspace.delete(blackhole.clone(), None).await?;

    // Someone edits `presets.yaml` by hand before the save.
    let by_hand = format!("{PRESETS}\n- name: hang-up\n  lie: hang-up\n");
    fs::write(directory.path().join("presets.yaml"), &by_hand)?;
    let refused = workspace.save().await;

    assert_eq!(
        refused,
        Err(SaveError::ChangedOutside {
            reason: "presets.yaml changed since the store read it".to_owned(),
        })
    );
    // The save wrote neither file, and the workspace still holds both changes.
    assert_eq!(fs::read_to_string(directory.path().join("selectors.yaml"))?, SELECTORS);
    assert_eq!(fs::read_to_string(directory.path().join("presets.yaml"))?, by_hand);
    assert_eq!(
        workspace.unsaved().await,
        [DefinitionName::from(producers), DefinitionName::from(blackhole)]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_lets_go_of_what_nobody_saved() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    let fetches: SelectorName = "order-fetches".parse()?;
    let read = workspace.stores().selectors().get(&fetches).await?;
    let every_fetch = Selector::new(fetches.clone(), Conditions::new().with(Condition::is(ApiKey::FETCH)));
    workspace.update(every_fetch, None).await?;

    // The reload compares the files with what the workspace holds: the edit that nobody saved goes back.
    let reload = workspace.reload().await?;
    assert_eq!(reload.changes(), [DefinitionChange::Updated(fetches.clone().into())]);
    reload.take().await;

    assert_eq!(workspace.stores().selectors().get(&fetches).await?, read);
    assert_eq!(workspace.unsaved().await, Vec::<DefinitionName>::new());
    assert_eq!(workspace.save().await?, Vec::<DefinitionName>::new());
    assert_eq!(fs::read_to_string(directory.path().join("selectors.yaml"))?, SELECTORS);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rule_deleted_before_the_save_takes_its_file_away_with_the_save() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let rules = directory.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("fetches.yaml"),
        "- name: blackhole-order-fetches\n  selector: order-fetches\n  action: blackhole\n",
    )?;
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    let blackhole_fetches: RuleName = "blackhole-order-fetches".parse()?;

    workspace.delete(blackhole_fetches.clone(), None).await?;
    assert!(
        rules.join("fetches.yaml").exists(),
        "nothing is written before the save"
    );
    let saved = workspace.save().await?;

    assert_eq!(saved, [DefinitionName::from(blackhole_fetches)]);
    assert!(!rules.join("fetches.yaml").exists());
    Ok(())
}

/// A save that stops during its renames says what it saved, and keeps the rest waiting.
///
/// The directory of the rules takes no change, so the rule cannot go, and the selector before it is saved already.
/// A system that runs the test as a user who may change anything takes no such refusal, and the test needs Unix
/// permissions.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_save_that_stops_during_its_renames_names_what_it_saved_and_keeps_the_rest() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let rules = directory.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("fetches.yaml"),
        "- name: blackhole-order-fetches\n  selector: order-fetches\n  action: blackhole\n",
    )?;
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let mut workspace = Workspace::new(FileContext::new(stores));
    let producers: SelectorName = "order-producers".parse()?;
    let titled = Selector::new(
        producers.clone(),
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    )
    .with_title("Order producers".parse()?);
    let blackhole_fetches: RuleName = "blackhole-order-fetches".parse()?;
    workspace.update(titled, None).await?;
    workspace.delete(blackhole_fetches.clone(), None).await?;

    // Nobody may take a file out of the directory of the rules any more.
    let writable = fs::metadata(&rules)?.permissions();
    fs::set_permissions(&rules, fs::Permissions::from_mode(0o555))?;
    let stopped = workspace.save().await;
    fs::set_permissions(&rules, writable)?;

    let Err(SaveError::PartlySaved { saved, reason }) = stopped else {
        anyhow::bail!("a save that could not take a file away did not stop: {stopped:?}");
    };
    // The selector came before the rule, and its file holds it. The rule still waits, and its file is there.
    assert_eq!(saved, [DefinitionName::from(producers)]);
    assert!(
        reason.starts_with("the rule store did not save: rules/fetches.yaml: "),
        "{reason}"
    );
    assert!(fs::read_to_string(directory.path().join("selectors.yaml"))?.contains("title: Order producers"));
    assert!(rules.join("fetches.yaml").exists());
    assert_eq!(
        workspace.unsaved().await,
        [DefinitionName::from(blackhole_fetches.clone())]
    );

    // The next save writes what waited.
    assert_eq!(workspace.save().await?, [DefinitionName::from(blackhole_fetches)]);
    assert!(!rules.join("fetches.yaml").exists());
    Ok(())
}
