//! A host over the files of a workspace takes in what someone edited by hand when it reloads them, as one change,
//! and tells whoever follows it. A reload of files that do not fit changes nothing, and a read-only workspace
//! reloads and takes no change.
//!
//! What a reload does to the rules that are armed in an environment is in `tests/real_cluster`, which runs them.

use std::fs;

use gaslightmq_definition_api::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// The run of the host of a test. A test names its own, and `gmq` makes one from the randomness of the system.
const RUN: RunId = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);

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

/// What someone saved while the host ran: the fetches of one topic only, and the preset `hang-up` gone.
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

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_takes_an_edit_by_hand_in_as_one_change_and_tells_whoever_follows() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
    );
    let mut changes = host.changes();
    let fetches: SelectorName = "order-fetches".parse()?;
    let producers: SelectorName = "order-producers".parse()?;
    let hang_up: PresetName = "hang-up".parse()?;

    // Someone edits the files while the host runs, and the host goes on with what it read.
    let read = host.workspace().await.stores().selectors().get(&fetches).await?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS_EDITED)?;
    let before = host.workspace().await.stores().presets().get(&hang_up).await?;
    assert!(
        before.is_some(),
        "the host read the files once, and holds what they held"
    );

    let reloaded = host.reload().await?;

    assert_eq!(
        reloaded.changes,
        [
            DefinitionChange::Updated(fetches.clone().into()),
            DefinitionChange::Deleted(hang_up.clone().into()),
        ]
    );
    {
        let workspace = host.workspace().await;
        let edited = workspace.stores().selectors().get(&fetches).await?;
        assert!(
            edited.is_some() && edited != read,
            "the selector is the one of the file now: {edited:?}"
        );
        // The reload is one change: what it changed has its revision, and the rest keeps the one it had.
        assert_eq!(workspace.revision(fetches.clone()), reloaded.revision);
        assert_eq!(workspace.revision(producers), workspace.loaded());
        assert_eq!(workspace.stores().presets().get(&hang_up).await?, None);
    }
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionChanged { name: fetches.into() }
    );
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionDeleted { name: hang_up.into() }
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_of_files_that_do_not_fit_changes_nothing() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
    );
    // Someone saves a rule that names a preset that nobody wrote.
    fs::create_dir(directory.path().join("rules"))?;
    fs::write(
        directory.path().join("rules").join("fetches.yaml"),
        "- name: delay-order-fetches\n  selector: order-fetches\n  action: delay-1s\n",
    )?;

    let refused = host.reload().await;

    let Err(ReloadWorkspaceError::NotReloaded { error }) = refused else {
        anyhow::bail!("a reload took in a rule that names nothing: {refused:?}");
    };
    assert_eq!(
        error.to_string(),
        "the definitions that the stores read again do not fit each other: \
         rule \"delay-order-fetches\" refers to preset \"delay-1s\", which does not exist"
    );
    assert_eq!(
        host.workspace().await.stores().rules().list().await?,
        Vec::<Rule>::new()
    );

    // Once the file names what exists, the next reload takes it in.
    fs::write(
        directory.path().join("rules").join("fetches.yaml"),
        "- name: blackhole-order-fetches\n  selector: order-fetches\n  action: blackhole\n",
    )?;
    let reloaded = host.reload().await?;
    let blackhole_fetches: RuleName = "blackhole-order-fetches".parse()?;
    assert_eq!(reloaded.changes, [DefinitionChange::Created(blackhole_fetches.into())]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_workspace_reloads_and_takes_no_change() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    // CI mounts the workspace read-only, and whoever starts the process says so.
    let stores = FileStores::open(directory.path(), Access::ReadOnly).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
    );
    assert!(host.workspace().await.read_only());

    let quiet = Preset::new("quiet".parse()?, gaslightmq_lie::LieDefinition::Silence);
    let created = host.create(quiet, ChangeSurface::Ui).await;

    let Err(CreateDefinitionError::NotCreated {
        error: CreateError::StoreFailed { reason, .. },
    }) = created
    else {
        anyhow::bail!("a read-only workspace took a new preset: {created:?}");
    };
    assert_eq!(reason, "the workspace is read-only");
    assert!(!directory.path().join("presets.yaml").exists());

    // A reload only reads, so it still takes in what someone changed.
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    let reloaded = host.reload().await?;
    let fetches: SelectorName = "order-fetches".parse()?;
    assert_eq!(reloaded.changes, [DefinitionChange::Updated(fetches.into())]);
    Ok(())
}
