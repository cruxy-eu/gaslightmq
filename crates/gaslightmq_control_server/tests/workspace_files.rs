//! The control plane over the files of a workspace: a reload that takes in what someone edited, a write over a file
//! that someone edited since, a definition whose write would lose a comment, and a workspace that is read-only.
//!
//! Each test writes the files of a workspace in a temporary directory, starts a host over them, puts a server in
//! front of it on a free port, and speaks to that server with a client, as `gmq` and the UI do.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use gaslightmq_control_api::clock::Duration;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule};
use gaslightmq_control_api::selector::{Selector, SelectorName};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::{Host, HostConfiguration};
use gaslightmq_lie_api::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// How long a test waits for a message of the live stream that the server has a reason to send.
const PATIENCE: Duration = Duration::from_secs(10);

/// The address that the server of a test listens on, and that its client connects to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port, so the tests run side by side.
const ANY_PORT: u16 = 0;

/// The run of the host of a test, as the snapshot writes it. A test names its own run.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// Two selectors, and a comment inside the first one that a write of it would lose.
const SELECTORS: &str = "\
- name: order-fetches
  match:
    api: Fetch   # every fetch, of every topic

- name: order-producers
  match:
    api: Produce
";

/// What someone saved while the process ran: the fetches of one topic only.
const SELECTORS_EDITED: &str = "\
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
async fn a_reload_answers_with_what_it_took_in_and_every_client_hears_of_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;
    let StreamMessage::Snapshot(first) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream did not start with a snapshot");
    };
    assert!(!first.workspace.read_only, "the workspace takes changes");
    let fetches: SelectorName = "order-fetches".parse()?;
    let blackhole: PresetName = "blackhole".parse()?;

    // Someone edits the selectors and takes the presets away while the process runs, and `gmq reload` follows.
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    fs::remove_file(directory.path().join("presets.yaml"))?;
    let reloaded = client.send(ReloadWorkspace { discard_unsaved: false }).await??;

    assert_eq!(
        reloaded.changes,
        [
            DefinitionChange::Updated(fetches.clone().into()),
            DefinitionChange::Deleted(blackhole.clone().into()),
        ]
    );
    let got = client
        .send(GetDefinition::<Selector> { name: fetches.clone() })
        .await??;
    assert_eq!(got.revision, reloaded.revision);
    // The comment went with the edit, so a write of the selector loses nothing any more.
    assert!(!got.loses_comment);
    // Every client hears of each definition that changed or went.
    let mut heard = Vec::new();
    while heard.len() < 2 {
        let message = tokio::time::timeout(PATIENCE, stream.next()).await??;
        if matches!(
            message,
            StreamMessage::DefinitionChanged { .. } | StreamMessage::DefinitionDeleted { .. }
        ) {
            heard.push(message);
        }
    }
    assert_eq!(
        heard,
        [
            StreamMessage::DefinitionChanged {
                definition: AnyDefinition::Selector(got.definition),
                revision: reloaded.revision,
                loses_comment: false,
            },
            StreamMessage::DefinitionDeleted { name: blackhole.into() },
        ]
    );
    server.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_over_a_file_that_someone_edited_since_is_changed_outside_until_a_reload() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let fetches: SelectorName = "order-fetches".parse()?;

    // The selector holds a comment, and the control plane says so beside it, so the UI asks before it saves.
    let read = client
        .send(GetDefinition::<Selector> { name: fetches.clone() })
        .await??;
    assert!(read.loses_comment);

    // Someone edits `selectors.yaml` by hand, and then the UI saves a title for the selector.
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    let titled = read.definition.clone().with_title("Order fetches".parse()?);
    let refused = client
        .send(UpdateDefinition {
            expects: None,
            definition: titled.clone(),
        })
        .await?;

    assert_eq!(
        refused,
        Err(UpdateDefinitionError::ChangedOutside {
            name: fetches.clone().into(),
            reason: "selectors.yaml changed since the store read it".to_owned(),
        })
    );
    assert_eq!(
        fs::read_to_string(directory.path().join("selectors.yaml"))?,
        SELECTORS_EDITED
    );
    // A rule of a new file goes through: the check is per file.
    let blackhole_fetches = Rule::new(
        "blackhole-order-fetches".parse()?,
        fetches.clone(),
        "blackhole".parse::<PresetName>()?,
    );
    client
        .send(CreateDefinition {
            definition: blackhole_fetches,
        })
        .await??;

    // A reload takes the edit in, and the title goes on top of it.
    client.send(ReloadWorkspace { discard_unsaved: false }).await??;
    let edited = client
        .send(GetDefinition::<Selector> { name: fetches.clone() })
        .await??;
    let titled = edited.definition.with_title("Order fetches".parse()?);
    client
        .send(UpdateDefinition {
            expects: None,
            definition: titled,
        })
        .await??;
    server.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_workspace_says_so_and_refuses_every_write() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS)?;
    // CI mounts the workspace read-only, and whoever starts the process says so.
    let stores = FileStores::open(directory.path(), Access::ReadOnly).await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;

    let StreamMessage::Snapshot(state) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream did not start with a snapshot");
    };
    assert!(state.workspace.read_only);
    let deleted = client
        .send(DeleteDefinition::<Selector> {
            name: "order-producers".parse()?,
            expects: None,
        })
        .await?;
    assert_eq!(
        deleted,
        Err(DeleteDefinitionError::StoreFailed {
            name: "order-producers".parse::<SelectorName>()?.into(),
            reason: "the workspace is read-only".to_owned(),
        })
    );
    // A reload only reads, so it still works.
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    let reloaded = client.send(ReloadWorkspace { discard_unsaved: false }).await??;
    assert_eq!(
        reloaded.changes,
        [DefinitionChange::Updated(
            "order-fetches".parse::<SelectorName>()?.into()
        )]
    );
    server.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_that_writes_on_save_holds_its_changes_until_someone_saves_them() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("selectors.yaml"), SELECTORS_EDITED)?;
    fs::write(directory.path().join("presets.yaml"), PRESETS)?;
    // Whoever starts the process says that the workspace writes on save.
    let stores = FileStores::open(directory.path(), Access::WriteOnSave).await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;
    let StreamMessage::Snapshot(first) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream did not start with a snapshot");
    };
    let nothing_unsaved = WorkspaceStatus {
        write_back: WriteBack::OnSave,
        read_only: false,
        unsaved: Vec::new(),
    };
    assert_eq!(first.workspace, nothing_unsaved);
    let blackhole: PresetName = "blackhole".parse()?;

    // Someone deletes the preset in the UI. The file stays as it is, and every client hears what waits for a save.
    client
        .send(DeleteDefinition::<gaslightmq_control_api::rule::Preset> {
            name: blackhole.clone(),
            expects: None,
        })
        .await??;
    let waiting = loop {
        let message = tokio::time::timeout(PATIENCE, stream.next()).await??;
        if let StreamMessage::WorkspaceChanged { workspace } = message {
            break workspace;
        }
    };
    assert_eq!(waiting.unsaved, [DefinitionRef::from(blackhole.clone())]);
    assert!(directory.path().join("presets.yaml").exists());

    // `gmq reload` would let go of the change, so it refuses.
    let refused = client.send(ReloadWorkspace { discard_unsaved: false }).await?;
    assert_eq!(
        refused,
        Err(ReloadWorkspaceError::Unsaved {
            definitions: vec![blackhole.clone().into()],
        })
    );

    // The save writes it, and every client hears that nothing waits any more.
    let saved = client.send(SaveWorkspace {}).await??;

    assert_eq!(saved.saved, [DefinitionRef::from(blackhole)]);
    assert!(!directory.path().join("presets.yaml").exists());
    let after = loop {
        let message = tokio::time::timeout(PATIENCE, stream.next()).await??;
        if let StreamMessage::WorkspaceChanged { workspace } = message {
            break workspace;
        }
    };
    assert_eq!(after, nothing_unsaved);
    server.stop().await;
    Ok(())
}
