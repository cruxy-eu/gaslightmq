//! What the stores of files read, and what they name when a file does not read.

use std::fs;
use std::path::{Path, PathBuf};

use gaslightmq_definition_api::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// The examples of the repository, each a workspace of its own.
const EXAMPLES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples");

/// The directory that example 11 keeps its workspace in, next to the files that deploy it.
const NESTED_WORKSPACE: &str = "gaslight";

#[tokio::test(flavor = "multi_thread")]
async fn the_definitions_of_every_example_read_and_fit_together() -> anyhow::Result<()> {
    let mut examples: Vec<PathBuf> = fs::read_dir(EXAMPLES)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<_, _>>()?;
    examples.retain(|path| path.is_dir());
    examples.sort();
    assert!(!examples.is_empty(), "the repository has examples");

    for example in examples {
        let nested = example.join(NESTED_WORKSPACE);
        let directory = if nested.is_dir() { nested } else { example };
        let stores = FileStores::open(&directory, Access::ReadOnly)
            .await
            .map_err(|error| anyhow::anyhow!("{}: {error}", directory.display()))?;
        let workspace = Workspace::new(FileContext::new(stores));
        workspace
            .validate()
            .await
            .map_err(|error| anyhow::anyhow!("{}: {error}", directory.display()))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_complete_workspace_holds_every_definition_of_its_files() -> anyhow::Result<()> {
    let directory = Path::new(EXAMPLES)
        .join("11-complete-orders-setup")
        .join(NESTED_WORKSPACE);
    let stores = FileStores::open(&directory, Access::ReadOnly).await?;

    let selectors: Vec<String> = stores
        .selectors()
        .list()
        .await?
        .iter()
        .map(|s| s.name().to_string())
        .collect();
    let rules: Vec<String> = stores
        .rules()
        .list()
        .await?
        .iter()
        .map(|r| r.name().to_string())
        .collect();
    let environments = stores.environments().list().await?;

    assert_eq!(selectors, ["group-coordination", "order-fetches", "order-producers"]);
    assert_eq!(
        rules,
        [
            "every-hundredth-order-fetch-hangs",
            "order-fetches-are-slow-for-analytics",
            "orders-sometimes-fail",
            "refuse-with-not-leader",
        ]
    );
    assert_eq!(environments.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_does_not_read_names_itself_and_the_key_that_failed() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::create_dir(workspace.path().join("rules"))?;
    fs::write(
        workspace.path().join("rules").join("orders.yaml"),
        "\
- name: orders-sometimes-fail
  selector: order-producers
  action: error-retriable
  trigger:
    every: 0
",
    )?;

    let error = FileStores::open(workspace.path(), Access::ReadWrite)
        .await
        .expect_err("a count is at least one");

    assert_eq!(
        error.errors,
        [FileError {
            file: Path::new("rules").join("orders.yaml"),
            reason: "[0].trigger.every: a count is at least one".to_owned(),
        }]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn every_file_that_does_not_read_is_named_and_a_uuid_is_a_key_that_no_definition_has() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(
        workspace.path().join("selectors.yaml"),
        "- name: order-fetches\n  uuid: 0b5c4a52-8f3e-4f0e-9a43-46a5c2d1e7f1\n  match:\n    api: Fetch\n",
    )?;
    let rules = workspace.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("fetches.yaml"),
        "- name: blackhole-fetches\n  selector: order-fetches\n  action:\n    lie: silence\n  revision: 3\n",
    )?;

    let error = FileStores::open(workspace.path(), Access::ReadWrite)
        .await
        .expect_err("two keys that no definition has");

    let files: Vec<PathBuf> = error.errors.iter().map(|error| error.file.clone()).collect();
    assert_eq!(
        files,
        [Path::new("selectors.yaml"), &Path::new("rules").join("fetches.yaml")]
    );
    assert!(
        error.errors[0].reason.starts_with("[0].uuid: unknown field `uuid`"),
        "{error}"
    );
    assert!(
        error.errors[1]
            .reason
            .starts_with("[0].revision: unknown field `revision`"),
        "{error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_in_two_files_of_rules_does_not_read() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let rules = workspace.path().join("rules");
    fs::create_dir(&rules)?;
    let rule = "- name: blackhole-fetches\n  selector:\n    api: Fetch\n  action:\n    lie: silence\n";
    fs::write(rules.join("fetches.yaml"), rule)?;
    fs::write(rules.join("more-fetches.yaml"), rule)?;

    let error = FileStores::open(workspace.path(), Access::ReadWrite)
        .await
        .expect_err("one name twice");

    assert_eq!(
        error.to_string(),
        "the files of the workspace do not read: rules/more-fetches.yaml: \
         [0].name: rule \"blackhole-fetches\" is in rules/fetches.yaml already"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_directory_is_an_empty_workspace() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;

    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    assert_eq!(stores.selectors().list().await?, Vec::<Selector>::new());
    assert_eq!(stores.rules().list().await?, Vec::<Rule>::new());
    assert_eq!(stores.environments().list().await?, Vec::<Environment>::new());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_is_no_definition_file_is_not_read() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let rules = workspace.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(rules.join("README.md"), "Rules load disarmed.\n")?;
    fs::write(rules.join(".orders.yaml.swp"), "not YAML at all: [\n")?;
    fs::write(
        rules.join("orders.yaml"),
        "- name: blackhole-fetches\n  selector:\n    api: Fetch\n  action:\n    lie: silence\n",
    )?;

    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    let name: RuleName = "blackhole-fetches".parse()?;
    assert!(stores.rules().get(&name).await?.is_some());
    assert_eq!(stores.rules().list().await?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_that_was_a_boolean_in_old_yaml_stays_a_name() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(
        workspace.path().join("selectors.yaml"),
        "- name: on\n  match:\n    api: Fetch\n",
    )?;

    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    let on: SelectorName = "on".parse()?;
    assert!(stores.selectors().get(&on).await?.is_some());
    Ok(())
}
