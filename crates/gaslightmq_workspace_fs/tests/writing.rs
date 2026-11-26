//! A store of files writes the one definition that changed, and leaves every other byte of its file.

use std::fs;

use gaslightmq_definition_api::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// The selectors of an order desk, as someone wrote them: with comments, and a blank line between them.
const SELECTORS: &str = "\
# Which calls. Rules and filters refer to these by name.

- name: order-producers
  title: Order producers   # shown on the pads
  match:
    api: Produce
    topic: \"orders.*\"

- name: order-fetches
  title: Order fetches
  match:
    api: Fetch
    topic: orders
";

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_of_one_selector_keeps_the_comments_and_the_layout_of_every_other() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    let fetches: SelectorName = "order-fetches".parse()?;
    let all_fetches = Selector::new(fetches.clone(), Conditions::new().with(Condition::is(ApiKey::FETCH)))
        .with_title("Order fetches".parse()?);
    stores.selectors().update(all_fetches.clone()).await?;

    // The other selector, its comments and the blank lines are as they were. The edited one has its written form.
    assert_eq!(
        fs::read_to_string(workspace.path().join("selectors.yaml"))?,
        SELECTORS.replace(
            "  match:\n    api: Fetch\n    topic: orders\n",
            "  match:\n    api: Fetch\n"
        )
    );
    assert_eq!(stores.selectors().get(&fetches).await?, Some(all_fetches));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_definition_written_once_is_written_the_same_way_again() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    let producers: SelectorName = "order-producers".parse()?;
    let selector = stores.selectors().get(&producers).await?.expect("the selector");

    stores.selectors().update(selector.clone()).await?;
    let once = fs::read_to_string(workspace.path().join("selectors.yaml"))?;
    stores.selectors().update(selector).await?;
    let twice = fs::read_to_string(workspace.path().join("selectors.yaml"))?;

    assert_eq!(once, twice);
    assert_eq!(
        once,
        SELECTORS.replace(
            "  title: Order producers   # shown on the pads\n  match:\n    api: Produce\n    topic: \"orders.*\"\n",
            "  title: Order producers\n  match:\n    api: Produce\n    topic: orders.*\n"
        )
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_says_which_definition_holds_a_comment_that_a_write_would_lose() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    let producers: SelectorName = "order-producers".parse()?;
    let fetches: SelectorName = "order-fetches".parse()?;

    assert!(stores.selectors().would_lose_comment(&producers).await?);
    assert!(!stores.selectors().would_lose_comment(&fetches).await?);

    // The comment goes with the write, and the written form holds none.
    let selector = stores.selectors().get(&producers).await?.expect("the selector");
    stores.selectors().update(selector).await?;
    assert!(!stores.selectors().would_lose_comment(&producers).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_selector_comes_after_the_last_one_with_the_same_space_between() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    let metadata = Selector::new(
        "metadata".parse()?,
        Conditions::new().with(Condition::is(ApiKey::METADATA)),
    );
    stores.selectors().create(metadata).await?;

    assert_eq!(
        fs::read_to_string(workspace.path().join("selectors.yaml"))?,
        format!("{SELECTORS}\n- name: metadata\n  match:\n    api: Metadata\n")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_selector_takes_its_lines_and_one_blank_line_with_it() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;

    stores.selectors().delete(&"order-producers".parse()?).await?;

    assert_eq!(
        fs::read_to_string(workspace.path().join("selectors.yaml"))?,
        "# Which calls. Rules and filters refer to these by name.\n\n\
         - name: order-fetches\n  title: Order fetches\n  match:\n    api: Fetch\n    topic: orders\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_rule_gets_a_file_of_its_name_and_goes_with_it() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    let name: RuleName = "blackhole-order-fetches".parse()?;
    let rule = Rule::new(
        name.clone(),
        "order-fetches".parse::<SelectorName>()?,
        LieDefinition::Silence,
    );
    let file = workspace.path().join("rules").join("blackhole-order-fetches.yaml");

    stores.rules().create(rule).await?;
    assert_eq!(
        fs::read_to_string(&file)?,
        "- name: blackhole-order-fetches\n  selector: order-fetches\n  action:\n    lie: silence\n"
    );

    // The file held nothing but the rule, so it goes too.
    stores.rules().delete(&name).await?;
    assert!(!file.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_leaves_no_temporary_file_behind() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    let producers: SelectorName = "order-producers".parse()?;
    let selector = stores.selectors().get(&producers).await?.expect("the selector");

    stores.selectors().update(selector).await?;

    let names: Vec<String> = fs::read_dir(workspace.path())?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    assert_eq!(names, ["selectors.yaml"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_over_the_files_writes_every_change_it_lets_through() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    fs::write(workspace.path().join("selectors.yaml"), SELECTORS)?;
    let stores = FileStores::open(workspace.path(), Access::ReadWrite).await?;
    let mut desk = Workspace::new(FileContext::new(stores));

    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);
    desk.create(blackhole).await?;

    assert_eq!(
        fs::read_to_string(workspace.path().join("presets.yaml"))?,
        "- name: blackhole\n  lie: silence\n"
    );
    Ok(())
}
