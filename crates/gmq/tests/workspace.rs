//! `gmq proxy` on a workspace whose files do not read: it starts nothing, and names every file that does not read.
//!
//! The files are read before the proxy reaches for its cluster, so these tests need none.

use tokio::process::Command;

/// The binary under test.
const GMQ: &str = env!("CARGO_BIN_EXE_gmq");

/// A cluster that nothing reaches: the proxy never gets that far.
const NO_CLUSTER: &str = "127.0.0.1:9";

#[tokio::test]
async fn a_workspace_whose_files_do_not_read_names_each_of_them_on_a_line_of_its_own() -> anyhow::Result<()> {
    let workspace = tempfile::tempdir()?;
    // A selector with a key that no selector has, and a rule whose trigger fires zero times.
    std::fs::write(
        workspace.path().join("selectors.yaml"),
        "- name: order-producers\n  match:\n    api: Produce\n  colour: red\n",
    )?;
    std::fs::create_dir(workspace.path().join("rules"))?;
    std::fs::write(
        workspace.path().join("rules/orders.yaml"),
        "- name: refuse-orders\n  selector: order-producers\n  action:\n    lie: refusal\n    error: REQUEST_TIMED_OUT\n  trigger:\n    every: 0\n",
    )?;

    let gmq = Command::new(GMQ)
        .args(["proxy", "--upstream", NO_CLUSTER, "--no-control", "--workspace"])
        .arg(workspace.path())
        .output()
        .await?;

    let printed = String::from_utf8(gmq.stderr)?;
    let printed: Vec<&str> = printed.lines().collect();
    assert!(!gmq.status.success());
    assert_eq!(
        printed[0],
        format!(
            "the files of the workspace in {} do not read:",
            workspace.path().display()
        )
    );
    assert!(printed[1].starts_with("selectors.yaml: "), "{printed:?}");
    assert!(printed[2].starts_with("rules/orders.yaml: "), "{printed:?}");
    assert_eq!(printed.len(), 3, "{printed:?}");
    assert_eq!(gmq.stdout, b"");
    Ok(())
}
