//! `gmq proxy --advertised-listeners -`: the proxy reads where its brokers are published from its standard input,
//! before it starts.
//!
//! The input is read before the proxy reaches for its cluster, so these tests need none.

use std::process::Stdio;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// The binary under test.
const GMQ: &str = env!("CARGO_BIN_EXE_gmq");

/// A cluster that nothing reaches: the proxy never gets that far.
const NO_CLUSTER: &str = "127.0.0.1:9";

#[tokio::test]
async fn a_proxy_whose_input_ends_before_it_names_the_advertised_listeners_starts_nothing() -> anyhow::Result<()> {
    let gmq = Command::new(GMQ)
        .args([
            "proxy",
            "--upstream",
            NO_CLUSTER,
            "--no-control",
            "--advertised-listeners",
            "-",
        ])
        .stdin(Stdio::null())
        .output()
        .await?;

    assert!(!gmq.status.success());
    assert_eq!(
        String::from_utf8(gmq.stderr)?,
        "the advertised listeners did not come from the standard input: it ended before its first line\n"
    );
    Ok(())
}

#[tokio::test]
async fn a_proxy_reads_the_advertised_listeners_of_the_first_line_of_its_input() -> anyhow::Result<()> {
    let mut gmq = Command::new(GMQ)
        .args([
            "proxy",
            "--upstream",
            NO_CLUSTER,
            "--no-control",
            "--advertised-listeners",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut input = gmq.stdin.take().ok_or_else(|| anyhow::anyhow!("the input of gmq"))?;

    input.write_all(b"19092=localhost\n").await?;
    drop(input);
    let gmq = gmq.wait_with_output().await?;

    // The line names no port for the address, so the proxy tells why, and starts nothing.
    assert!(!gmq.status.success());
    assert_eq!(
        String::from_utf8(gmq.stderr)?,
        "the advertised listeners of the standard input: advertised listener on port 19092: \
         server address \"localhost\" has no port: write host:port\n"
    );
    Ok(())
}
