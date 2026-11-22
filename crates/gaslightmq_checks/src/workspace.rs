//! The crates of the workspace, as `cargo metadata` describes them.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;

/// The crates of the workspace, and the workspace crates each one depends on.
pub struct Workspace {
    /// The directory of the workspace's `Cargo.toml`.
    pub root: PathBuf,
    pub members: Vec<Member>,
}

/// One crate of the workspace.
pub struct Member {
    /// The package name: `gaslightmq_field`.
    pub name: String,
    /// The directory of its `Cargo.toml`, relative to the workspace root: `crates/gaslightmq_field`.
    pub dir: PathBuf,
    /// The workspace crates it needs to build, without its dev-dependencies.
    pub dependencies: BTreeSet<String>,
}

impl Workspace {
    /// Asks cargo for the workspace that holds the current directory.
    pub fn load() -> Result<Self, WorkspaceError> {
        // `cargo run` names the cargo that started it, which need not be the one on the `PATH`.
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = Command::new(cargo)
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .output()
            .map_err(|error| WorkspaceError::CargoDidNotStart { error })?;
        if !output.status.success() {
            return Err(WorkspaceError::CargoFailed {
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Self::from_metadata(&String::from_utf8_lossy(&output.stdout))
    }

    /// Reads the output of `cargo metadata --no-deps --format-version 1`.
    pub fn from_metadata(json: &str) -> Result<Self, WorkspaceError> {
        let metadata: Metadata = serde_json::from_str(json).map_err(|error| WorkspaceError::Unreadable { error })?;
        let names: BTreeSet<&str> = metadata.packages.iter().map(|package| package.name.as_str()).collect();
        let members = metadata
            .packages
            .iter()
            .map(|package| {
                let manifest_dir = package.manifest_path.parent().unwrap_or(&package.manifest_path);
                let dir = manifest_dir
                    .strip_prefix(&metadata.workspace_root)
                    .unwrap_or(manifest_dir);
                let dependencies = package
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.kind.as_deref() != Some("dev"))
                    .filter(|dependency| names.contains(dependency.name.as_str()))
                    .map(|dependency| dependency.name.clone())
                    .collect();
                Member {
                    name: package.name.clone(),
                    dir: dir.to_path_buf(),
                    dependencies,
                }
            })
            .collect();
        Ok(Workspace {
            root: metadata.workspace_root,
            members,
        })
    }
}

/// What `cargo metadata` prints, as far as the checks read it.
#[derive(Deserialize)]
struct Metadata {
    workspace_root: PathBuf,
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    manifest_path: PathBuf,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    /// `None` for a normal dependency, `"dev"` or `"build"` otherwise.
    kind: Option<String>,
}

/// Why the checks could not learn what the workspace holds.
#[derive(Debug)]
pub enum WorkspaceError {
    CargoDidNotStart { error: io::Error },
    CargoFailed { stderr: String },
    Unreadable { error: serde_json::Error },
}

impl fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CargoDidNotStart { error } => write!(f, "`cargo metadata` did not start: {error}"),
            Self::CargoFailed { stderr } => write!(f, "`cargo metadata` failed: {stderr}"),
            Self::Unreadable { error } => write!(f, "`cargo metadata` printed what the checks cannot read: {error}"),
        }
    }
}

impl std::error::Error for WorkspaceError {}

#[cfg(test)]
mod tests {
    use super::*;

    const METADATA: &str = r#"{
        "workspace_root": "/repo",
        "packages": [
            {
                "name": "gaslightmq_kernel",
                "manifest_path": "/repo/crates/gaslightmq_kernel/Cargo.toml",
                "dependencies": [{ "name": "serde", "kind": null }]
            },
            {
                "name": "gaslightmq_field",
                "manifest_path": "/repo/crates/gaslightmq_field/Cargo.toml",
                "dependencies": [
                    { "name": "gaslightmq_kernel", "kind": null },
                    { "name": "gaslightmq_clock", "kind": "build" },
                    { "name": "gaslightmq_selector", "kind": "dev" },
                    { "name": "serde", "kind": null }
                ]
            },
            {
                "name": "gaslightmq_clock",
                "manifest_path": "/repo/crates/gaslightmq_clock/Cargo.toml",
                "dependencies": []
            },
            {
                "name": "gaslightmq_selector",
                "manifest_path": "/repo/crates/gaslightmq_selector/Cargo.toml",
                "dependencies": []
            }
        ]
    }"#;

    #[test]
    fn a_member_lives_in_a_directory_relative_to_the_root() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        assert_eq!(workspace.root, PathBuf::from("/repo"));
        assert_eq!(workspace.members[1].dir, PathBuf::from("crates/gaslightmq_field"));
    }

    #[test]
    fn a_member_depends_on_the_workspace_crates_it_builds_with() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        let field = &workspace.members[1];
        assert_eq!(field.name, "gaslightmq_field");
        assert_eq!(
            field.dependencies,
            BTreeSet::from(["gaslightmq_clock".to_owned(), "gaslightmq_kernel".to_owned()])
        );
    }

    #[test]
    fn json_without_packages_is_unreadable() {
        let error = Workspace::from_metadata(r#"{ "workspace_root": "/repo" }"#)
            .err()
            .unwrap();

        assert!(matches!(error, WorkspaceError::Unreadable { .. }));
    }
}
