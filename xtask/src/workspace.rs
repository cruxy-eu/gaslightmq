//! The crates of the workspace, as `cargo metadata` describes them.

use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;

/// The directory of the workspace's tooling, which is not product code: the checks themselves.
///
/// Its crates need no row in `crates.md`, and they are the one exception to the rule that a directory carries a crate's name.
pub const TOOLING: &str = "xtask";

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
    /// The packages from outside the workspace that it names as a dependency, without its dev-dependencies.
    /// An optional one counts, and so does one for a single target.
    pub outside_dependencies: BTreeSet<String>,
    /// The packages that only its tests, examples and benchmarks need, from the workspace or from outside.
    pub dev_dependencies: BTreeSet<String>,
    /// The Rust edition it builds with: `2024`.
    pub edition: String,
    /// The modules it declares sans-IO in its manifest, each as a path from the crate root: `logic`, `logic::cluster`.
    ///
    /// The manifest lists them as `sans_io_modules` under `[package.metadata.gaslightmq]`.
    pub sans_io_modules: Vec<String>,
}

impl Member {
    /// Whether the crate is tooling rather than product code.
    pub fn is_tooling(&self) -> bool {
        self.dir.starts_with(TOOLING)
    }
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
                let (dev, built_with): (Vec<&Dependency>, Vec<&Dependency>) = package
                    .dependencies
                    .iter()
                    .partition(|dependency| dependency.kind.as_deref() == Some("dev"));
                let (dependencies, outside_dependencies) = built_with
                    .into_iter()
                    .map(|dependency| dependency.name.clone())
                    .partition(|name| names.contains(name.as_str()));
                let declared = package
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.gaslightmq.as_ref());
                Member {
                    name: package.name.clone(),
                    dir: dir.to_path_buf(),
                    dependencies,
                    outside_dependencies,
                    dev_dependencies: dev.into_iter().map(|dependency| dependency.name.clone()).collect(),
                    edition: package.edition.clone(),
                    sans_io_modules: declared.map_or_else(Vec::new, |declared| declared.sans_io_modules.clone()),
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
    edition: String,
    dependencies: Vec<Dependency>,
    /// The `[package.metadata]` table of the manifest, which cargo leaves to tools.
    metadata: Option<PackageMetadata>,
}

/// The tables of `[package.metadata]`, of which the checks read their own.
#[derive(Deserialize)]
struct PackageMetadata {
    gaslightmq: Option<Declared>,
}

/// What a crate declares to the checks, under `[package.metadata.gaslightmq]`.
/// A key that the checks do not know is a slip, and fails, so that a declaration never goes unread.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Declared {
    #[serde(default)]
    sans_io_modules: Vec<String>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    /// `None` for a normal dependency, `"dev"` or `"build"` otherwise.
    kind: Option<String>,
}

/// Why the checks could not learn what the workspace holds.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("`cargo metadata` did not start: {error}")]
    CargoDidNotStart { error: io::Error },
    #[error("`cargo metadata` failed: {stderr}")]
    CargoFailed { stderr: String },
    #[error("`cargo metadata` printed what the checks cannot read: {error}")]
    Unreadable { error: serde_json::Error },
}

#[cfg(test)]
impl Member {
    /// A crate in `crates/<name>` that depends on nothing and declares nothing, for the tests of the checks.
    pub fn alone(name: &str) -> Member {
        Member {
            name: name.to_owned(),
            dir: PathBuf::from("crates").join(name),
            dependencies: BTreeSet::new(),
            outside_dependencies: BTreeSet::new(),
            dev_dependencies: BTreeSet::new(),
            edition: "2024".to_owned(),
            sans_io_modules: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const METADATA: &str = r#"{
        "workspace_root": "/repo",
        "packages": [
            {
                "name": "gaslightmq_kernel",
                "manifest_path": "/repo/crates/gaslightmq_kernel/Cargo.toml",
                "edition": "2024",
                "metadata": null,
                "dependencies": [
                    { "name": "serde", "kind": null },
                    { "name": "serde_json", "kind": "dev" }
                ]
            },
            {
                "name": "gaslightmq_field",
                "manifest_path": "/repo/crates/gaslightmq_field/Cargo.toml",
                "edition": "2021",
                "metadata": { "gaslightmq": { "sans_io_modules": ["logic", "logic::cluster"] }, "docs": { "rs": {} } },
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
                "edition": "2024",
                "dependencies": []
            },
            {
                "name": "gaslightmq_selector",
                "manifest_path": "/repo/crates/gaslightmq_selector/Cargo.toml",
                "edition": "2024",
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
    fn a_member_names_the_packages_from_outside_the_workspace_it_builds_with() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        let kernel = &workspace.members[0];
        assert_eq!(kernel.outside_dependencies, BTreeSet::from(["serde".to_owned()]));
    }

    #[test]
    fn a_member_names_the_packages_that_only_its_tests_need() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        assert_eq!(
            workspace.members[0].dev_dependencies,
            BTreeSet::from(["serde_json".to_owned()])
        );
        assert_eq!(
            workspace.members[1].dev_dependencies,
            BTreeSet::from(["gaslightmq_selector".to_owned()])
        );
    }

    #[test]
    fn a_member_has_an_edition() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        assert_eq!(workspace.members[0].edition, "2024");
        assert_eq!(workspace.members[1].edition, "2021");
    }

    #[test]
    fn a_member_declares_its_sans_io_modules_in_its_manifest() {
        let workspace = Workspace::from_metadata(METADATA).unwrap();

        assert_eq!(workspace.members[0].sans_io_modules, [""; 0]);
        assert_eq!(workspace.members[1].sans_io_modules, ["logic", "logic::cluster"]);
    }

    #[test]
    fn a_declaration_that_the_checks_do_not_know_is_unreadable() {
        let json = METADATA.replace("sans_io_modules", "sans_io_module");

        let error = Workspace::from_metadata(&json).err().unwrap();

        assert!(matches!(error, WorkspaceError::Unreadable { .. }));
    }

    #[test]
    fn json_without_packages_is_unreadable() {
        let error = Workspace::from_metadata(r#"{ "workspace_root": "/repo" }"#)
            .err()
            .unwrap();

        assert!(matches!(error, WorkspaceError::Unreadable { .. }));
    }
}
