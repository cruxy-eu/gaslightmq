//! The check of the workspace rules, which `CLAUDE.md` and `crates.md` state for every crate.
//!
//! - **Edition 2024.** Every crate builds with it.
//! - **Native async traits.** No crate depends on `async-trait`, for its code or for its tests.
//! - **Workspace dependency versions.** Every dependency in a crate's manifest says `workspace = true`,
//!   so its version, its path or its repository stands once, in `[workspace.dependencies]`.
//!
//! The rules hold for the tooling in `xtask/` too.
//!
//! What the check does not catch:
//!
//! - Another package that writes async traits the way `async-trait` does. The check knows that one name.
//! - What the [reader of manifests](crate::manifest) does not read.

use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::manifest;
use crate::workspace::{Member, Workspace};

/// The edition every crate builds with.
const EDITION: &str = "2024";

/// The package that wrote async traits before Rust had them.
const ASYNC_TRAIT: &str = "async-trait";

/// The file name of a manifest.
const MANIFEST: &str = "Cargo.toml";

/// Everything in which a crate breaks the workspace rules.
pub fn violations(workspace: &Workspace) -> Result<Vec<RuleViolation>, ManifestError> {
    let mut violations = Vec::new();
    for member in &workspace.members {
        let path = member.dir.join(MANIFEST);
        let text = std::fs::read_to_string(workspace.root.join(&path))
            .map_err(|error| ManifestError::Unreadable { path, error })?;
        violations.extend(of_member(member, &text));
    }
    Ok(violations)
}

/// The rules that one crate breaks. `manifest` is the text of its `Cargo.toml`.
fn of_member(member: &Member, manifest: &str) -> Vec<RuleViolation> {
    let name = &member.name;
    let mut violations = Vec::new();
    if member.edition != EDITION {
        violations.push(RuleViolation::OtherEdition {
            name: name.clone(),
            edition: member.edition.clone(),
        });
    }
    if member.outside_dependencies.contains(ASYNC_TRAIT) || member.dev_dependencies.contains(ASYNC_TRAIT) {
        violations.push(RuleViolation::AsyncTrait { name: name.clone() });
    }
    let inline = manifest::dependencies(manifest)
        .into_iter()
        .filter(|dependency| !dependency.from_workspace)
        .map(|dependency| RuleViolation::NotFromWorkspace {
            path: member.dir.join(MANIFEST),
            line: dependency.line,
            table: dependency.table,
            dependency: dependency.name,
        });
    violations.extend(inline);
    violations
}

/// A workspace rule that a crate breaks.
#[derive(Debug, PartialEq)]
pub enum RuleViolation {
    /// The crate builds with another edition than the workspace's.
    OtherEdition { name: String, edition: String },
    /// The crate depends on `async-trait`.
    AsyncTrait { name: String },
    /// A dependency in the crate's manifest does not say `workspace = true`.
    NotFromWorkspace {
        path: PathBuf,
        line: usize,
        table: String,
        dependency: String,
    },
}

impl fmt::Display for RuleViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OtherEdition { name, edition } => {
                write!(f, "`{name}` builds with edition {edition}, and not with {EDITION}")
            }
            Self::AsyncTrait { name } => write!(
                f,
                "`{name}` depends on `{ASYNC_TRAIT}`; a trait declares its `async fn` itself"
            ),
            Self::NotFromWorkspace {
                path,
                line,
                table,
                dependency,
            } => write!(
                f,
                "{}:{line}: `{table}` names `{dependency}` without `workspace = true`; \
                 its version stands in `[workspace.dependencies]`",
                path.display()
            ),
        }
    }
}

/// Why a manifest could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("{} cannot be read: {error}", .path.display())]
    Unreadable { path: PathBuf, error: io::Error },
}

#[cfg(test)]
mod tests {
    use super::*;

    const FOLLOWING_THE_RULES: &str = "\
[package]
name = \"gaslightmq_field\"
edition.workspace = true

[dependencies]
gaslightmq_kernel.workspace = true
serde = { workspace = true, optional = true }

[dev-dependencies]
serde_json.workspace = true
";

    fn member() -> Member {
        Member::alone("gaslightmq_field")
    }

    #[test]
    fn a_crate_that_follows_the_rules_passes() {
        assert_eq!(of_member(&member(), FOLLOWING_THE_RULES), []);
    }

    #[test]
    fn another_edition_fails() {
        let member = Member {
            edition: "2021".to_owned(),
            ..member()
        };

        let violations = of_member(&member, FOLLOWING_THE_RULES);

        assert_eq!(
            violations,
            [RuleViolation::OtherEdition {
                name: "gaslightmq_field".to_owned(),
                edition: "2021".to_owned()
            }]
        );
        assert_eq!(
            violations[0].to_string(),
            "`gaslightmq_field` builds with edition 2021, and not with 2024"
        );
    }

    #[test]
    fn async_trait_fails_in_code_and_in_tests() {
        let async_trait = || [ASYNC_TRAIT.to_owned()].into();
        let in_code = Member {
            outside_dependencies: async_trait(),
            ..member()
        };
        let in_tests = Member {
            dev_dependencies: async_trait(),
            ..member()
        };
        let expected = [RuleViolation::AsyncTrait {
            name: "gaslightmq_field".to_owned(),
        }];

        assert_eq!(of_member(&in_code, FOLLOWING_THE_RULES), expected);
        assert_eq!(of_member(&in_tests, FOLLOWING_THE_RULES), expected);
    }

    #[test]
    fn a_dependency_with_a_version_of_its_own_fails() {
        let manifest = FOLLOWING_THE_RULES.replace("serde_json.workspace = true", "serde_json = \"1\"");

        let violations = of_member(&member(), &manifest);

        assert_eq!(
            violations,
            [RuleViolation::NotFromWorkspace {
                path: PathBuf::from("crates/gaslightmq_field/Cargo.toml"),
                line: 10,
                table: "[dev-dependencies]".to_owned(),
                dependency: "serde_json".to_owned(),
            }]
        );
        assert_eq!(
            violations[0].to_string(),
            "crates/gaslightmq_field/Cargo.toml:10: `[dev-dependencies]` names `serde_json` without \
             `workspace = true`; its version stands in `[workspace.dependencies]`"
        );
    }

    #[test]
    fn a_dependency_with_a_path_of_its_own_fails() {
        let manifest = FOLLOWING_THE_RULES.replace(
            "gaslightmq_kernel.workspace = true",
            "gaslightmq_kernel = { path = \"../gaslightmq_kernel\" }",
        );

        assert_eq!(of_member(&member(), &manifest).len(), 1);
    }
}
