//! The check that every crate is named as `CLAUDE.md` says.
//!
//! A crate name starts with `gaslightmq_` and uses lowercase letters, digits and underscores.
//! The facade `gaslightmq` and the binary `gmq` stand apart, as `crates.md` says, and need no prefix.
//! The directory carries the name: `crates/gaslightmq_wire` holds `gaslightmq_wire`,
//! and a family shares a directory, so `crates/gaslightmq_kafka_client/api` holds `gaslightmq_kafka_client_api`.
//! The tooling in `xtask/` is the one exception to where a crate lives, and not to its name.

use std::fmt;
use std::path::{Component, Path, PathBuf};

use crate::workspace::Workspace;

/// The prefix of every crate name.
pub const PREFIX: &str = "gaslightmq_";

/// The two crates that stand apart, and carry no prefix: the facade and the binary.
pub const UNPREFIXED: [&str; 2] = ["gaslightmq", "gmq"];

/// The crate that a name in the tables of `crates.md` stands for: `selector` is `gaslightmq_selector`.
pub fn full_name(name: &str) -> String {
    if UNPREFIXED.contains(&name) {
        name.to_owned()
    } else {
        format!("{PREFIX}{name}")
    }
}

/// The directory that holds every crate.
const CRATES: &str = "crates";

/// Every crate whose name or directory breaks the rules.
pub fn violations(workspace: &Workspace) -> Vec<NameViolation> {
    let mut violations = Vec::new();
    for member in &workspace.members {
        let name = &member.name;
        if !name.starts_with(PREFIX) && !UNPREFIXED.contains(&name.as_str()) {
            violations.push(NameViolation::NoPrefix { name: name.clone() });
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            violations.push(NameViolation::NotSnakeCase { name: name.clone() });
        }
        if !member.is_tooling() && spelled_by(&member.dir).as_deref() != Some(name.as_str()) {
            violations.push(NameViolation::Elsewhere {
                name: name.clone(),
                dir: member.dir.clone(),
            });
        }
    }
    violations
}

/// The crate name that a directory under `crates/` spells: its parts below `crates/`, joined by `_`.
fn spelled_by(dir: &Path) -> Option<String> {
    let parts: Option<Vec<&str>> = dir
        .components()
        .map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect();
    match parts?.as_slice() {
        [CRATES, rest @ ..] if !rest.is_empty() => Some(rest.join("_")),
        _ => None,
    }
}

/// A crate named against the rules.
#[derive(Debug, PartialEq)]
pub enum NameViolation {
    NoPrefix {
        name: String,
    },
    NotSnakeCase {
        name: String,
    },
    /// The crate lives in a directory whose path below `crates/` does not spell its name.
    Elsewhere {
        name: String,
        dir: PathBuf,
    },
}

impl fmt::Display for NameViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPrefix { name } => write!(f, "`{name}` does not start with `{PREFIX}`"),
            Self::NotSnakeCase { name } => write!(
                f,
                "`{name}` holds a character other than a lowercase letter, a digit or an underscore"
            ),
            Self::Elsewhere { name, dir } => write!(
                f,
                "`{name}` lives in `{}`, and not in `{CRATES}/{name}` or a family directory that spells its name",
                dir.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::Member;

    fn workspace(members: &[(&str, &str)]) -> Workspace {
        Workspace {
            root: PathBuf::from("/repo"),
            members: members
                .iter()
                .map(|&(name, dir)| Member {
                    name: name.to_owned(),
                    dir: PathBuf::from(dir),
                    dependencies: [].into(),
                    outside_dependencies: [].into(),
                })
                .collect(),
        }
    }

    #[test]
    fn a_crate_in_its_own_directory_passes() {
        assert_eq!(
            violations(&workspace(&[("gaslightmq_wire", "crates/gaslightmq_wire")])),
            []
        );
    }

    #[test]
    fn a_crate_of_a_family_passes() {
        let workspace = workspace(&[("gaslightmq_kafka_client_api", "crates/gaslightmq_kafka_client/api")]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn a_name_without_the_prefix_fails() {
        assert_eq!(
            violations(&workspace(&[("proxy", "crates/proxy")])),
            [NameViolation::NoPrefix {
                name: "proxy".to_owned()
            }]
        );
    }

    #[test]
    fn the_facade_and_the_binary_need_no_prefix() {
        let workspace = workspace(&[("gaslightmq", "crates/gaslightmq"), ("gmq", "crates/gmq")]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn the_tooling_lives_in_xtask() {
        assert_eq!(violations(&workspace(&[("gaslightmq_checks", "xtask")])), []);
    }

    #[test]
    fn the_tooling_needs_the_prefix_too() {
        assert_eq!(
            violations(&workspace(&[("checks", "xtask")])),
            [NameViolation::NoPrefix {
                name: "checks".to_owned()
            }]
        );
    }

    #[test]
    fn a_table_name_stands_for_the_crate_with_the_prefix() {
        assert_eq!(full_name("selector"), "gaslightmq_selector");
        assert_eq!(full_name("gmq"), "gmq");
    }

    #[test]
    fn a_name_with_a_hyphen_fails() {
        assert_eq!(
            violations(&workspace(&[(
                "gaslightmq_kafka-client",
                "crates/gaslightmq_kafka-client"
            )])),
            [NameViolation::NotSnakeCase {
                name: "gaslightmq_kafka-client".to_owned()
            }]
        );
    }

    #[test]
    fn a_crate_in_a_directory_that_spells_another_name_fails() {
        assert_eq!(
            violations(&workspace(&[("gaslightmq_wire", "crates/wire")])),
            [NameViolation::Elsewhere {
                name: "gaslightmq_wire".to_owned(),
                dir: PathBuf::from("crates/wire")
            }]
        );
    }

    #[test]
    fn a_crate_outside_the_crates_directory_fails() {
        assert_eq!(
            violations(&workspace(&[("gaslightmq_xtask", "gaslightmq_xtask")])),
            [NameViolation::Elsewhere {
                name: "gaslightmq_xtask".to_owned(),
                dir: PathBuf::from("gaslightmq_xtask")
            }]
        );
    }
}
