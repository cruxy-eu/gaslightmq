//! The check that test support never reaches production code.
//!
//! `crates.md` says that a test-support crate is for the tests of other crates:
//! the check of a store, or a real Kafka cluster for a test.
//! Only a dev-dependency names it, so no crate that ships needs one to build.
//!
//! A crate declares itself test support in its `Cargo.toml`:
//!
//! ```toml
//! [package.metadata.gaslightmq]
//! test_support = true
//! ```
//!
//! - A crate that is no test support builds with no test-support crate:
//!   none of its dependencies and build dependencies is one, and none of them builds with one either.
//!   The check follows the dependencies through the workspace, and names the crates on the way.
//!   A dev-dependency passes, and so does a test-support crate that builds with another one.
//! - A crate that looks like test support declares it: one whose name ends in `_conformance`,
//!   and the cluster of real brokers in [`NAMES`].
//!   Without the declaration, the first rule would not see the crate.
//!
//! What the check does not catch:
//!
//! - A test-support crate under another name that does not declare itself.
//! - A crate from outside the workspace. The check knows the crates of the workspace alone.
//! - What a crate does with a dev-dependency: a test may use whatever it likes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use crate::workspace::{Member, Workspace};

/// The end of the name of a crate that holds the checks of a contract, which the tests of an implementation run.
const SUFFIX: &str = "_conformance";

/// The crates that are test support by what they are, whatever their names end in:
/// the cluster of real brokers for the tests of this workspace.
const NAMES: [&str; 1] = ["gaslightmq_test_kafka"];

/// Whether the name of a crate says that it is test support.
fn looks_like_test_support(name: &str) -> bool {
    name.ends_with(SUFFIX) || NAMES.contains(&name)
}

/// Every crate that looks like test support without declaring it,
/// and every crate that is no test support and builds with a crate that is.
pub fn violations(workspace: &Workspace) -> Vec<TestSupportViolation> {
    let by_name: BTreeMap<&str, &Member> = workspace
        .members
        .iter()
        .map(|member| (member.name.as_str(), member))
        .collect();
    let mut violations = Vec::new();
    for member in &workspace.members {
        if member.test_support {
            continue;
        }
        if looks_like_test_support(&member.name) {
            violations.push(TestSupportViolation::NotDeclared {
                name: member.name.clone(),
            });
        }
        violations.extend(reached_from(member, &by_name));
    }
    violations
}

/// The test-support crates that a crate builds with, each with the shortest way to it.
///
/// The walk does not go on behind a test-support crate: what that one builds with is its own business.
fn reached_from(member: &Member, by_name: &BTreeMap<&str, &Member>) -> Vec<TestSupportViolation> {
    let mut violations = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::from([member.name.as_str()]);
    // Each crate that the walk reached, with the crates between `member` and it.
    let mut reached: VecDeque<(&Member, Vec<String>)> = VecDeque::from([(member, Vec::new())]);
    while let Some((from, through)) = reached.pop_front() {
        for name in &from.dependencies {
            let Some(&dependency) = by_name.get(name.as_str()) else {
                continue;
            };
            if !seen.insert(name.as_str()) {
                continue;
            }
            if dependency.test_support {
                violations.push(TestSupportViolation::BuildsWith {
                    name: member.name.clone(),
                    test_support: name.clone(),
                    through: through.clone(),
                });
            } else {
                let mut through = through.clone();
                through.push(name.clone());
                reached.push_back((dependency, through));
            }
        }
    }
    violations
}

/// A way in which test support reaches production code, or hides from the check.
#[derive(Debug, PartialEq)]
pub enum TestSupportViolation {
    /// A crate has the name of test support, and its manifest does not say that it is.
    NotDeclared { name: String },
    /// A crate that is no test support needs a test-support crate to build.
    /// `through` holds the crates on the way from the one to the other, and is empty for a dependency of its own.
    BuildsWith {
        name: String,
        test_support: String,
        through: Vec<String>,
    },
}

impl fmt::Display for TestSupportViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotDeclared { name } => write!(
                f,
                "`{name}` has the name of a test-support crate, and its `Cargo.toml` does not say \
                 `test_support = true` under `[package.metadata.gaslightmq]`"
            ),
            Self::BuildsWith {
                name,
                test_support,
                through,
            } => {
                write!(
                    f,
                    "`{name}` is no test support, and builds with `{test_support}`, which is"
                )?;
                if !through.is_empty() {
                    let way: Vec<String> = through.iter().map(|name| format!("`{name}`")).collect();
                    write!(f, ", through {}", way.join(" and "))?;
                }
                f.write_str("; only a dev-dependency names a test-support crate")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const HOST: &str = "gaslightmq_host";
    const WORKSPACE_FS: &str = "gaslightmq_workspace_fs";
    const SELECTOR: &str = "gaslightmq_selector";
    const SELECTOR_CONFORMANCE: &str = "gaslightmq_selector_conformance";
    const DEFINITION_CONFORMANCE: &str = "gaslightmq_definition_conformance";
    const TEST_KAFKA: &str = "gaslightmq_test_kafka";

    fn owned(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|&name| name.to_owned()).collect()
    }

    /// A crate that ships, and builds with those crates.
    fn production(name: &str, dependencies: &[&str]) -> Member {
        Member {
            dependencies: owned(dependencies),
            ..Member::alone(name)
        }
    }

    /// A crate that declares itself test support, and builds with those crates.
    fn test_support(name: &str, dependencies: &[&str]) -> Member {
        Member {
            test_support: true,
            ..production(name, dependencies)
        }
    }

    fn workspace(members: Vec<Member>) -> Workspace {
        Workspace {
            root: PathBuf::from("/repo"),
            members,
        }
    }

    #[test]
    fn a_crate_that_only_tests_with_test_support_passes() {
        let workspace = workspace(vec![
            Member {
                dev_dependencies: owned(&[SELECTOR_CONFORMANCE, TEST_KAFKA]),
                ..production(WORKSPACE_FS, &[SELECTOR])
            },
            production(SELECTOR, &[]),
            test_support(SELECTOR_CONFORMANCE, &[SELECTOR]),
            test_support(TEST_KAFKA, &[]),
        ]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn a_test_support_crate_builds_with_another_one() {
        let workspace = workspace(vec![
            production(SELECTOR, &[]),
            test_support(SELECTOR_CONFORMANCE, &[SELECTOR, DEFINITION_CONFORMANCE]),
            test_support(DEFINITION_CONFORMANCE, &[]),
        ]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn a_crate_that_builds_with_test_support_fails() {
        let workspace = workspace(vec![
            production(WORKSPACE_FS, &[SELECTOR, SELECTOR_CONFORMANCE]),
            production(SELECTOR, &[]),
            test_support(SELECTOR_CONFORMANCE, &[SELECTOR]),
        ]);

        assert_eq!(
            violations(&workspace),
            [TestSupportViolation::BuildsWith {
                name: WORKSPACE_FS.to_owned(),
                test_support: SELECTOR_CONFORMANCE.to_owned(),
                through: Vec::new(),
            }]
        );
    }

    #[test]
    fn a_crate_that_reaches_test_support_through_another_crate_fails_too() {
        let workspace = workspace(vec![
            production(HOST, &[WORKSPACE_FS]),
            production(WORKSPACE_FS, &[SELECTOR, TEST_KAFKA]),
            production(SELECTOR, &[]),
            test_support(TEST_KAFKA, &[]),
        ]);

        assert_eq!(
            violations(&workspace),
            [
                TestSupportViolation::BuildsWith {
                    name: HOST.to_owned(),
                    test_support: TEST_KAFKA.to_owned(),
                    through: vec![WORKSPACE_FS.to_owned()],
                },
                TestSupportViolation::BuildsWith {
                    name: WORKSPACE_FS.to_owned(),
                    test_support: TEST_KAFKA.to_owned(),
                    through: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn what_a_test_support_crate_builds_with_is_not_reached_through_it() {
        // The check of the selectors builds with the check of every store. The crate that builds with the first
        // is told about that one alone.
        let workspace = workspace(vec![
            production(WORKSPACE_FS, &[SELECTOR_CONFORMANCE]),
            test_support(SELECTOR_CONFORMANCE, &[DEFINITION_CONFORMANCE]),
            test_support(DEFINITION_CONFORMANCE, &[]),
        ]);

        assert_eq!(
            violations(&workspace),
            [TestSupportViolation::BuildsWith {
                name: WORKSPACE_FS.to_owned(),
                test_support: SELECTOR_CONFORMANCE.to_owned(),
                through: Vec::new(),
            }]
        );
    }

    #[test]
    fn a_test_support_crate_is_reached_once_by_the_shortest_way() {
        let workspace = workspace(vec![
            production(HOST, &[WORKSPACE_FS, TEST_KAFKA]),
            production(WORKSPACE_FS, &[TEST_KAFKA]),
            test_support(TEST_KAFKA, &[]),
        ]);

        let of_the_host: Vec<TestSupportViolation> = violations(&workspace)
            .into_iter()
            .filter(|violation| matches!(violation, TestSupportViolation::BuildsWith { name, .. } if name == HOST))
            .collect();

        assert_eq!(
            of_the_host,
            [TestSupportViolation::BuildsWith {
                name: HOST.to_owned(),
                test_support: TEST_KAFKA.to_owned(),
                through: Vec::new(),
            }]
        );
    }

    #[test]
    fn a_conformance_crate_that_does_not_declare_itself_fails() {
        let workspace = workspace(vec![
            production(SELECTOR, &[]),
            production(SELECTOR_CONFORMANCE, &[SELECTOR]),
        ]);

        assert_eq!(
            violations(&workspace),
            [TestSupportViolation::NotDeclared {
                name: SELECTOR_CONFORMANCE.to_owned(),
            }]
        );
    }

    #[test]
    fn the_cluster_of_real_brokers_that_does_not_declare_itself_fails() {
        let workspace = workspace(vec![production(TEST_KAFKA, &[])]);

        assert_eq!(
            violations(&workspace),
            [TestSupportViolation::NotDeclared {
                name: TEST_KAFKA.to_owned(),
            }]
        );
    }

    #[test]
    fn a_crate_with_another_name_declares_nothing() {
        // The test library and the container module start GaslightMQ for the tests of a user: they ship.
        let workspace = workspace(vec![
            production("gaslightmq_test", &[HOST]),
            production("gaslightmq_testcontainers", &[]),
            production(HOST, &[]),
        ]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn a_dependency_from_outside_the_workspace_is_not_followed() {
        let workspace = workspace(vec![Member {
            outside_dependencies: owned(&["tokio"]),
            ..production(HOST, &["gaslightmq_engine"])
        }]);

        assert_eq!(violations(&workspace), []);
    }

    #[test]
    fn a_violation_says_who_builds_with_what_and_on_which_way() {
        let direct = TestSupportViolation::BuildsWith {
            name: WORKSPACE_FS.to_owned(),
            test_support: TEST_KAFKA.to_owned(),
            through: Vec::new(),
        };
        let indirect = TestSupportViolation::BuildsWith {
            name: "gmq".to_owned(),
            test_support: TEST_KAFKA.to_owned(),
            through: vec![HOST.to_owned(), WORKSPACE_FS.to_owned()],
        };
        let hidden = TestSupportViolation::NotDeclared {
            name: SELECTOR_CONFORMANCE.to_owned(),
        };

        assert_eq!(
            direct.to_string(),
            "`gaslightmq_workspace_fs` is no test support, and builds with `gaslightmq_test_kafka`, which is; \
             only a dev-dependency names a test-support crate"
        );
        assert_eq!(
            indirect.to_string(),
            "`gmq` is no test support, and builds with `gaslightmq_test_kafka`, which is, \
             through `gaslightmq_host` and `gaslightmq_workspace_fs`; \
             only a dev-dependency names a test-support crate"
        );
        assert_eq!(
            hidden.to_string(),
            "`gaslightmq_selector_conformance` has the name of a test-support crate, and its `Cargo.toml` does not say \
             `test_support = true` under `[package.metadata.gaslightmq]`"
        );
    }
}
