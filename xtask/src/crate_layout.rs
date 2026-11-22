//! The crate tables of `crates.md`, and the check that every crate depends on what its row lists.
//!
//! A row reads `| `field` | what it holds | `kernel`, `clock` |`.
//! The check compares a crate's dependencies on other workspace crates with its row, both ways:
//! a dependency the row does not list fails, and so does a listed dependency the crate does not have.
//! A row for a crate that does not exist yet is a plan, and passes.
//! The tooling in `xtask/` is not product code, and needs no row.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::Path;

use crate::table::{self, Row};
use crate::workspace::Workspace;

/// Where the crate tables live, relative to the workspace root.
pub const PATH: &str = "docs/specs/architecture/crates.md";

/// The header row that starts a crate table.
const HEADER: &str = "| Crate | Holds | Depends on |";

/// Every row of every crate table, by full crate name.
pub struct CrateLayout {
    rows: BTreeMap<String, Dependencies>,
}

/// The last cell of a row.
#[derive(Debug, PartialEq)]
enum Dependencies {
    /// The crates in backticks, or none for `—`.
    Listed(BTreeSet<String>),
    /// Words instead of crate names, such as "The public crates".
    Described(String),
}

impl CrateLayout {
    pub fn load(root: &Path) -> Result<Self, CrateLayoutError> {
        let text = std::fs::read_to_string(root.join(PATH)).map_err(|error| CrateLayoutError::Unreadable { error })?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self, CrateLayoutError> {
        let mut rows = BTreeMap::new();
        for row in table::rows(text, HEADER) {
            let (name, dependencies) = parse_row(&row)?;
            if rows.insert(name.clone(), dependencies).is_some() {
                return Err(CrateLayoutError::TwoRows { line: row.line, name });
            }
        }
        if rows.is_empty() {
            return Err(CrateLayoutError::NoTables);
        }
        Ok(CrateLayout { rows })
    }

    /// Whether a crate, by its full name, has a row.
    pub fn has_row(&self, name: &str) -> bool {
        self.rows.contains_key(name)
    }

    /// Everything in which the workspace differs from the tables.
    pub fn violations(&self, workspace: &Workspace) -> Vec<LayoutViolation> {
        let mut violations = Vec::new();
        for member in workspace.members.iter().filter(|member| !member.is_tooling()) {
            let name = &member.name;
            match self.rows.get(name) {
                None => violations.push(LayoutViolation::NoRow { name: name.clone() }),
                Some(Dependencies::Described(words)) => {
                    violations.push(LayoutViolation::Described {
                        name: name.clone(),
                        words: words.clone(),
                    });
                }
                Some(Dependencies::Listed(listed)) => {
                    for dependency in member.dependencies.difference(listed) {
                        violations.push(LayoutViolation::Unlisted {
                            name: name.clone(),
                            dependency: dependency.clone(),
                        });
                    }
                    for dependency in listed.difference(&member.dependencies) {
                        violations.push(LayoutViolation::Missing {
                            name: name.clone(),
                            dependency: dependency.clone(),
                        });
                    }
                }
            }
        }
        violations
    }
}

fn parse_row(row: &Row<'_>) -> Result<(String, Dependencies), CrateLayoutError> {
    let malformed = || CrateLayoutError::MalformedRow { line: row.line };
    let [name, _holds, dependencies] = row.cells[..] else {
        return Err(malformed());
    };
    let name = table::crate_name(name).ok_or_else(malformed)?;
    let dependencies = if dependencies == "—" {
        Dependencies::Listed(BTreeSet::new())
    } else if dependencies.contains('`') {
        let names = table::crate_names(dependencies).ok_or_else(malformed)?;
        Dependencies::Listed(names.into_iter().collect())
    } else {
        Dependencies::Described(dependencies.to_owned())
    };
    Ok((name, dependencies))
}

/// A way in which the workspace differs from the crate tables.
#[derive(Debug, PartialEq)]
pub enum LayoutViolation {
    /// The crate has no row.
    NoRow { name: String },
    /// The crate's row describes its dependencies in words, which the check cannot compare.
    Described { name: String, words: String },
    /// The crate depends on a crate that its row does not list.
    Unlisted { name: String, dependency: String },
    /// The crate's row lists a crate that it does not depend on.
    Missing { name: String, dependency: String },
}

impl fmt::Display for LayoutViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRow { name } => write!(f, "`{name}` has no row in {PATH}"),
            Self::Described { name, words } => write!(
                f,
                "the row of `{name}` in {PATH} says \"{words}\", where it should list crates in backticks"
            ),
            Self::Unlisted { name, dependency } => write!(
                f,
                "`{name}` depends on `{dependency}`, which its row in {PATH} does not list"
            ),
            Self::Missing { name, dependency } => write!(
                f,
                "the row of `{name}` in {PATH} lists `{dependency}`, which `{name}` does not depend on"
            ),
        }
    }
}

/// Why the crate tables could not be read.
#[derive(Debug)]
pub enum CrateLayoutError {
    Unreadable { error: io::Error },
    NoTables,
    MalformedRow { line: usize },
    TwoRows { line: usize, name: String },
}

impl fmt::Display for CrateLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { error } => write!(f, "{PATH} cannot be read: {error}"),
            Self::NoTables => write!(f, "{PATH} has no table that starts with `{HEADER}`"),
            Self::MalformedRow { line } => write!(
                f,
                "{PATH}:{line}: a row of a crate table has three cells: \
                 the crate in backticks, what it holds, and its dependencies in backticks or `—`"
            ),
            Self::TwoRows { line, name } => write!(f, "{PATH}:{line}: `{name}` has a second row"),
        }
    }
}

impl std::error::Error for CrateLayoutError {}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::workspace::Member;

    const TABLES: &str = "\
# Crate layout

| Crate | Holds | Depends on |
|---|---|---|
| `kernel` | The value objects of Kafka. | — |
| `field` | The field vocabulary. | `kernel`, `clock` |

Some prose in between.

| Crate | Holds | Depends on |
|---|---|---|
| `clock` | The clock. | — |
| `gaslightmq` | The facade crate. | The public crates |
| `planned` | A crate that does not exist yet. | `kernel` |

| Step | Crates | Result |
|---|---|---|
| 1 | `kernel`, `field` | Not a crate table. |
";

    fn member(name: &str, dependencies: &[&str]) -> Member {
        Member {
            name: name.to_owned(),
            dir: PathBuf::from("crates").join(name),
            dependencies: dependencies.iter().map(|&dependency| dependency.to_owned()).collect(),
            outside_dependencies: BTreeSet::new(),
        }
    }

    fn workspace(members: Vec<Member>) -> Workspace {
        Workspace {
            root: PathBuf::from("/repo"),
            members,
        }
    }

    fn following_the_tables() -> Vec<Member> {
        vec![
            member("gaslightmq_kernel", &[]),
            member("gaslightmq_field", &["gaslightmq_kernel", "gaslightmq_clock"]),
            member("gaslightmq_clock", &[]),
        ]
    }

    #[test]
    fn a_workspace_that_follows_the_tables_passes() {
        let layout = CrateLayout::parse(TABLES).unwrap();

        assert_eq!(layout.violations(&workspace(following_the_tables())), []);
    }

    #[test]
    fn a_dependency_the_row_does_not_list_fails() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let mut members = following_the_tables();
        members[0] = member("gaslightmq_kernel", &["gaslightmq_clock"]);

        assert_eq!(
            layout.violations(&workspace(members)),
            [LayoutViolation::Unlisted {
                name: "gaslightmq_kernel".to_owned(),
                dependency: "gaslightmq_clock".to_owned()
            }]
        );
    }

    #[test]
    fn a_listed_dependency_the_crate_does_not_have_fails() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let mut members = following_the_tables();
        members[1] = member("gaslightmq_field", &["gaslightmq_kernel"]);

        assert_eq!(
            layout.violations(&workspace(members)),
            [LayoutViolation::Missing {
                name: "gaslightmq_field".to_owned(),
                dependency: "gaslightmq_clock".to_owned()
            }]
        );
    }

    #[test]
    fn a_crate_without_a_row_fails() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let mut members = following_the_tables();
        members.push(member("gaslightmq_wire", &[]));

        assert_eq!(
            layout.violations(&workspace(members)),
            [LayoutViolation::NoRow {
                name: "gaslightmq_wire".to_owned()
            }]
        );
    }

    #[test]
    fn the_tooling_needs_no_row() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let mut members = following_the_tables();
        members.push(Member {
            name: "gaslightmq_checks".to_owned(),
            dir: PathBuf::from("xtask"),
            dependencies: BTreeSet::new(),
            outside_dependencies: BTreeSet::new(),
        });

        assert_eq!(layout.violations(&workspace(members)), []);
    }

    #[test]
    fn a_crate_whose_row_describes_its_dependencies_in_words_fails() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let mut members = following_the_tables();
        members.push(member("gaslightmq", &["gaslightmq_kernel"]));

        assert_eq!(
            layout.violations(&workspace(members)),
            [LayoutViolation::Described {
                name: "gaslightmq".to_owned(),
                words: "The public crates".to_owned()
            }]
        );
    }

    #[test]
    fn a_row_without_three_cells_is_malformed() {
        let text = "| Crate | Holds | Depends on |\n|---|---|---|\n| `kernel` | — |\n";

        assert!(matches!(
            CrateLayout::parse(text),
            Err(CrateLayoutError::MalformedRow { line: 3 })
        ));
    }

    #[test]
    fn a_dependency_outside_backticks_is_malformed() {
        let text = "| Crate | Holds | Depends on |\n|---|---|---|\n| `field` | Fields. | `kernel`, clock |\n";

        assert!(matches!(
            CrateLayout::parse(text),
            Err(CrateLayoutError::MalformedRow { line: 3 })
        ));
    }

    #[test]
    fn a_crate_with_two_rows_fails() {
        let text = "| Crate | Holds | Depends on |\n|---|---|---|\n| `kernel` | A. | — |\n| `kernel` | B. | — |\n";

        assert!(matches!(
            CrateLayout::parse(text),
            Err(CrateLayoutError::TwoRows { line: 4, .. })
        ));
    }

    #[test]
    fn a_text_without_crate_tables_fails() {
        assert!(matches!(
            CrateLayout::parse("# Crate layout\n"),
            Err(CrateLayoutError::NoTables)
        ));
    }

    #[test]
    fn the_crate_tables_of_this_repository_parse() {
        let layout = CrateLayout::parse(include_str!("../../docs/specs/architecture/crates.md")).unwrap();

        assert_eq!(layout.rows["gaslightmq_kernel"], Dependencies::Listed(BTreeSet::new()));
    }
}
