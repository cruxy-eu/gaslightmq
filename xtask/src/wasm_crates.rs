//! The table of `crates.md` that lists what compiles to WebAssembly, and the check that guards it.
//!
//! A row reads `| `field`, `selector` | what the UI uses them for |`.
//! The table is the only list of these crates: `just wasm` builds the ones that exist for the browser.
//! A crate of the table that does not exist yet is a plan, and passes.
//!
//! The crates of the table travel to the UI, so none of them reaches the Kafka codec.
//! The check follows the dependencies of each one through the workspace, and fails when a crate on the way
//! names `kafka-protocol`. It does not look inside a package from outside the workspace.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::io;
use std::path::Path;

use crate::crate_layout::{CrateLayout, PATH};
use crate::table;
use crate::workspace::{Member, Workspace};

/// The header row of the table.
const HEADER: &str = "| Crate | The UI uses it to |";

/// The package that reads and writes Kafka's bytes, which the browser never loads.
const KAFKA_CODEC: &str = "kafka-protocol";

/// The crates that compile to WebAssembly, by full name.
pub struct WasmCrates {
    names: BTreeSet<String>,
}

impl WasmCrates {
    pub fn load(root: &Path) -> Result<Self, WasmCratesError> {
        let text = std::fs::read_to_string(root.join(PATH)).map_err(|error| WasmCratesError::Unreadable { error })?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self, WasmCratesError> {
        let mut names = BTreeSet::new();
        for row in table::rows(text, HEADER) {
            let malformed = || WasmCratesError::MalformedRow { line: row.line };
            let [crates, _use] = row.cells[..] else {
                return Err(malformed());
            };
            names.extend(table::crate_names(crates).ok_or_else(malformed)?);
        }
        if names.is_empty() {
            return Err(WasmCratesError::NoTable);
        }
        Ok(WasmCrates { names })
    }

    /// The crates of the table that the workspace holds, in the order of their names.
    pub fn existing<'a>(&self, workspace: &'a Workspace) -> Vec<&'a Member> {
        let mut members: Vec<&Member> = workspace
            .members
            .iter()
            .filter(|member| self.names.contains(&member.name))
            .collect();
        members.sort_by(|a, b| a.name.cmp(&b.name));
        members
    }

    /// Every crate of the table that the crate tables do not know, or that reaches the Kafka codec.
    pub fn violations(&self, layout: &CrateLayout, workspace: &Workspace) -> Vec<WasmViolation> {
        let unknown = self
            .names
            .iter()
            .filter(|name| !layout.has_row(name))
            .map(|name| WasmViolation::NoRow { name: name.clone() });
        let reaching = self
            .existing(workspace)
            .into_iter()
            .filter_map(|member| way_to_kafka_codec(member, workspace))
            .map(|way| WasmViolation::ReachesKafkaCodec { way });
        unknown.chain(reaching).collect()
    }
}

/// The shortest way from a crate to a workspace crate that names the Kafka codec, both included.
fn way_to_kafka_codec(from: &Member, workspace: &Workspace) -> Option<Vec<String>> {
    let members: BTreeMap<&str, &Member> = workspace
        .members
        .iter()
        .map(|member| (member.name.as_str(), member))
        .collect();
    // Each crate the search has seen, with the crate that led to it.
    let mut reached_from: BTreeMap<&str, Option<&str>> = BTreeMap::from([(from.name.as_str(), None)]);
    let mut to_visit = VecDeque::from([from]);
    while let Some(member) = to_visit.pop_front() {
        if member.outside_dependencies.contains(KAFKA_CODEC) {
            let mut way = vec![member.name.clone()];
            let mut at = member.name.as_str();
            while let Some(&Some(previous)) = reached_from.get(at) {
                way.push(previous.to_owned());
                at = previous;
            }
            way.reverse();
            return Some(way);
        }
        for dependency in &member.dependencies {
            let Some(&next) = members.get(dependency.as_str()) else {
                continue;
            };
            if !reached_from.contains_key(next.name.as_str()) {
                reached_from.insert(&next.name, Some(&member.name));
                to_visit.push_back(next);
            }
        }
    }
    None
}

/// A way in which a crate of the table breaks the boundary of the browser.
#[derive(Debug, PartialEq)]
pub enum WasmViolation {
    /// The table names a crate that has no row in the crate tables.
    NoRow { name: String },
    /// A crate of the table reaches the Kafka codec.
    /// `way` starts at that crate and ends at the workspace crate that names the codec.
    ReachesKafkaCodec { way: Vec<String> },
}

impl fmt::Display for WasmViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRow { name } => write!(
                f,
                "{PATH} says that `{name}` compiles to WebAssembly, and has no row for it in the crate tables"
            ),
            Self::ReachesKafkaCodec { way } => {
                let name = way.first().map_or("", String::as_str);
                write!(f, "`{name}` compiles to WebAssembly, and it reaches `{KAFKA_CODEC}`: ")?;
                for step in way {
                    write!(f, "`{step}` → ")?;
                }
                write!(f, "`{KAFKA_CODEC}`")
            }
        }
    }
}

/// Why the table could not be read.
#[derive(Debug, thiserror::Error)]
pub enum WasmCratesError {
    #[error("{PATH} cannot be read: {error}")]
    Unreadable { error: io::Error },
    #[error("{PATH} has no table that starts with `{HEADER}`")]
    NoTable,
    #[error(
        "{PATH}:{line}: a row of the WebAssembly table has two cells: \
         the crates in backticks, and what the UI uses them for"
    )]
    MalformedRow { line: usize },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const TABLES: &str = "\
| Crate | Holds | Depends on |
|---|---|---|
| `kernel` | The value objects of Kafka. | — |
| `field` | The field vocabulary. | `kernel` |
| `selector` | Selectors. | `field` |
| `wire` | The Kafka codec. | — |
| `call` | A crate that does not exist yet. | `field` |

| Crate | The UI uses it to |
|---|---|
| `kernel` | Check topic names. |
| `selector`, `field` | Build selectors. |
| `call` | Render verdicts. |
";

    fn member(name: &str, dependencies: &[&str], outside_dependencies: &[&str]) -> Member {
        let owned = |names: &[&str]| names.iter().map(|&name| name.to_owned()).collect();
        Member {
            dependencies: owned(dependencies),
            outside_dependencies: owned(outside_dependencies),
            ..Member::alone(name)
        }
    }

    fn workspace(members: Vec<Member>) -> Workspace {
        Workspace {
            root: PathBuf::from("/repo"),
            members,
        }
    }

    /// A workspace in which no crate of the table reaches `gaslightmq_wire`.
    fn within_the_boundary() -> Vec<Member> {
        vec![
            member("gaslightmq_wire", &[], &["bytes", "kafka-protocol"]),
            member("gaslightmq_selector", &["gaslightmq_field"], &["serde"]),
            member("gaslightmq_kernel", &[], &["serde"]),
            member("gaslightmq_field", &["gaslightmq_kernel"], &["serde"]),
        ]
    }

    fn violations(tables: &str, members: Vec<Member>) -> Vec<WasmViolation> {
        let layout = CrateLayout::parse(tables).unwrap();
        WasmCrates::parse(tables)
            .unwrap()
            .violations(&layout, &workspace(members))
    }

    #[test]
    fn the_crates_of_the_table_that_exist_come_in_the_order_of_their_names() {
        let wasm_crates = WasmCrates::parse(TABLES).unwrap();
        let workspace = workspace(within_the_boundary());

        let names: Vec<&str> = wasm_crates
            .existing(&workspace)
            .into_iter()
            .map(|member| member.name.as_str())
            .collect();
        assert_eq!(names, ["gaslightmq_field", "gaslightmq_kernel", "gaslightmq_selector"]);
    }

    #[test]
    fn crates_that_stay_clear_of_the_kafka_codec_pass() {
        assert_eq!(violations(TABLES, within_the_boundary()), []);
    }

    #[test]
    fn a_crate_that_names_the_kafka_codec_fails() {
        let mut members = within_the_boundary();
        members[2] = member("gaslightmq_kernel", &[], &["kafka-protocol"]);

        assert_eq!(
            violations(TABLES, members),
            [
                WasmViolation::ReachesKafkaCodec {
                    way: vec!["gaslightmq_field".to_owned(), "gaslightmq_kernel".to_owned()]
                },
                WasmViolation::ReachesKafkaCodec {
                    way: vec!["gaslightmq_kernel".to_owned()]
                },
                WasmViolation::ReachesKafkaCodec {
                    way: vec![
                        "gaslightmq_selector".to_owned(),
                        "gaslightmq_field".to_owned(),
                        "gaslightmq_kernel".to_owned()
                    ]
                },
            ]
        );
    }

    #[test]
    fn a_crate_that_reaches_the_kafka_codec_through_another_crate_fails() {
        let mut members = within_the_boundary();
        members[3] = member("gaslightmq_field", &["gaslightmq_kernel", "gaslightmq_wire"], &[]);

        assert_eq!(
            violations(TABLES, members),
            [
                WasmViolation::ReachesKafkaCodec {
                    way: vec!["gaslightmq_field".to_owned(), "gaslightmq_wire".to_owned()]
                },
                WasmViolation::ReachesKafkaCodec {
                    way: vec![
                        "gaslightmq_selector".to_owned(),
                        "gaslightmq_field".to_owned(),
                        "gaslightmq_wire".to_owned()
                    ]
                },
            ]
        );
    }

    #[test]
    fn crates_that_depend_on_each_other_in_a_circle_pass() {
        let mut members = within_the_boundary();
        members[2] = member("gaslightmq_kernel", &["gaslightmq_field"], &[]);

        assert_eq!(violations(TABLES, members), []);
    }

    #[test]
    fn a_crate_without_a_row_in_the_crate_tables_fails() {
        let tables = TABLES.replace("| `call` | Render verdicts. |", "| `kernal` | Render verdicts. |");

        assert_eq!(
            violations(&tables, within_the_boundary()),
            [WasmViolation::NoRow {
                name: "gaslightmq_kernal".to_owned()
            }]
        );
    }

    #[test]
    fn a_violation_shows_the_way_to_the_kafka_codec() {
        let violation = WasmViolation::ReachesKafkaCodec {
            way: vec!["gaslightmq_field".to_owned(), "gaslightmq_wire".to_owned()],
        };

        assert_eq!(
            violation.to_string(),
            "`gaslightmq_field` compiles to WebAssembly, and it reaches `kafka-protocol`: \
             `gaslightmq_field` → `gaslightmq_wire` → `kafka-protocol`"
        );
    }

    #[test]
    fn a_row_without_two_cells_is_malformed() {
        let text = "| Crate | The UI uses it to |\n|---|---|\n| `kernel` |\n";

        assert!(matches!(
            WasmCrates::parse(text),
            Err(WasmCratesError::MalformedRow { line: 3 })
        ));
    }

    #[test]
    fn a_crate_outside_backticks_is_malformed() {
        let text = "| Crate | The UI uses it to |\n|---|---|\n| `field`, selector | Build selectors. |\n";

        assert!(matches!(
            WasmCrates::parse(text),
            Err(WasmCratesError::MalformedRow { line: 3 })
        ));
    }

    #[test]
    fn a_text_without_the_table_fails() {
        assert!(matches!(
            WasmCrates::parse("# Crate layout\n"),
            Err(WasmCratesError::NoTable)
        ));
    }

    #[test]
    fn the_table_of_this_repository_parses() {
        let wasm_crates = WasmCrates::parse(include_str!("../../docs/specs/architecture/crates.md")).unwrap();

        assert!(wasm_crates.names.contains("gaslightmq_kernel"));
        assert!(wasm_crates.names.contains("gaslightmq_control_api"));
    }
}
