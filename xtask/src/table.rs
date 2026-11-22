//! The tables of `crates.md`, as far as the checks read them.
//!
//! A table is its header row, a row of dashes, and the rows below, up to the first line that is not a row.
//! A cell names crates in backticks, without the `gaslightmq_` prefix, and separates them with commas.

use crate::crate_names::full_name;

/// One row below a header.
pub struct Row<'a> {
    /// Its line in the text, counted from one.
    pub line: usize,
    pub cells: Vec<&'a str>,
}

/// The rows of every table whose header row is `header`.
pub fn rows<'a>(text: &'a str, header: &str) -> Vec<Row<'a>> {
    let mut rows = Vec::new();
    let mut in_table = false;
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line == header {
            in_table = true;
        } else if !line.starts_with('|') {
            in_table = false;
        } else if in_table && !line.starts_with("|---") {
            rows.push(Row {
                line: index + 1,
                cells: line.trim_matches('|').split('|').map(str::trim).collect(),
            });
        }
    }
    rows
}

/// The full name of the one crate that a cell names.
pub fn crate_name(cell: &str) -> Option<String> {
    let name = cell.strip_prefix('`')?.strip_suffix('`')?;
    (!name.is_empty() && !name.contains('`')).then(|| full_name(name))
}

/// The full names of the crates that a cell names, or `None` when a part of it is not a crate in backticks.
pub fn crate_names(cell: &str) -> Option<Vec<String>> {
    cell.split(',').map(|name| crate_name(name.trim())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "\
| Crate | Holds |
|---|---|
| `kernel` | Value objects. |
| `field`, `selector` | Fields. |
Prose ends the table.
| `clock` | Not in a table. |

| Step | Crates |
|---|---|
| 1 | Another table. |
";

    #[test]
    fn the_rows_of_a_table_follow_its_header() {
        let rows = rows(TEXT, "| Crate | Holds |");

        let lines: Vec<usize> = rows.iter().map(|row| row.line).collect();
        assert_eq!(lines, [3, 4]);
        assert_eq!(rows[1].cells, ["`field`, `selector`", "Fields."]);
    }

    #[test]
    fn a_crate_name_gets_its_prefix() {
        assert_eq!(crate_name("`kernel`"), Some("gaslightmq_kernel".to_owned()));
    }

    #[test]
    fn a_cell_names_several_crates() {
        assert_eq!(
            crate_names("`field`, `selector`"),
            Some(vec!["gaslightmq_field".to_owned(), "gaslightmq_selector".to_owned()])
        );
    }

    #[test]
    fn a_name_outside_backticks_is_no_crate() {
        assert_eq!(crate_name("kernel"), None);
        assert_eq!(crate_name("``"), None);
        assert_eq!(crate_names("`field`, selector"), None);
    }
}
