//! The dependencies that a `Cargo.toml` names, and whether each one comes from the workspace.
//!
//! `cargo metadata` resolves `workspace = true` away, so only the text of a manifest says where a version comes from.
//! The reader goes through that text line by line, and it is no TOML parser. It knows:
//!
//! - a dependency table by its header: `[dependencies]`, `[dev-dependencies]` and `[build-dependencies]`,
//!   alone or under a target, as in `[target.'cfg(unix)'.dependencies]`;
//! - a dependency by its key in such a table: `serde.workspace = true` or `serde = { workspace = true }`,
//!   where a value may run over several lines until its brackets close;
//! - a dependency with a table of its own: `[dependencies.serde]`.
//!
//! `[workspace.dependencies]` is where the versions live, and the reader passes over it.
//! It does not read a string that runs over several lines, and it does not know a key in quotes with a `=` inside.

/// The last name of a header that starts a dependency table.
const DEPENDENCY_TABLES: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];

/// The table of the root manifest, which holds what the members inherit.
const WORKSPACE: &str = "workspace";

/// One dependency that a manifest names.
#[derive(Debug, PartialEq)]
pub struct Dependency {
    /// The line of its key or of its header, counted from one.
    pub line: usize,
    /// The header of its table, as the manifest writes it: `[dev-dependencies]`.
    pub table: String,
    /// Its key: `serde`.
    pub name: String,
    /// Whether it says `workspace = true`.
    pub from_workspace: bool,
}

/// The table that the reader is in.
enum Table {
    /// A table of dependencies, with its header.
    Dependencies(String),
    /// The table of one dependency, such as `[dependencies.serde]`. Its entry is the last one found.
    OneDependency,
    Other,
}

/// Every dependency that a manifest names, in the order of the text.
/// A dependency that several lines of one table describe comes once.
pub fn dependencies(text: &str) -> Vec<Dependency> {
    let mut found: Vec<Dependency> = Vec::new();
    let mut table = Table::Other;
    let mut lines = text.lines().enumerate();
    while let Some((index, raw)) = lines.next() {
        let mut code = code_of(raw);
        if code.trim().is_empty() {
            continue;
        }
        if code.trim_start().starts_with('[') {
            table = header(raw.trim(), code.trim(), index + 1, &mut found);
            continue;
        }
        // A value that opens a bracket runs on until the bracket closes.
        while depth(&code) > 0
            && let Some((_, next)) = lines.next()
        {
            code.push_str(&code_of(next));
        }
        let Some((key, value)) = code.split_once('=') else {
            continue;
        };
        let mut key = key.split('.').map(str::trim);
        let value: String = value.chars().filter(|c| !c.is_whitespace()).collect();
        match &table {
            Table::Dependencies(header) => {
                // The name comes from the line itself, which still has the text of a key in quotes.
                let name = raw.split(['=', '.']).next().unwrap_or_default();
                let name = name.trim().trim_matches(['"', '\'']).to_owned();
                let from_workspace = match key.nth(1) {
                    Some(WORKSPACE) => value == "true",
                    Some(_) => false,
                    None => value.contains("{workspace=true") || value.contains(",workspace=true"),
                };
                let known = found
                    .iter_mut()
                    .find(|dependency| dependency.table == *header && dependency.name == name);
                match known {
                    Some(dependency) => dependency.from_workspace |= from_workspace,
                    None => found.push(Dependency {
                        line: index + 1,
                        table: header.clone(),
                        name,
                        from_workspace,
                    }),
                }
            }
            Table::OneDependency => {
                if key.next() == Some(WORKSPACE)
                    && value == "true"
                    && let Some(dependency) = found.last_mut()
                {
                    dependency.from_workspace = true;
                }
            }
            Table::Other => {}
        }
    }
    found
}

/// The table that a header starts.
/// `raw` is the header as written, and `code` is the same without what its strings hold.
/// A header such as `[dependencies.serde]` names a dependency, which joins `found`.
fn header(raw: &str, code: &str, line: usize, found: &mut Vec<Dependency>) -> Table {
    if code.starts_with("[[") {
        return Table::Other;
    }
    let keys: Vec<&str> = code.trim_matches(['[', ']']).split('.').map(str::trim).collect();
    match keys[..] {
        [WORKSPACE, ..] => Table::Other,
        [.., last] if DEPENDENCY_TABLES.contains(&last) => Table::Dependencies(raw.to_owned()),
        [.., parent, name] if DEPENDENCY_TABLES.contains(&parent) => {
            found.push(Dependency {
                line,
                table: raw.to_owned(),
                name: name.to_owned(),
                from_workspace: false,
            });
            Table::OneDependency
        }
        _ => Table::Other,
    }
}

/// A line without its comment, and with every string emptied, so that no bracket, `=` or `#` in a string counts.
fn code_of(line: &str) -> String {
    let mut code = String::new();
    let mut characters = line.chars();
    while let Some(character) = characters.next() {
        match character {
            '#' => break,
            '"' | '\'' => {
                code.push(character);
                while let Some(inside) = characters.next() {
                    if inside == '\\' && character == '"' {
                        characters.next();
                    } else if inside == character {
                        break;
                    }
                }
                code.push(character);
            }
            _ => code.push(character),
        }
    }
    code
}

/// How many brackets and braces a text opens and does not close.
fn depth(code: &str) -> i32 {
    code.chars()
        .map(|character| match character {
            '[' | '{' => 1,
            ']' | '}' => -1,
            _ => 0,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each dependency as its table, its name, and whether it comes from the workspace.
    fn read(text: &str) -> Vec<(String, String, bool)> {
        dependencies(text)
            .into_iter()
            .map(|dependency| (dependency.table, dependency.name, dependency.from_workspace))
            .collect()
    }

    fn entry(table: &str, name: &str, from_workspace: bool) -> (String, String, bool) {
        (table.to_owned(), name.to_owned(), from_workspace)
    }

    #[test]
    fn a_dependency_says_whether_it_comes_from_the_workspace() {
        let text = "\
[package]
name = \"gaslightmq_field\"
version.workspace = true

[dependencies]
gaslightmq_kernel.workspace = true
serde = { workspace = true, optional = true }
tokio = { features = [\"net\"], workspace = true }
bytes = \"1\"
indexmap = { version = \"2\", features = [\"serde\"] }
gaslightmq_clock = { path = \"../gaslightmq_clock\" }
futures.workspace = false
";

        assert_eq!(
            read(text),
            [
                entry("[dependencies]", "gaslightmq_kernel", true),
                entry("[dependencies]", "serde", true),
                entry("[dependencies]", "tokio", true),
                entry("[dependencies]", "bytes", false),
                entry("[dependencies]", "indexmap", false),
                entry("[dependencies]", "gaslightmq_clock", false),
                entry("[dependencies]", "futures", false),
            ]
        );
    }

    #[test]
    fn a_dependency_has_the_line_of_its_key() {
        let text = "[package]\nname = \"a\"\n\n[dependencies]\n# The codec.\nbytes = \"1\"\n";

        assert_eq!(
            dependencies(text),
            [Dependency {
                line: 6,
                table: "[dependencies]".to_owned(),
                name: "bytes".to_owned(),
                from_workspace: false,
            }]
        );
    }

    #[test]
    fn every_kind_of_dependency_table_is_read() {
        let text = "\
[dev-dependencies]
serde_json = \"1\"

[build-dependencies]
cc.workspace = true

[target.'cfg(target_arch = \"wasm32\")'.dependencies]
web-sys = \"0.3\"

[features]
serde = [\"dep:serde\"]

[package.metadata.gaslightmq]
sans_io_modules = [\"logic\"]
";

        assert_eq!(
            read(text),
            [
                entry("[dev-dependencies]", "serde_json", false),
                entry("[build-dependencies]", "cc", true),
                entry(
                    "[target.'cfg(target_arch = \"wasm32\")'.dependencies]",
                    "web-sys",
                    false
                ),
            ]
        );
    }

    #[test]
    fn a_value_runs_over_several_lines() {
        let text = "\
[dependencies]
tokio = { workspace = true, features = [
    \"net\", # sockets
    \"io-util\",
] }
serde = { version = \"1\", features = [
    \"derive\",
] }
bytes.workspace = true
";

        assert_eq!(
            read(text),
            [
                entry("[dependencies]", "tokio", true),
                entry("[dependencies]", "serde", false),
                entry("[dependencies]", "bytes", true),
            ]
        );
    }

    #[test]
    fn a_dependency_with_a_table_of_its_own_is_read() {
        let text = "\
[dependencies.serde]
workspace = true
features = [\"derive\"]

[dependencies.bytes]
version = \"1\"

[dependencies]
indexmap.workspace = true
";

        assert_eq!(
            read(text),
            [
                entry("[dependencies.serde]", "serde", true),
                entry("[dependencies.bytes]", "bytes", false),
                entry("[dependencies]", "indexmap", true),
            ]
        );
    }

    #[test]
    fn a_dependency_on_several_lines_comes_once() {
        let text = "[dependencies]\nserde.workspace = true\nserde.features = [\"derive\"]\nbytes.version = \"1\"\n";

        assert_eq!(
            read(text),
            [
                entry("[dependencies]", "serde", true),
                entry("[dependencies]", "bytes", false)
            ]
        );
    }

    #[test]
    fn the_table_of_the_workspace_is_not_a_dependency_table() {
        let text = "[workspace.dependencies]\nbytes = \"1\"\n\n[workspace.dependencies.serde]\nversion = \"1\"\n";

        assert_eq!(read(text), []);
    }

    #[test]
    fn text_in_a_string_or_a_comment_is_no_code() {
        let text = "\
[dependencies]
# bytes = \"1\"
serde = { workspace = true, features = [\"a]b\"] } # , version = \"1\" }
indexmap = { version = \"{workspace=true\" }
\"quoted\" = \"1\"
";

        assert_eq!(
            read(text),
            [
                entry("[dependencies]", "serde", true),
                entry("[dependencies]", "indexmap", false),
                entry("[dependencies]", "quoted", false),
            ]
        );
    }
}
