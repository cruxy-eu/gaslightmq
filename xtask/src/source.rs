//! The Rust source of a crate, as the checks read it.
//!
//! A check reads every `.rs` file under the `src/` directory of a crate.
//! It does not follow `mod` declarations, so it reads no file that a `#[path]` attribute moves out of `src/`,
//! and it reads neither `build.rs` nor `tests/`, `benches/` and `examples/`.
//!
//! The checks read what is written, and not what a macro expands to:
//! a type or a function that a macro writes stays unseen.
//! Code under `#[cfg(test)]` or `#[test]` is test code, and the checks pass over it.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use syn::{Attribute, Item};

use crate::workspace::Member;

/// The directory of a crate that holds its source.
const SRC: &str = "src";

/// One source file, parsed.
pub struct SourceFile {
    /// Its path, relative to the workspace root: `crates/gaslightmq_field/src/part.rs`.
    pub path: PathBuf,
    /// The module it holds, as a path from the crate root: `logic`, `cluster` for `src/logic/cluster.rs`.
    /// The crate root itself is the empty path.
    pub module: Vec<String>,
    pub syntax: syn::File,
}

impl SourceFile {
    /// Parses the text of the file at `in_src`, a path relative to the `src/` directory of the crate in `crate_dir`.
    pub fn parse(crate_dir: &Path, in_src: &Path, text: &str) -> Result<Self, SourceError> {
        let path = crate_dir.join(SRC).join(in_src);
        match syn::parse_file(text) {
            Ok(syntax) => Ok(SourceFile {
                module: module_of(in_src),
                path,
                syntax,
            }),
            Err(error) => Err(SourceError::NotRust {
                path,
                line: error.span().start().line,
                error,
            }),
        }
    }

    /// Whether the whole file is test code: it starts with `#![cfg(test)]`.
    pub fn is_test(&self) -> bool {
        is_test(&self.syntax.attrs)
    }

    /// Every item outside test code, with the items of the modules that the file writes out in braces.
    /// An item inside a function body is not among them.
    pub fn items(&self) -> Vec<&Item> {
        let mut items = Vec::new();
        if !self.is_test() {
            collect(&self.syntax.items, &mut items);
        }
        items
    }
}

fn collect<'a>(from: &'a [Item], into: &mut Vec<&'a Item>) {
    for item in from {
        if is_test(attributes(item)) {
            continue;
        }
        into.push(item);
        if let Item::Mod(module) = item
            && let Some((_, inner)) = &module.content
        {
            collect(inner, into);
        }
    }
}

/// The module that a file holds: `src/logic/cluster.rs` and `src/logic/cluster/mod.rs` both hold `logic::cluster`.
fn module_of(in_src: &Path) -> Vec<String> {
    let mut module: Vec<String> = in_src
        .with_extension("")
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    let is_root = module.len() == 1 && matches!(module[0].as_str(), "lib" | "main");
    if is_root || module.last().is_some_and(|last| last == "mod") {
        module.pop();
    }
    module
}

/// Every source file of a crate, in the order of their paths.
pub fn of(root: &Path, member: &Member) -> Result<Vec<SourceFile>, SourceError> {
    let mut in_src = Vec::new();
    let src = root.join(&member.dir).join(SRC);
    if src.is_dir() {
        find(&src, Path::new(""), &mut in_src).map_err(|error| SourceError::Unreadable {
            path: member.dir.join(SRC),
            error,
        })?;
    }
    in_src.sort();
    in_src
        .iter()
        .map(|in_src| {
            let text = std::fs::read_to_string(src.join(in_src)).map_err(|error| SourceError::Unreadable {
                path: member.dir.join(SRC).join(in_src),
                error,
            })?;
            SourceFile::parse(&member.dir, in_src, &text)
        })
        .collect()
}

/// Adds the `.rs` files below `src/at` to `found`, each as a path relative to `src`.
fn find(src: &Path, at: &Path, found: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in std::fs::read_dir(src.join(at))? {
        let entry = entry?;
        let below = at.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            find(src, &below, found)?;
        } else if below.extension().is_some_and(|extension| extension == "rs") {
            found.push(below);
        }
    }
    Ok(())
}

/// Whether attributes mark test code: `#[cfg(test)]` or `#[test]`.
pub fn is_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let path = attribute.path();
        path.is_ident("test")
            || path.is_ident("cfg")
                && attribute
                    .parse_args::<syn::Ident>()
                    .is_ok_and(|condition| condition == "test")
    })
}

/// The attributes of an item.
pub fn attributes(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        // What syn does not know as an item has no attributes that it can tell.
        _ => &[],
    }
}

/// Why the source of a crate could not be read.
#[derive(Debug)]
pub enum SourceError {
    Unreadable {
        path: PathBuf,
        error: io::Error,
    },
    /// The file does not parse as Rust. `line` is where the parser stopped.
    NotRust {
        path: PathBuf,
        line: usize,
        error: syn::Error,
    },
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { path, error } => write!(f, "{} cannot be read: {error}", path.display()),
            Self::NotRust { path, line, error } => {
                write!(
                    f,
                    "{}:{line}: the checks cannot read this as Rust: {error}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for SourceError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(in_src: &str, text: &str) -> SourceFile {
        SourceFile::parse(Path::new("crates/gaslightmq_field"), Path::new(in_src), text).unwrap()
    }

    #[test]
    fn a_file_has_a_path_from_the_workspace_root() {
        assert_eq!(
            file("logic/cluster.rs", "").path,
            PathBuf::from("crates/gaslightmq_field/src/logic/cluster.rs")
        );
    }

    #[test]
    fn a_file_holds_the_module_that_its_path_spells() {
        assert_eq!(file("lib.rs", "").module, [""; 0]);
        assert_eq!(file("main.rs", "").module, [""; 0]);
        assert_eq!(file("logic.rs", "").module, ["logic"]);
        assert_eq!(file("logic/mod.rs", "").module, ["logic"]);
        assert_eq!(file("logic/cluster.rs", "").module, ["logic", "cluster"]);
        assert_eq!(file("logic/lib.rs", "").module, ["logic", "lib"]);
    }

    #[test]
    fn the_items_of_a_file_leave_test_code_out() {
        let file = file(
            "lib.rs",
            "struct Kept;
             mod inner { struct Nested; }
             #[cfg(test)] mod tests { struct Skipped; }
             #[test] fn skipped() {}",
        );

        let names: Vec<String> = file
            .items()
            .into_iter()
            .filter_map(|item| match item {
                Item::Struct(item) => Some(item.ident.to_string()),
                Item::Mod(item) => Some(item.ident.to_string()),
                Item::Fn(item) => Some(item.sig.ident.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(names, ["Kept", "inner", "Nested"]);
    }

    #[test]
    fn a_file_that_is_test_code_as_a_whole_has_no_items() {
        assert!(file("fixtures.rs", "#![cfg(test)]\nstruct Skipped;").items().is_empty());
    }

    #[test]
    fn a_condition_that_only_mentions_tests_is_not_test_code() {
        let file = file("lib.rs", "#[cfg(not(test))] struct Kept;");

        assert_eq!(file.items().len(), 1);
    }

    #[test]
    fn text_that_is_not_rust_fails_with_its_line() {
        let error = SourceFile::parse(
            Path::new("crates/gaslightmq_field"),
            Path::new("lib.rs"),
            "struct A;\nstruct;",
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "crates/gaslightmq_field/src/lib.rs:2: the checks cannot read this as Rust: expected identifier"
        );
    }
}
