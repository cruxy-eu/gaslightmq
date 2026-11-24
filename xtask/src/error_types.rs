//! The error types of the workspace, as the checks of the error rules know them.
//!
//! An error type is a struct or an enum that implements `Error` in its own crate, or derives it,
//! as `#[derive(thiserror::Error)]` does.
//! The checks know a type by its name, so an error type of one crate is an error type in every crate.
//!
//! A type that a field or a conversion names is an error when it is such an error type,
//! or when its name ends in `Error`: a macro writes many an error type, and a library has its own.
//! [`Names`] says where a file takes the type from: from the workspace, or from outside it.
//! An error that a crate of the workspace exports is an error of the workspace,
//! so the `ResponseError` that `gaslightmq_wire` hands on is one, and the one a file takes from the codec is not.
//!
//! What every check of the error rules misses:
//!
//! - An error type that a macro writes, or one inside a function body. See [`source`](crate::source).
//!   A check that reads the fields of error types does not read those of such a type.
//! - Two types with the same name, of which one is an error type.
//! - Where a name comes from that a `use` with a `*` brings in, or a `use` inside a function body.
//!   The checks take such a name for one of the workspace.

use std::collections::{BTreeMap, BTreeSet};

use syn::{Attribute, Fields, GenericArgument, Item, ItemImpl, PathArguments, Type, TypeParamBound, UseTree};

use crate::crate_names;
use crate::source::SourceFile;

/// The trait that makes a type an error type, by the last name of its path.
pub const ERROR_TRAIT: &str = "Error";

/// The types that hold what they wrap, so that a field of `Box<TopicNameError>` holds a `TopicNameError`.
const WRAPPERS: [&str; 4] = ["Arc", "Box", "Option", "Rc"];

/// The first names of a path that stays in the crate of the file: `crate::error::ReadError`, `Self::Error`.
const OWN_CRATE: [&str; 4] = ["crate", "self", "super", "Self"];

/// The names of the types that a crate makes an error type: it implements `Error` for them, or derives it.
pub fn names_in(files: &[SourceFile]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for item in files.iter().flat_map(SourceFile::items) {
        match item {
            Item::Impl(implementation) if implements_error(implementation) => {
                names.extend(last_name(&implementation.self_ty));
            }
            Item::Enum(item) if derives_error(&item.attrs) => {
                names.insert(item.ident.to_string());
            }
            Item::Struct(item) if derives_error(&item.attrs) => {
                names.insert(item.ident.to_string());
            }
            _ => {}
        }
    }
    names
}

/// One error type, as its crate writes it.
pub struct ErrorType<'a> {
    /// `ClientError`.
    pub name: String,
    pub attributes: &'a [Attribute],
    /// Whether a derive writes its message, from the `#[error("...")]` attributes.
    pub derived: bool,
    /// What holds its fields: the struct itself, or each variant of the enum.
    pub holders: Vec<Holder<'a>>,
}

/// What holds the fields of an error: an error struct, or one variant of an error enum.
pub struct Holder<'a> {
    /// `ReadError` for a struct, and `ClientError::Refused` for a variant.
    pub name: String,
    /// The line of that name.
    pub line: usize,
    /// The attributes of the variant. An error struct has none but those of the type.
    pub attributes: &'a [Attribute],
    pub fields: &'a Fields,
}

impl<'a> ErrorType<'a> {
    /// The error type that an item defines, when its crate makes it one. `own` holds the error types of that crate.
    pub fn of(item: &'a Item, own: &BTreeSet<String>) -> Option<Self> {
        match item {
            Item::Enum(item) if own.contains(&item.ident.to_string()) => Some(ErrorType {
                name: item.ident.to_string(),
                attributes: &item.attrs,
                derived: derives_error(&item.attrs),
                holders: item
                    .variants
                    .iter()
                    .map(|variant| Holder {
                        name: format!("{}::{}", item.ident, variant.ident),
                        line: variant.ident.span().start().line,
                        attributes: &variant.attrs,
                        fields: &variant.fields,
                    })
                    .collect(),
            }),
            Item::Struct(item) if own.contains(&item.ident.to_string()) => Some(ErrorType {
                name: item.ident.to_string(),
                attributes: &item.attrs,
                derived: derives_error(&item.attrs),
                holders: vec![Holder {
                    name: item.ident.to_string(),
                    line: item.ident.span().start().line,
                    attributes: &[],
                    fields: &item.fields,
                }],
            }),
            _ => None,
        }
    }
}

/// The type that a field or a conversion holds, under `Box`, `Arc`, `Rc`, `Option` and a reference.
pub struct Held {
    /// The type as the code writes it: `std::io::Error`, or `dyn Error` for a trait object of `Error`.
    pub written: String,
    /// The last name of its path: `Error`.
    name: String,
    /// The line of that name.
    pub line: usize,
    /// The start of its path: `std`. A trait object has none.
    start: Option<Start>,
}

/// How the path of a type starts.
struct Start {
    /// The first name of the path: `std` for `std::io::Error`, and `ReadError` for `ReadError`.
    first: String,
    /// Whether more names follow the first one.
    has_more: bool,
    /// Whether the path starts with `::`, so that its first name is a crate.
    from_a_crate: bool,
}

impl Held {
    /// The type that this type holds. A type that is no path and no trait object of `Error` holds none: a tuple, a slice.
    pub fn of(ty: &Type) -> Option<Held> {
        match ty {
            Type::Path(ty) if ty.qself.is_none() => {
                let (first, last) = (ty.path.segments.first()?, ty.path.segments.last()?);
                let name = last.ident.to_string();
                if WRAPPERS.contains(&name.as_str())
                    && let PathArguments::AngleBracketed(arguments) = &last.arguments
                {
                    return arguments.args.iter().find_map(|argument| match argument {
                        GenericArgument::Type(wrapped) => Held::of(wrapped),
                        _ => None,
                    });
                }
                let written: Vec<String> = ty
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect();
                Some(Held {
                    written: written.join("::"),
                    name,
                    line: last.ident.span().start().line,
                    start: Some(Start {
                        first: first.ident.to_string(),
                        has_more: ty.path.segments.len() > 1,
                        from_a_crate: ty.path.leading_colon.is_some(),
                    }),
                })
            }
            Type::TraitObject(object) => object.bounds.iter().find_map(|bound| match bound {
                TypeParamBound::Trait(bound) => {
                    let last = bound.path.segments.last()?;
                    (last.ident == ERROR_TRAIT).then(|| Held {
                        written: format!("dyn {ERROR_TRAIT}"),
                        name: ERROR_TRAIT.to_owned(),
                        line: last.ident.span().start().line,
                        start: None,
                    })
                }
                _ => None,
            }),
            Type::Reference(reference) => Held::of(&reference.elem),
            Type::Paren(inner) => Held::of(&inner.elem),
            Type::Group(inner) => Held::of(&inner.elem),
            _ => None,
        }
    }

    /// Whether the type is an error: an error type of a crate of the workspace, or a type whose name ends in `Error`.
    /// `known` holds the error types of every crate.
    pub fn is_error(&self, known: &BTreeSet<&str>) -> bool {
        self.name.ends_with(ERROR_TRAIT) || known.contains(self.name.as_str())
    }
}

/// Where a file takes a type from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Origin {
    /// From a crate of the workspace: its own crate, or one whose name starts with `gaslightmq_`.
    Workspace,
    /// From a library or from the system.
    Outside,
    /// From nowhere that the file names: the file defines the type, a macro writes it there, or a `use` with a `*`
    /// brings it in. A type of the prelude and a type parameter come from nowhere too: `String`, `E`.
    Unnamed,
}

impl Origin {
    /// The origin of what a crate with this name holds.
    fn of_crate(name: &str) -> Origin {
        if name.starts_with(crate_names::PREFIX) {
            Origin::Workspace
        } else {
            Origin::Outside
        }
    }
}

/// The names that one file brings in with `use`, and the modules it declares.
pub struct Names {
    /// Each name that a `use` brings in, with the origin of the path it comes from.
    /// `use std::io::{self, Read as _}` brings in `io`, from outside.
    used: BTreeMap<String, Origin>,
    modules: BTreeSet<String>,
}

impl Names {
    /// Reads the `use` and `mod` items of a file, outside its test code and its function bodies.
    pub fn of(file: &SourceFile) -> Names {
        let items = file.items();
        let modules: BTreeSet<String> = items
            .iter()
            .filter_map(|item| match item {
                Item::Mod(module) => Some(module.ident.to_string()),
                _ => None,
            })
            .collect();
        let mut used = BTreeMap::new();
        for item in items {
            let Item::Use(item) = item else { continue };
            // The first name of a `use` is a crate, a word for the own crate, or a module that the file declares.
            let origin = |first: &str| {
                if item.leading_colon.is_none() && (OWN_CRATE.contains(&first) || modules.contains(first)) {
                    Origin::Workspace
                } else {
                    Origin::of_crate(first)
                }
            };
            bring_in(&item.tree, None, &origin, &mut used);
        }
        Names { used, modules }
    }

    /// Where the file takes a type from.
    pub fn origin(&self, held: &Held) -> Origin {
        let Some(start) = &held.start else {
            // A trait object of `Error` is the error of anything.
            return Origin::Outside;
        };
        let first = start.first.as_str();
        if start.from_a_crate {
            Origin::of_crate(first)
        } else if OWN_CRATE.contains(&first) || self.modules.contains(first) {
            Origin::Workspace
        } else if let Some(origin) = self.used.get(first) {
            *origin
        } else if start.has_more {
            // A path that starts with a name from nowhere starts with a crate: `serde_json::Error`.
            Origin::of_crate(first)
        } else {
            Origin::Unnamed
        }
    }
}

/// Notes the names that one `use` brings in. `path` holds the first and the last name of the path so far.
fn bring_in<'a>(
    tree: &'a UseTree,
    path: Option<(&'a syn::Ident, &'a syn::Ident)>,
    origin: &impl Fn(&str) -> Origin,
    used: &mut BTreeMap<String, Origin>,
) {
    let first_of = |name: &'a syn::Ident| path.map_or(name, |(first, _)| first);
    match tree {
        UseTree::Path(step) => bring_in(&step.tree, Some((first_of(&step.ident), &step.ident)), origin, used),
        UseTree::Group(group) => {
            for tree in &group.items {
                bring_in(tree, path, origin, used);
            }
        }
        // `use std::io::{self}` brings in `io`.
        UseTree::Name(name) if name.ident == "self" => {
            if let Some((first, last)) = path {
                used.insert(last.to_string(), origin(&first.to_string()));
            }
        }
        UseTree::Name(name) => {
            used.insert(name.ident.to_string(), origin(&first_of(&name.ident).to_string()));
        }
        UseTree::Rename(rename) => {
            used.insert(rename.rename.to_string(), origin(&first_of(&rename.ident).to_string()));
        }
        // What a `*` brings in has no name here.
        UseTree::Glob(_) => {}
    }
}

/// Whether an `impl` block implements `Error`: `impl std::error::Error for TopicNameError`.
pub fn implements_error(implementation: &ItemImpl) -> bool {
    implementation
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
        .is_some_and(|last| last.ident == ERROR_TRAIT)
}

/// Whether a type derives `Error`, as `#[derive(thiserror::Error)]` does.
pub fn derives_error(attributes: &[Attribute]) -> bool {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("derive"))
        .any(|attribute| {
            let mut derives = false;
            // A derive that does not parse is one the compiler rejects too.
            let _ = attribute.parse_nested_meta(|derived| {
                derives |= derived
                    .path
                    .segments
                    .last()
                    .is_some_and(|last| last.ident == ERROR_TRAIT);
                Ok(())
            });
            derives
        })
}

/// The last name of the path that a type is written as: `TopicNameError` for `crate::TopicNameError<'a>`.
pub fn last_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(ty) => ty.path.segments.last().map(|last| last.ident.to_string()),
        Type::Reference(reference) => last_name(&reference.elem),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn file(text: &str) -> SourceFile {
        SourceFile::parse(Path::new("crates/gaslightmq_wire"), Path::new("error.rs"), text).unwrap()
    }

    fn held(ty: &str) -> Option<Held> {
        Held::of(&syn::parse_str(ty).unwrap())
    }

    #[test]
    fn a_type_that_derives_or_implements_error_is_an_error_type() {
        let file = file(
            "#[derive(Debug, thiserror::Error)] pub enum ReadError { Truncated }
             #[derive(Debug, Error)] pub struct WriteError;
             pub struct FrameError;
             impl std::error::Error for FrameError {}
             #[derive(Debug)] pub struct Frame;",
        );

        let names = names_in(&[file]);

        assert_eq!(
            names.into_iter().collect::<Vec<_>>(),
            ["FrameError", "ReadError", "WriteError"]
        );
    }

    #[test]
    fn an_error_enum_holds_its_fields_in_its_variants_and_an_error_struct_in_itself() {
        let file = file(
            "#[derive(Debug, thiserror::Error)] pub enum ReadError { Truncated { length: usize }, Empty }
             pub struct FrameError { length: usize }
             impl std::error::Error for FrameError {}
             pub struct Frame { length: usize }",
        );
        let own = names_in(std::slice::from_ref(&file));

        let error_types: Vec<ErrorType<'_>> = file
            .items()
            .into_iter()
            .filter_map(|item| ErrorType::of(item, &own))
            .collect();

        let holders: Vec<Vec<&str>> = error_types
            .iter()
            .map(|error_type| error_type.holders.iter().map(|holder| holder.name.as_str()).collect())
            .collect();
        assert_eq!(
            holders,
            [vec!["ReadError::Truncated", "ReadError::Empty"], vec!["FrameError"]]
        );
        let derived: Vec<bool> = error_types.iter().map(|error_type| error_type.derived).collect();
        assert_eq!(derived, [true, false]);
    }

    #[test]
    fn a_wrapper_and_a_reference_hold_what_they_wrap() {
        let written = |ty: &str| held(ty).map(|held| held.written);

        assert_eq!(written("std::io::Error").as_deref(), Some("std::io::Error"));
        assert_eq!(written("Box<ClientError>").as_deref(), Some("ClientError"));
        assert_eq!(
            written("Option<Arc<wire::ReadError>>").as_deref(),
            Some("wire::ReadError")
        );
        assert_eq!(written("&'static ReadError").as_deref(), Some("ReadError"));
        assert_eq!(
            written("Box<dyn std::error::Error + Send>").as_deref(),
            Some("dyn Error")
        );
        assert_eq!(written("Vec<ReadError>").as_deref(), Some("Vec"));
        assert_eq!(written("(ReadError, usize)"), None);
        assert_eq!(written("Box<dyn Send>"), None);
    }

    #[test]
    fn an_error_is_an_error_type_or_has_a_name_that_ends_in_error() {
        let known = BTreeSet::from(["UnsupportedIncidents"]);
        let is_error = |ty: &str| held(ty).unwrap().is_error(&known);

        assert!(is_error("Box<UnsupportedIncidents>"));
        assert!(is_error("TopicNameError"));
        assert!(is_error("std::io::Error"));
        assert!(is_error("&dyn Error"));
        assert!(!is_error("String"));
        assert!(!is_error("ErrorCode"));
    }

    /// Where a file with these `use` and `mod` items takes a type from.
    fn origin(items: &str, ty: &str) -> Origin {
        Names::of(&file(items)).origin(&held(ty).unwrap())
    }

    #[test]
    fn a_type_of_the_own_crate_or_of_a_crate_of_the_workspace_is_of_the_workspace() {
        let items = "use crate::read::ReadError;
                     use gaslightmq_kernel::{TopicName, TopicNameError as NameError};
                     use gaslightmq_wire::{self as wire, frame};
                     use super::*;
                     mod write;";

        for ty in [
            "ReadError",
            "Box<NameError>",
            "wire::ResponseError",
            "frame::FrameError",
            "write::WriteError",
            "crate::read::ReadError",
            "super::ReadError",
            "Self::Error",
            "gaslightmq_field::PartError",
            "::gaslightmq_field::PartError",
        ] {
            assert_eq!(origin(items, ty), Origin::Workspace, "{ty}");
        }
    }

    #[test]
    fn a_type_of_a_library_or_of_the_system_is_from_outside() {
        let items = "use std::io::{self, Read};
                     use std::num::ParseIntError;
                     use kafka_protocol::error::ResponseError;
                     use tokio::time::error as time;
                     use ::serde_json;
                     mod read;";

        for ty in [
            "io::Error",
            "ParseIntError",
            "Option<ResponseError>",
            "time::Elapsed",
            "serde_json::Error",
            "std::fmt::Error",
            "syn::Error",
            "::read::ReadError",
            "Box<dyn std::error::Error>",
        ] {
            assert_eq!(origin(items, ty), Origin::Outside, "{ty}");
        }
    }

    #[test]
    fn a_type_that_no_use_brings_in_by_its_name_comes_from_nowhere() {
        let items = "use gaslightmq_kernel::prelude::*;
                     use std::io::*;
                     #[cfg(test)]
                     mod tests { use std::io::Error; }";

        for ty in ["TopicNameError", "Error", "String", "E"] {
            assert_eq!(origin(items, ty), Origin::Unnamed, "{ty}");
        }
    }
}
