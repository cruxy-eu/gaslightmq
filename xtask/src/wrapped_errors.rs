//! The check that an error type keeps an error it holds in a field `error`, and has no source.
//!
//! `crates.md` says that an error says everything in one message:
//! a variant that holds another error keeps it in a field named `error`, and `source()` returns `None`.
//!
//! An error type is a struct or an enum that implements `Error` in its own crate, or derives it.
//! A field holds another error when its type is an error:
//! the name of the type ends in `Error`, or a crate of the workspace implements `Error` for a type of that name,
//! or the type is `dyn Error`.
//! `Box`, `Arc`, `Rc`, `Option` and a reference around such a type hold an error too.
//!
//! - A field that holds an error is named `error`. A field of a tuple has no name, and fails.
//! - A `source()` that an error type writes returns `None` and nothing else.
//!   Leaving `source()` out is the same, and passes.
//! - A derived `Error` has no field with `#[source]` or `#[from]`, which make its `source()` return that field.
//!
//! What the check does not catch:
//!
//! - Whether the message writes the error it holds, and writes it once. No check reads a `Display`.
//! - An error behind a type parameter, a type alias or a collection: `E`, `Failure` or `Vec<TopicNameError>`.
//! - An error type that a macro writes, or one inside a function body. See [`source`].
//! - Two types of one crate with the same name, of which one is an error type. The check knows a type by its name.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use syn::{
    Attribute, Expr, Fields, GenericArgument, ImplItem, Item, ItemImpl, PathArguments, Stmt, Type, TypeParamBound,
};

use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The name of the field that holds another error.
const ERROR_FIELD: &str = "error";

/// The trait that makes a type an error type, by the last name of its path.
const ERROR_TRAIT: &str = "Error";

/// The types that hold what they wrap, so that a field of `Box<TopicNameError>` holds a `TopicNameError`.
const WRAPPERS: [&str; 4] = ["Arc", "Box", "Option", "Rc"];

/// The attributes with which a derived `Error` makes a field its source.
const SOURCE_ATTRIBUTES: [&str; 2] = ["from", "source"];

/// Every error type of the workspace that holds another error the wrong way.
pub fn violations(workspace: &Workspace) -> Result<Vec<WrappedErrorViolation>, SourceError> {
    let crates: Vec<Vec<SourceFile>> = workspace
        .members
        .iter()
        .map(|member| source::of(&workspace.root, member))
        .collect::<Result<_, _>>()?;
    Ok(in_crates(&crates))
}

/// The violations in the source of several crates, one list of files per crate.
fn in_crates(crates: &[Vec<SourceFile>]) -> Vec<WrappedErrorViolation> {
    let error_types: Vec<BTreeSet<String>> = crates.iter().map(|files| error_types(files)).collect();
    // A field holds an error when its type is an error type of any crate of the workspace.
    let known: BTreeSet<&str> = error_types.iter().flatten().map(String::as_str).collect();
    let mut violations = Vec::new();
    for (files, own) in crates.iter().zip(&error_types) {
        for file in files {
            for item in file.items() {
                in_item(item, file, own, &known, &mut violations);
            }
        }
    }
    violations
}

/// The names of the types that a crate makes an error type: it implements `Error` for them, or derives it.
fn error_types(files: &[SourceFile]) -> BTreeSet<String> {
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

fn in_item(
    item: &Item,
    file: &SourceFile,
    own: &BTreeSet<String>,
    known: &BTreeSet<&str>,
    violations: &mut Vec<WrappedErrorViolation>,
) {
    match item {
        Item::Enum(item) if own.contains(&item.ident.to_string()) => {
            let derived = derives_error(&item.attrs);
            for variant in &item.variants {
                let holder = format!("{}::{}", item.ident, variant.ident);
                in_fields(&variant.fields, &holder, derived, file, known, violations);
            }
        }
        Item::Struct(item) if own.contains(&item.ident.to_string()) => {
            let derived = derives_error(&item.attrs);
            in_fields(&item.fields, &item.ident.to_string(), derived, file, known, violations);
        }
        Item::Impl(implementation) if implements_error(implementation) => {
            let source = implementation.items.iter().find_map(|item| match item {
                ImplItem::Fn(function) if function.sig.ident == "source" => Some(function),
                _ => None,
            });
            if let Some(source) = source
                && !returns_none(&source.block.stmts)
            {
                violations.push(WrappedErrorViolation::HasSource {
                    path: file.path.clone(),
                    line: source.sig.ident.span().start().line,
                    holder: last_name(&implementation.self_ty).unwrap_or_default(),
                });
            }
        }
        _ => {}
    }
}

/// Checks the fields of one error struct, or of one variant of an error enum.
fn in_fields(
    fields: &Fields,
    holder: &str,
    derived: bool,
    file: &SourceFile,
    known: &BTreeSet<&str>,
    violations: &mut Vec<WrappedErrorViolation>,
) {
    for field in fields {
        let Some((held, line)) = held_error(&field.ty, known) else {
            continue;
        };
        let (path, holder) = (file.path.clone(), holder.to_owned());
        match &field.ident {
            Some(name) if name == ERROR_FIELD => {
                let makes_it_the_source = field
                    .attrs
                    .iter()
                    .any(|attribute| SOURCE_ATTRIBUTES.iter().any(|source| attribute.path().is_ident(source)));
                if derived && makes_it_the_source {
                    let line = name.span().start().line;
                    violations.push(WrappedErrorViolation::HasSource { path, line, holder });
                }
            }
            Some(name) => violations.push(WrappedErrorViolation::NamedOtherwise {
                path,
                line: name.span().start().line,
                holder,
                field: name.to_string(),
                held,
            }),
            None => violations.push(WrappedErrorViolation::WithoutName {
                path,
                line,
                holder,
                held,
            }),
        }
    }
}

/// The error that a field of this type holds, as the code writes its type, with the line of that type.
fn held_error(ty: &Type, known: &BTreeSet<&str>) -> Option<(String, usize)> {
    match ty {
        Type::Path(ty) if ty.qself.is_none() => {
            let last = ty.path.segments.last()?;
            let name = last.ident.to_string();
            if name.ends_with(ERROR_TRAIT) || known.contains(name.as_str()) {
                let written: Vec<String> = ty
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect();
                return Some((written.join("::"), last.ident.span().start().line));
            }
            if !WRAPPERS.contains(&name.as_str()) {
                return None;
            }
            let PathArguments::AngleBracketed(arguments) = &last.arguments else {
                return None;
            };
            arguments.args.iter().find_map(|argument| match argument {
                GenericArgument::Type(wrapped) => held_error(wrapped, known),
                _ => None,
            })
        }
        Type::TraitObject(object) => object.bounds.iter().find_map(|bound| match bound {
            TypeParamBound::Trait(bound) => {
                let last = bound.path.segments.last()?;
                (last.ident == ERROR_TRAIT).then(|| (format!("dyn {ERROR_TRAIT}"), last.ident.span().start().line))
            }
            _ => None,
        }),
        Type::Reference(reference) => held_error(&reference.elem, known),
        Type::Paren(inner) => held_error(&inner.elem, known),
        Type::Group(inner) => held_error(&inner.elem, known),
        _ => None,
    }
}

/// Whether an `impl` block implements `Error`: `impl std::error::Error for TopicNameError`.
fn implements_error(implementation: &ItemImpl) -> bool {
    implementation
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
        .is_some_and(|last| last.ident == ERROR_TRAIT)
}

/// Whether a type derives `Error`, as `#[derive(thiserror::Error)]` does.
fn derives_error(attributes: &[Attribute]) -> bool {
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
fn last_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(ty) => ty.path.segments.last().map(|last| last.ident.to_string()),
        Type::Reference(reference) => last_name(&reference.elem),
        _ => None,
    }
}

/// Whether the body of a function is `None` and nothing else.
fn returns_none(body: &[Stmt]) -> bool {
    matches!(body, [Stmt::Expr(Expr::Path(value), None)] if value.path.is_ident("None"))
}

/// A way in which an error type holds another error against the rule.
#[derive(Debug, PartialEq)]
pub enum WrappedErrorViolation {
    /// A field of a tuple holds the error. `holder` is `ClientError::Broker`, and `held` is `ResponseError`.
    WithoutName {
        path: PathBuf,
        line: usize,
        holder: String,
        held: String,
    },
    /// A field with another name than `error` holds the error.
    NamedOtherwise {
        path: PathBuf,
        line: usize,
        holder: String,
        field: String,
        held: String,
    },
    /// The `source()` of the error type returns something, or a derive makes it return a field.
    HasSource { path: PathBuf, line: usize, holder: String },
}

impl fmt::Display for WrappedErrorViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutName {
                path,
                line,
                holder,
                held,
            } => write!(
                f,
                "{}:{line}: `{holder}` holds a `{held}` in a field without a name, \
                 and not in a field named `{ERROR_FIELD}`",
                path.display()
            ),
            Self::NamedOtherwise {
                path,
                line,
                holder,
                field,
                held,
            } => write!(
                f,
                "{}:{line}: `{holder}` holds a `{held}` in the field `{field}`, \
                 and not in a field named `{ERROR_FIELD}`",
                path.display()
            ),
            Self::HasSource { path, line, holder } => write!(
                f,
                "{}:{line}: the `source()` of `{holder}` returns an error, and not `None`; \
                 the message of `{holder}` already says it",
                path.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn file(krate: &str, text: &str) -> SourceFile {
        SourceFile::parse(&Path::new("crates").join(krate), Path::new("error.rs"), text).unwrap()
    }

    /// The violations in one file of one crate, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        in_crates(&[vec![file("gaslightmq_kafka_client_native", text)]])
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    const IMPLEMENTS: &str = "impl std::error::Error for ClientError {}";

    #[test]
    fn an_error_in_a_field_named_error_passes() {
        let text = format!(
            "pub enum ClientError {{
                 Broker {{ error: ResponseError }},
                 CallTimedOut {{ error: Box<ClientError>, attempts: u32 }},
                 Unreadable {{ error: std::io::Error }},
                 RequestTimedOut,
                 UnsupportedVersion {{ api_key: i16 }},
                 Protocol(String),
             }}
             {IMPLEMENTS}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn an_error_in_a_tuple_field_fails() {
        let text = format!("pub enum ClientError {{\n    Broker(ResponseError),\n}}\n{IMPLEMENTS}");

        assert_eq!(
            lines(&text),
            ["crates/gaslightmq_kafka_client_native/src/error.rs:2: \
              `ClientError::Broker` holds a `ResponseError` in a field without a name, \
              and not in a field named `error`"]
        );
    }

    #[test]
    fn an_error_in_a_field_with_another_name_fails() {
        let text = format!(
            "pub enum ClientError {{
                 Unreadable {{ cause: std::io::Error }},
                 CallTimedOut {{ last: Option<Box<ClientError>> }},
             }}
             {IMPLEMENTS}"
        );

        assert_eq!(
            lines(&text),
            [
                "crates/gaslightmq_kafka_client_native/src/error.rs:2: \
                 `ClientError::Unreadable` holds a `std::io::Error` in the field `cause`, \
                 and not in a field named `error`",
                "crates/gaslightmq_kafka_client_native/src/error.rs:3: \
                 `ClientError::CallTimedOut` holds a `ClientError` in the field `last`, \
                 and not in a field named `error`",
            ]
        );
    }

    #[test]
    fn an_error_struct_is_held_to_the_same_rule() {
        let text = "pub struct ReadError(pub std::io::Error);
                    pub struct ParseError { source: Box<dyn std::error::Error + Send + Sync> }
                    impl core::error::Error for ReadError {}
                    impl Error for ParseError {}";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_native/src/error.rs:1: \
                 `ReadError` holds a `std::io::Error` in a field without a name, \
                 and not in a field named `error`",
                "crates/gaslightmq_kafka_client_native/src/error.rs:2: \
                 `ParseError` holds a `dyn Error` in the field `source`, \
                 and not in a field named `error`",
            ]
        );
    }

    #[test]
    fn a_type_that_is_no_error_type_holds_errors_as_it_likes() {
        let text = "pub struct Attempt { last: Option<ClientError>, failures: Vec<ClientError> }
                    pub enum Outcome { Failed(ClientError), Refused(ResponseError) }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn what_is_no_error_passes_in_any_field() {
        let text = format!(
            "pub enum ClientError {{
                 Refused(ErrorCode),
                 Unknown {{ kind: std::io::ErrorKind, errors: Vec<TopicNameError>, text: String }},
             }}
             {IMPLEMENTS}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn an_error_type_of_the_workspace_is_an_error_under_any_name() {
        let incident = file(
            "gaslightmq_incident",
            "pub struct UnsupportedIncidents { kinds: Vec<IncidentKind> }
             impl std::error::Error for UnsupportedIncidents {}",
        );
        let world = file(
            "gaslightmq_world",
            "pub enum WorldError { Unsupported(UnsupportedIncidents), Unknown(IncidentKind) }
             impl std::error::Error for WorldError {}",
        );

        let violations = in_crates(&[vec![incident], vec![world]]);

        assert_eq!(
            violations,
            [WrappedErrorViolation::WithoutName {
                path: PathBuf::from("crates/gaslightmq_world/src/error.rs"),
                line: 1,
                holder: "WorldError::Unsupported".to_owned(),
                held: "UnsupportedIncidents".to_owned(),
            }]
        );
    }

    #[test]
    fn a_source_that_returns_none_passes() {
        let text = "pub enum ClientError { Broker { error: ResponseError } }
                    impl std::error::Error for ClientError {
                        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                            None
                        }
                    }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_source_that_returns_an_error_fails() {
        let text = "pub enum ClientError { Broker { error: ResponseError } }
                    impl std::error::Error for ClientError {
                        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                            match self { Self::Broker { error } => Some(error) }
                        }
                    }";

        assert_eq!(
            lines(text),
            ["crates/gaslightmq_kafka_client_native/src/error.rs:3: \
              the `source()` of `ClientError` returns an error, and not `None`; \
              the message of `ClientError` already says it"]
        );
    }

    #[test]
    fn a_derived_error_is_an_error_type() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        Broker(#[from] ResponseError),
                        Unreadable { #[source] error: std::io::Error },
                        Protocol { error: WireError },
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_native/src/error.rs:3: \
                 `ClientError::Broker` holds a `ResponseError` in a field without a name, \
                 and not in a field named `error`",
                "crates/gaslightmq_kafka_client_native/src/error.rs:4: \
                 the `source()` of `ClientError::Unreadable` returns an error, and not `None`; \
                 the message of `ClientError::Unreadable` already says it",
            ]
        );
    }

    #[test]
    fn an_error_type_in_test_code_passes() {
        let text = "#[cfg(test)]
                    mod tests {
                        enum TestError { Failed(std::io::Error) }
                        impl std::error::Error for TestError {}
                    }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_type_is_known_across_the_files_of_its_crate() {
        let definition = file("gaslightmq_wire", "pub enum WireError { Truncated(std::io::Error) }");
        let implementation = SourceFile::parse(
            Path::new("crates/gaslightmq_wire"),
            Path::new("lib.rs"),
            "impl std::error::Error for error::WireError {}",
        )
        .unwrap();

        assert_eq!(in_crates(&[vec![definition, implementation]]).len(), 1);
    }
}
