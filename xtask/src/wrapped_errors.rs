//! The check that an error type keeps an error it holds in a field `error`, and has no source.
//!
//! `crates.md` says that an error says everything in one message:
//! a variant that holds another error keeps it in a field named `error`, and `source()` returns `None`.
//!
//! [`error_types`] says what an error type is.
//! A field holds another error when its type is an error:
//! the name of the type ends in `Error`, or the type is an error type of the workspace, or it is `dyn Error`.
//! `Box`, `Arc`, `Rc`, `Option` and a reference around such a type hold an error too.
//!
//! - A field that holds an error is named `error`. A field of a tuple has no name, and fails.
//! - A `source()` that an error type writes returns `None` and nothing else.
//!   Leaving `source()` out is the same, and passes.
//! - A derived `Error` makes no field its source: no field has `#[source]` or `#[from]`, and no field is named `source`.
//!   Each makes its `source()` return that field, whatever the type of the field is.
//! - A derived `Error` is not `#[error(transparent)]`, which hands its message and its `source()` to the error it holds.
//!
//! What the check does not catch:
//!
//! - Whether the message writes the error it holds, and writes it once. That is the check of [`error_messages`].
//! - An error behind a type parameter, a type alias or a collection: `E`, `Failure` or `Vec<TopicNameError>`.
//! - What no check of the error rules catches. See [`error_types`].
//!
//! [`error_messages`]: crate::error_messages
//! [`error_types`]: crate::error_types

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use syn::{Attribute, Expr, Fields, ImplItem, Item, Stmt, Type};

use crate::error_types::{self, Held, derives_error, implements_error, last_name};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The name of the field that holds another error.
const ERROR_FIELD: &str = "error";

/// The attributes with which a derived `Error` makes a field its source.
const SOURCE_ATTRIBUTES: [&str; 2] = ["from", "source"];

/// The name with which a derived `Error` makes a field its source, without an attribute.
const SOURCE_FIELD: &str = "source";

/// The attribute that holds the message of a derived `Error`.
const MESSAGE_ATTRIBUTE: &str = "error";

/// What a derived `Error` writes in place of a message, to pass on the message and the source of the error it holds.
const TRANSPARENT: &str = "transparent";

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
    let error_types: Vec<BTreeSet<String>> = crates.iter().map(|files| error_types::names_in(files)).collect();
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
            if derived {
                in_message(&item.attrs, &item.ident.to_string(), file, violations);
            }
            for variant in &item.variants {
                let holder = format!("{}::{}", item.ident, variant.ident);
                if derived {
                    in_message(&variant.attrs, &holder, file, violations);
                }
                in_fields(&variant.fields, &holder, derived, file, known, violations);
            }
        }
        Item::Struct(item) if own.contains(&item.ident.to_string()) => {
            let derived = derives_error(&item.attrs);
            if derived {
                in_message(&item.attrs, &item.ident.to_string(), file, violations);
            }
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

/// Checks the attributes of a derived error struct, of a derived error enum or of one of its variants:
/// none of them is `#[error(transparent)]`.
fn in_message(attributes: &[Attribute], holder: &str, file: &SourceFile, violations: &mut Vec<WrappedErrorViolation>) {
    for attribute in attributes {
        if !attribute.path().is_ident(MESSAGE_ATTRIBUTE) {
            continue;
        }
        // A message that is no `transparent` does not parse as a name, and passes.
        if let Ok(written) = attribute.parse_args::<syn::Ident>()
            && written == TRANSPARENT
        {
            violations.push(WrappedErrorViolation::PassesSourceOn {
                path: file.path.clone(),
                line: written.span().start().line,
                holder: holder.to_owned(),
            });
        }
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
        let (path, holder) = (file.path.clone(), holder.to_owned());
        // The line on which a derived `Error` makes the field its source: that of the attribute, or of the name.
        let made_source = if derived { made_source(field) } else { None };
        match (held_error(&field.ty, known), &field.ident) {
            (Some(_), Some(name)) if name == ERROR_FIELD => {
                if let Some(line) = made_source {
                    violations.push(WrappedErrorViolation::HasSource { path, line, holder });
                }
            }
            (Some((held, _)), Some(name)) => violations.push(WrappedErrorViolation::NamedOtherwise {
                path,
                line: name.span().start().line,
                holder,
                field: name.to_string(),
                held,
            }),
            (Some((held, line)), None) => violations.push(WrappedErrorViolation::WithoutName {
                path,
                line,
                holder,
                held,
            }),
            // The check does not know the type as an error, and the derive makes the field a source all the same.
            (None, _) => {
                if let Some(line) = made_source {
                    violations.push(WrappedErrorViolation::HasSource { path, line, holder });
                }
            }
        }
    }
}

/// The line on which a derived `Error` makes a field its source:
/// the field has `#[source]` or `#[from]`, or it is named `source`.
fn made_source(field: &syn::Field) -> Option<usize> {
    let by_attribute = field.attrs.iter().find_map(|attribute| {
        let name = attribute.path().get_ident()?;
        SOURCE_ATTRIBUTES
            .iter()
            .any(|source| name == source)
            .then(|| name.span().start().line)
    });
    let by_name = field
        .ident
        .as_ref()
        .filter(|name| *name == SOURCE_FIELD)
        .map(|name| name.span().start().line);
    by_attribute.or(by_name)
}

/// The error that a field of this type holds, as the code writes its type, with the line of that type.
fn held_error(ty: &Type, known: &BTreeSet<&str>) -> Option<(String, usize)> {
    Held::of(ty)
        .filter(|held| held.is_error(known))
        .map(|held| (held.written, held.line))
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
    /// A derive makes the error type pass on the message and the `source()` of the error it holds.
    PassesSourceOn { path: PathBuf, line: usize, holder: String },
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
            Self::PassesSourceOn { path, line, holder } => write!(
                f,
                "{}:{line}: `{holder}` passes on the message and the `source()` of the error it holds; \
                 it writes a message of its own, and has no source",
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
                 Unreadable {{ error: WireError }},
                 RequestTimedOut,
                 UnsupportedVersion {{ api_key: i16 }},
                 ConnectionLost {{ reason: String }},
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
    fn a_derived_error_makes_no_field_its_source_whatever_the_type_of_the_field() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        Unreadable { #[source] cause: Failure },
                        Refused(#[from] Failure),
                        Unanswered { source: Failure, attempts: u32 },
                        Unwritable { reason: String, attempts: u32 },
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_native/src/error.rs:3: \
                 the `source()` of `ClientError::Unreadable` returns an error, and not `None`; \
                 the message of `ClientError::Unreadable` already says it",
                "crates/gaslightmq_kafka_client_native/src/error.rs:4: \
                 the `source()` of `ClientError::Refused` returns an error, and not `None`; \
                 the message of `ClientError::Refused` already says it",
                "crates/gaslightmq_kafka_client_native/src/error.rs:5: \
                 the `source()` of `ClientError::Unanswered` returns an error, and not `None`; \
                 the message of `ClientError::Unanswered` already says it",
            ]
        );
    }

    #[test]
    fn a_field_named_source_is_no_source_of_an_error_that_is_not_derived() {
        let text = format!("pub enum ClientError {{ Unanswered {{ source: Failure }} }}\n{IMPLEMENTS}");

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn a_derived_error_that_passes_its_source_on_fails() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        #[error(transparent)]
                        Broker { error: ResponseError },
                        #[error(\"connection lost: {reason}\")]
                        ConnectionLost { reason: String },
                    }
                    #[derive(Debug, thiserror::Error)]
                    #[error(transparent)]
                    pub struct ReadError { error: WireError }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_native/src/error.rs:3: \
                 `ClientError::Broker` passes on the message and the `source()` of the error it holds; \
                 it writes a message of its own, and has no source",
                "crates/gaslightmq_kafka_client_native/src/error.rs:9: \
                 `ReadError` passes on the message and the `source()` of the error it holds; \
                 it writes a message of its own, and has no source",
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
