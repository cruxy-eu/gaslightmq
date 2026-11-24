//! The check that no error type is made from the error of a library or of the system with a `From`.
//!
//! `crates.md` says that a crate turns the errors it meets into its own, where it calls what failed.
//! An `impl From<std::io::Error> for ClientError` turns them in one place for every call:
//! each `?` then makes the same variant, whatever the call was about, and the variant can only keep the error it got.
//!
//! [`error_types`] says what an error type is, what an error is, and where a file takes a type from.
//! An `impl From<T> for E` fails when `E` is an error type, and `T` is an error that the file takes from outside
//! the workspace: `std::io::Error`, `serde_json::Error` or `dyn Error`.
//! `Box`, `Arc`, `Rc`, `Option` and a reference around such a type are an error too.
//!
//! An error type made from an error of the workspace passes, and so does one made from what is no error.
//!
//! What the check does not catch:
//!
//! - An error of a library whose name does not end in `Error`: `Elapsed`, `Infallible`.
//! - An error of a library that a `use` with a `*` brings in.
//! - A `#[from]` on a field of a derived `Error`. The `wrapped-errors` check finds it.
//! - What no check of the error rules catches. See [`error_types`].
//!
//! [`error_types`]: crate::error_types

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use syn::{GenericArgument, Item, ItemImpl, PathArguments, Type};

use crate::error_types::{self, Held, Names, Origin, last_name};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The trait that makes one type from another, by the last name of its path.
const FROM: &str = "From";

/// Every error type of the workspace that a `From` makes from an error of a library or of the system.
pub fn violations(workspace: &Workspace) -> Result<Vec<ErrorConversionViolation>, SourceError> {
    let crates: Vec<Vec<SourceFile>> = workspace
        .members
        .iter()
        .map(|member| source::of(&workspace.root, member))
        .collect::<Result<_, _>>()?;
    Ok(in_crates(&crates))
}

/// The violations in the source of several crates, one list of files per crate.
fn in_crates(crates: &[Vec<SourceFile>]) -> Vec<ErrorConversionViolation> {
    let error_types: Vec<BTreeSet<String>> = crates.iter().map(|files| error_types::names_in(files)).collect();
    let known: BTreeSet<&str> = error_types.iter().flatten().map(String::as_str).collect();
    let mut violations = Vec::new();
    for file in crates.iter().flatten() {
        let names = Names::of(file);
        for item in file.items() {
            let Item::Impl(implementation) = item else { continue };
            let Some(made) = last_name(&implementation.self_ty).filter(|made| known.contains(made.as_str())) else {
                continue;
            };
            let from = made_from(implementation)
                .and_then(Held::of)
                .filter(|from| from.is_error(&known) && names.origin(from) == Origin::Outside);
            if let Some(from) = from {
                violations.push(ErrorConversionViolation {
                    path: file.path.clone(),
                    line: from.line,
                    made,
                    from: from.written,
                });
            }
        }
    }
    violations
}

/// The type that an `impl From<T> for E` makes an `E` from: `T`.
fn made_from(implementation: &ItemImpl) -> Option<&Type> {
    let (path, _) = implementation.trait_.as_ref()?;
    let last = path.segments.last().filter(|last| last.ident == FROM)?;
    let PathArguments::AngleBracketed(arguments) = &last.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(from) => Some(from),
        _ => None,
    })
}

/// A `From` that makes an error type from an error of a library or of the system.
/// `made` is `ClientError`, and `from` is `std::io::Error`.
#[derive(Debug, PartialEq)]
pub struct ErrorConversionViolation {
    path: PathBuf,
    line: usize,
    made: String,
    from: String,
}

impl fmt::Display for ErrorConversionViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: a `{FROM}` makes a `{}` from a `{}`, which is no error of the workspace; \
             the call that failed makes the `{}`, and keeps what the other said as text",
            self.path.display(),
            self.line,
            self.made,
            self.from,
            self.made
        )
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
        in_crates(&[vec![file("gaslightmq_kafka_client_api", text)]])
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    const CLIENT_ERROR: &str = "#[derive(Debug, thiserror::Error)]
                                pub enum ClientError { ConnectionLost { reason: String } }";

    #[test]
    fn an_error_type_made_from_an_error_of_the_system_fails() {
        let text = format!(
            "{CLIENT_ERROR}
             impl From<std::io::Error> for ClientError {{
                 fn from(error: std::io::Error) -> Self {{ Self::ConnectionLost {{ reason: error.to_string() }} }}
             }}"
        );

        assert_eq!(
            lines(&text),
            ["crates/gaslightmq_kafka_client_api/src/error.rs:3: \
              a `From` makes a `ClientError` from a `std::io::Error`, which is no error of the workspace; \
              the call that failed makes the `ClientError`, and keeps what the other said as text"]
        );
    }

    #[test]
    fn an_error_type_made_from_an_error_of_a_library_fails_under_a_wrapper_too() {
        let text = format!(
            "{CLIENT_ERROR}
             impl From<Box<serde_json::Error>> for ClientError {{}}
             use std::num::ParseIntError;
             impl From<&ParseIntError> for crate::ClientError {{}}
             impl From<Box<dyn std::error::Error + Send + Sync>> for ClientError {{}}"
        );

        assert_eq!(
            lines(&text),
            [
                "crates/gaslightmq_kafka_client_api/src/error.rs:3: \
                 a `From` makes a `ClientError` from a `serde_json::Error`, which is no error of the workspace; \
                 the call that failed makes the `ClientError`, and keeps what the other said as text",
                "crates/gaslightmq_kafka_client_api/src/error.rs:5: \
                 a `From` makes a `ClientError` from a `ParseIntError`, which is no error of the workspace; \
                 the call that failed makes the `ClientError`, and keeps what the other said as text",
                "crates/gaslightmq_kafka_client_api/src/error.rs:6: \
                 a `From` makes a `ClientError` from a `dyn Error`, which is no error of the workspace; \
                 the call that failed makes the `ClientError`, and keeps what the other said as text",
            ]
        );
    }

    #[test]
    fn an_error_type_made_from_an_error_of_the_workspace_passes() {
        let text = format!(
            "{CLIENT_ERROR}
             use crate::response::ResponseError;
             use gaslightmq_kernel::TopicNameError;
             impl From<ResponseError> for ClientError {{}}
             impl From<TopicNameError> for ClientError {{}}
             impl From<gaslightmq_wire::ReadError> for ClientError {{}}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn an_error_type_made_from_an_error_that_a_macro_writes_in_the_same_file_passes() {
        let text = format!(
            "{CLIENT_ERROR}
             text_value! {{ TopicName, TopicNameError }}
             impl From<TopicNameError> for ClientError {{}}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn an_error_type_made_from_what_is_no_error_passes() {
        let text = format!(
            "{CLIENT_ERROR}
             impl From<ErrorCode> for ClientError {{}}
             impl From<i16> for ClientError {{}}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn a_type_that_is_no_error_type_is_made_from_what_it_likes() {
        let text = "pub struct Outcome { text: String }
                    impl From<std::io::Error> for Outcome {}
                    impl From<std::io::Error> for String {}";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn another_trait_than_from_passes() {
        let text = format!(
            "{CLIENT_ERROR}
             impl PartialEq<std::io::Error> for ClientError {{}}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }

    #[test]
    fn a_conversion_in_test_code_passes() {
        let text = format!(
            "{CLIENT_ERROR}
             #[cfg(test)]
             mod tests {{
                 impl From<std::io::Error> for ClientError {{}}
             }}"
        );

        assert_eq!(lines(&text), [""; 0]);
    }
}
