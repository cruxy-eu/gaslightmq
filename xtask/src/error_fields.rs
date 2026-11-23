//! The check that every field of an error type has a name, and that a field `error` holds an error of the workspace.
//!
//! `crates.md` says that a crate turns the errors it meets into its own:
//! what a library or the system said is text, in a field named `reason`,
//! and a field named `error` keeps the error of a crate of the workspace.
//!
//! [`error_types`] says what an error type is, what an error is, and where a file takes a type from.
//!
//! - An error struct and a variant of an error enum have fields with names, or no field: no field of a tuple.
//! - A field named `error` holds an error that its file takes from the workspace,
//!   also under `Box`, `Arc`, `Rc`, `Option` or a reference.
//!   A `std::io::Error` is an error from outside, and a `String` and a type parameter are no error: all three fail.
//!
//! What the check does not catch:
//!
//! - What a field with another name holds. The `wrapped-errors` check finds an error among them.
//! - Whether a `reason` is text.
//! - An error of a library that a `use` with a `*` brings in.
//! - What no check of the error rules catches. See [`error_types`].
//!
//! [`error_types`]: crate::error_types

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use syn::Fields;

use crate::error_types::{self, ErrorType, Held, Holder, Names, Origin};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The name of the field that holds another error.
const ERROR_FIELD: &str = "error";

/// The name of the field that holds what a library or the system said.
const REASON_FIELD: &str = "reason";

/// Every error type of the workspace with a field against the rule.
pub fn violations(workspace: &Workspace) -> Result<Vec<ErrorFieldViolation>, SourceError> {
    let crates: Vec<Vec<SourceFile>> = workspace
        .members
        .iter()
        .map(|member| source::of(&workspace.root, member))
        .collect::<Result<_, _>>()?;
    Ok(in_crates(&crates))
}

/// The violations in the source of several crates, one list of files per crate.
fn in_crates(crates: &[Vec<SourceFile>]) -> Vec<ErrorFieldViolation> {
    let error_types: Vec<BTreeSet<String>> = crates.iter().map(|files| error_types::names_in(files)).collect();
    let known: BTreeSet<&str> = error_types.iter().flatten().map(String::as_str).collect();
    let mut violations = Vec::new();
    for (files, own) in crates.iter().zip(&error_types) {
        for file in files {
            let names = Names::of(file);
            let error_types = file.items().into_iter().filter_map(|item| ErrorType::of(item, own));
            for error_type in error_types {
                let found = error_type
                    .holders
                    .iter()
                    .filter_map(|holder| in_holder(holder, file, &names, &known));
                violations.extend(found);
            }
        }
    }
    violations
}

/// The violation in the fields of one error struct, or of one variant of an error enum.
fn in_holder(
    holder: &Holder<'_>,
    file: &SourceFile,
    names: &Names,
    known: &BTreeSet<&str>,
) -> Option<ErrorFieldViolation> {
    let (path, name) = (file.path.clone(), holder.name.clone());
    match holder.fields {
        Fields::Unnamed(_) => Some(ErrorFieldViolation::WithoutName {
            path,
            line: holder.line,
            holder: name,
        }),
        Fields::Named(fields) => {
            let (field, error) = fields
                .named
                .iter()
                .filter_map(|field| Some((field.ident.as_ref()?, field)))
                .find(|(field, _)| *field == ERROR_FIELD)?;
            let held = Held::of(&error.ty);
            let of_workspace = held
                .as_ref()
                .is_some_and(|held| held.is_error(known) && names.origin(held) != Origin::Outside);
            (!of_workspace).then(|| ErrorFieldViolation::NotOfWorkspace {
                path,
                line: field.span().start().line,
                holder: name,
                held: held.map(|held| held.written),
            })
        }
        Fields::Unit => None,
    }
}

/// A field of an error type that is against the rule. `holder` is `ClientError::Refused`.
#[derive(Debug, PartialEq)]
pub enum ErrorFieldViolation {
    /// The fields are those of a tuple.
    WithoutName { path: PathBuf, line: usize, holder: String },
    /// The field `error` holds something else than an error of the workspace.
    /// `held` is `std::io::Error`, and nothing for a type that is no path: a tuple, a slice.
    NotOfWorkspace {
        path: PathBuf,
        line: usize,
        holder: String,
        held: Option<String>,
    },
}

impl fmt::Display for ErrorFieldViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutName { path, line, holder } => write!(
                f,
                "{}:{line}: `{holder}` has fields without a name; every field of an error has a name",
                path.display()
            ),
            Self::NotOfWorkspace {
                path,
                line,
                holder,
                held,
            } => {
                write!(
                    f,
                    "{}:{line}: the field `{ERROR_FIELD}` of `{holder}` holds ",
                    path.display()
                )?;
                match held {
                    Some(held) => write!(f, "a `{held}`, which is no error of the workspace")?,
                    None => f.write_str("no error of the workspace")?,
                }
                write!(
                    f,
                    "; what a library or the system said is text, in a field `{REASON_FIELD}`"
                )
            }
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
        in_crates(&[vec![file("gaslightmq_kafka_client_api", text)]])
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn fields_with_a_name_pass() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        ConnectionLost { reason: String },
                        UnsupportedVersion { api_key: i16, version: i16 },
                        NoAnswer,
                    }
                    #[derive(Debug, thiserror::Error)]
                    pub struct AddressError { input: String }
                    #[derive(Debug, thiserror::Error)]
                    pub struct Closed;";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_field_of_a_tuple_fails_whatever_it_holds() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        Protocol(String),
                        Refused(ResponseError, u32),
                    }
                    pub struct AddressError(String);
                    impl std::error::Error for AddressError {}";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_api/src/error.rs:3: \
                 `ClientError::Protocol` has fields without a name; every field of an error has a name",
                "crates/gaslightmq_kafka_client_api/src/error.rs:4: \
                 `ClientError::Refused` has fields without a name; every field of an error has a name",
                "crates/gaslightmq_kafka_client_api/src/error.rs:6: \
                 `AddressError` has fields without a name; every field of an error has a name",
            ]
        );
    }

    #[test]
    fn an_error_of_the_own_crate_in_the_field_error_passes() {
        let text = "use crate::response::ResponseError;
                    #[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        Refused { error: ResponseError },
                        OutOfTime { error: Box<ClientError>, attempts: u32 },
                        Unanswered { error: Option<&'static crate::call::CallError> },
                    }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_of_another_crate_of_the_workspace_passes() {
        let text = "use gaslightmq_kernel::TopicNameError;
                    use gaslightmq_wire::{self as wire, ResponseError};
                    #[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        InvalidTopic { error: TopicNameError },
                        Refused { error: ResponseError },
                        Unreadable { error: wire::ReadError },
                        Unwritable { error: gaslightmq_wire::WriteError },
                    }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_type_without_the_word_error_in_its_name_passes() {
        let pipeline = file(
            "gaslightmq_pipeline",
            "#[derive(Debug, thiserror::Error)]
             pub enum UntoldLie { NoLie }",
        );
        let engine = file(
            "gaslightmq_engine",
            "use gaslightmq_pipeline::UntoldLie;
             #[derive(Debug, thiserror::Error)]
             pub enum EngineError { Unmerged { error: UntoldLie } }",
        );

        assert_eq!(in_crates(&[vec![pipeline], vec![engine]]), []);
    }

    #[test]
    fn an_error_that_a_macro_writes_in_the_same_file_passes() {
        let text = "text_value! { TopicName, TopicNameError }
                    #[derive(Debug, thiserror::Error)]
                    pub enum TopicPartitionError { InvalidTopic { input: String, error: TopicNameError } }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_of_a_library_or_of_the_system_in_the_field_error_fails() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        ConnectionLost { error: io::Error },
                        Unreadable { path: PathBuf, error: Box<serde_json::Error> },
                        Unsendable { error: Box<dyn std::error::Error + Send + Sync> },
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_api/src/error.rs:3: \
                 the field `error` of `ClientError::ConnectionLost` holds a `io::Error`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:4: \
                 the field `error` of `ClientError::Unreadable` holds a `serde_json::Error`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:5: \
                 the field `error` of `ClientError::Unsendable` holds a `dyn Error`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
            ]
        );
    }

    #[test]
    fn an_error_that_the_file_takes_from_a_library_fails_under_any_name() {
        let text = "use std::io;
                    use kafka_protocol::error::ResponseError;
                    use std::num::ParseIntError as NumberError;
                    #[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        ConnectionLost { error: io::Error },
                        Refused { error: ResponseError },
                        InvalidPort { error: NumberError },
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_api/src/error.rs:6: \
                 the field `error` of `ClientError::ConnectionLost` holds a `io::Error`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:7: \
                 the field `error` of `ClientError::Refused` holds a `ResponseError`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:8: \
                 the field `error` of `ClientError::InvalidPort` holds a `NumberError`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
            ]
        );
    }

    #[test]
    fn what_is_no_error_in_the_field_error_fails() {
        let text = "pub struct ReadError<E> { error: E }
                    impl<E> std::error::Error for ReadError<E> {}
                    #[derive(Debug, thiserror::Error)]
                    pub enum ClientError {
                        Unreadable { error: String },
                        Refused { error: (i16, String) },
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_kafka_client_api/src/error.rs:1: \
                 the field `error` of `ReadError` holds a `E`, which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:5: \
                 the field `error` of `ClientError::Unreadable` holds a `String`, \
                 which is no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
                "crates/gaslightmq_kafka_client_api/src/error.rs:6: \
                 the field `error` of `ClientError::Refused` holds no error of the workspace; \
                 what a library or the system said is text, in a field `reason`",
            ]
        );
    }

    #[test]
    fn a_type_that_is_no_error_type_has_the_fields_it_likes() {
        let text = "pub struct Attempt(u32, io::Error);
                    pub enum Outcome { Failed { error: io::Error }, Refused(String) }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_type_in_test_code_passes() {
        let text = "#[cfg(test)]
                    mod tests {
                        #[derive(Debug, thiserror::Error)]
                        enum TestError { Failed(io::Error), Unreadable { error: io::Error } }
                    }";

        assert_eq!(lines(text), [""; 0]);
    }
}
