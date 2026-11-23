//! The check that the message of an error writes the `error` and the `reason` it holds, each once.
//!
//! `crates.md` says that an error says everything in one message:
//! a variant that holds another error writes that error once, and a variant that holds a `reason` writes it once.
//! A message that leaves it out hides the cause, and a message that writes it twice says it twice.
//!
//! The check reads the message that a derive writes, from an `#[error("...")]` attribute:
//! that of an error struct, that of a variant, or that of the enum for a variant without one.
//! [`error_types`] says what an error type is.
//!
//! A message writes a field in its text, as `{error}` or `{reason:?}`,
//! or in an argument after the text, as `.error` or `.reason.to_lowercase()`.
//! The check counts both, for each field named `error` or `reason`, and the count is one.
//!
//! What the check does not catch:
//!
//! - A message that is not derived: a `Display` that the crate writes, or `#[error(fmt = ...)]`.
//! - Whether the message names its own value first, before the error it holds.
//! - A field with another name that holds an error or a reason. The `wrapped-errors` check finds the error.
//! - What no check of the error rules catches. See [`error_types`].
//!
//! What the check counts although it is no field of the error:
//!
//! - A field of something else with the same name, in an argument: `.call.reason`.
//!   The check counts a name, and leaves out only the name of a method, which stands before a `(`.
//!
//! [`error_types`]: crate::error_types

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use syn::parse::{Parse, ParseStream};
use syn::{Attribute, LitStr};

use crate::error_types::{self, ErrorType};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The fields that a message writes once: the error it holds, and what a library or the system said.
const WRITTEN_ONCE: [&str; 2] = ["error", "reason"];

/// The attribute that holds the message of a derived `Error`.
const MESSAGE_ATTRIBUTE: &str = "error";

/// Every message of an error type of the workspace that leaves out what it holds, or writes it twice.
pub fn violations(workspace: &Workspace) -> Result<Vec<ErrorMessageViolation>, SourceError> {
    let mut violations = Vec::new();
    for member in &workspace.members {
        violations.extend(in_files(&source::of(&workspace.root, member)?));
    }
    Ok(violations)
}

/// The violations in the source of one crate.
fn in_files(files: &[SourceFile]) -> Vec<ErrorMessageViolation> {
    let own: BTreeSet<String> = error_types::names_in(files);
    let mut violations = Vec::new();
    for file in files {
        for error_type in file.items().into_iter().filter_map(|item| ErrorType::of(item, &own)) {
            if !error_type.derived {
                continue;
            }
            for holder in &error_type.holders {
                let Some(message) = Message::among(holder.attributes).or_else(|| Message::among(error_type.attributes))
                else {
                    continue;
                };
                let fields = holder.fields.iter().filter_map(|field| field.ident.as_ref());
                for field in fields.filter(|field| WRITTEN_ONCE.iter().any(|written_once| field == written_once)) {
                    let times = message.times_written(&field.to_string());
                    if times != 1 {
                        violations.push(ErrorMessageViolation {
                            path: file.path.clone(),
                            line: message.text.span().start().line,
                            holder: holder.name.clone(),
                            field: field.to_string(),
                            times,
                        });
                    }
                }
            }
        }
    }
    violations
}

/// The message of a derived `Error`: `#[error("{} cannot be read: {reason}", .path.display())]`.
struct Message {
    text: LitStr,
    /// What follows the text, without the comma: `.path.display()`.
    arguments: TokenStream,
}

impl Parse for Message {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Message {
            text: input.parse()?,
            arguments: input.parse()?,
        })
    }
}

impl Message {
    /// The message among the attributes of an error struct, of an error enum or of a variant.
    /// An attribute that holds no text is no message: `#[error(transparent)]`.
    fn among(attributes: &[Attribute]) -> Option<Message> {
        attributes
            .iter()
            .filter(|attribute| attribute.path().is_ident(MESSAGE_ATTRIBUTE))
            .find_map(|attribute| attribute.parse_args::<Message>().ok())
    }

    /// How often the message writes a field: in its text and in its arguments.
    fn times_written(&self, field: &str) -> usize {
        in_text(&self.text.value(), field) + in_arguments(self.arguments.clone(), field)
    }
}

/// How often the text of a message writes a field: `{reason}` and `{reason:?}` each write `reason` once.
fn in_text(text: &str, field: &str) -> usize {
    let mut times = 0;
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        rest = &rest[open + 1..];
        // `{{` writes a brace, and opens nothing.
        if let Some(after_brace) = rest.strip_prefix('{') {
            rest = after_brace;
            continue;
        }
        let close = rest.find('}').unwrap_or(rest.len());
        let written = rest[..close].split(':').next().unwrap_or_default();
        if written.trim() == field {
            times += 1;
        }
        rest = &rest[close..];
    }
    times
}

/// How often the arguments of a message name a field: `.reason` and `.reason.len()` each name `reason` once.
/// The name of a method is no field: `.call.reason()`.
fn in_arguments(arguments: TokenStream, field: &str) -> usize {
    let trees: Vec<TokenTree> = arguments.into_iter().collect();
    let mut times = 0;
    for (at, tree) in trees.iter().enumerate() {
        match tree {
            TokenTree::Group(group) => times += in_arguments(group.stream(), field),
            TokenTree::Ident(name) if name == field => {
                let is_method = matches!(
                    trees.get(at + 1),
                    Some(TokenTree::Group(call)) if call.delimiter() == Delimiter::Parenthesis
                );
                if !is_method {
                    times += 1;
                }
            }
            _ => {}
        }
    }
    times
}

/// A message that does not write a field once. `holder` is `ClientError::Refused`, and `field` is `error` or `reason`.
#[derive(Debug, PartialEq)]
pub struct ErrorMessageViolation {
    path: PathBuf,
    line: usize,
    holder: String,
    field: String,
    times: usize,
}

impl fmt::Display for ErrorMessageViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            path,
            line,
            holder,
            field,
            times,
        } = self;
        write!(f, "{}:{line}: the message of `{holder}` ", path.display())?;
        match times {
            0 => write!(f, "does not write its `{field}`")?,
            _ => write!(f, "writes its `{field}` {times} times")?,
        }
        f.write_str("; a message writes what it holds once")
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The violations in one file of one crate, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        let file = SourceFile::parse(Path::new("crates/gaslightmq_listener"), Path::new("error.rs"), text).unwrap();
        in_files(&[file]).iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_message_that_writes_what_it_holds_once_passes() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      pub enum ListenError {
                          #[error("{address} cannot be opened: {reason}")]
                          NotOpened { address: SocketAddr, reason: String },
                          #[error("topic `{input}`: {error}")]
                          InvalidTopic { input: String, error: TopicNameError },
                          #[error("{} is refused: {error:?}, because {reason:>8}", .address.ip())]
                          Refused { address: SocketAddr, error: ResponseError, reason: String },
                          #[error("the listener is closed")]
                          Closed,
                      }
                      #[derive(Debug, thiserror::Error)]
                      #[error("{} cannot be read: {reason}", .path.display())]
                      pub struct ReadError { path: PathBuf, reason: String }"#;

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_message_that_leaves_out_what_it_holds_fails() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      pub enum ListenError {
                          #[error("{address} cannot be opened")]
                          NotOpened { address: SocketAddr, reason: String },
                          #[error("topic `{input}` is no topic")]
                          InvalidTopic { input: String, error: TopicNameError },
                      }
                      #[derive(Debug, thiserror::Error)]
                      #[error("{{reason}} cannot be read")]
                      pub struct ReadError { reason: String }"#;

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_listener/src/error.rs:3: \
                 the message of `ListenError::NotOpened` does not write its `reason`; \
                 a message writes what it holds once",
                "crates/gaslightmq_listener/src/error.rs:5: \
                 the message of `ListenError::InvalidTopic` does not write its `error`; \
                 a message writes what it holds once",
                "crates/gaslightmq_listener/src/error.rs:9: \
                 the message of `ReadError` does not write its `reason`; \
                 a message writes what it holds once",
            ]
        );
    }

    #[test]
    fn a_message_that_writes_what_it_holds_twice_fails() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      pub enum ListenError {
                          #[error("{reason}: {address} cannot be opened: {reason}")]
                          NotOpened { address: SocketAddr, reason: String },
                          #[error("topic `{input}`: {error} ({})", .error.code())]
                          InvalidTopic { input: String, error: TopicNameError },
                      }"#;

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_listener/src/error.rs:3: \
                 the message of `ListenError::NotOpened` writes its `reason` 2 times; \
                 a message writes what it holds once",
                "crates/gaslightmq_listener/src/error.rs:5: \
                 the message of `ListenError::InvalidTopic` writes its `error` 2 times; \
                 a message writes what it holds once",
            ]
        );
    }

    #[test]
    fn an_argument_after_the_text_writes_a_field() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      pub enum ListenError {
                          #[error("{address} cannot be opened: {}", .reason)]
                          NotOpened { address: SocketAddr, reason: String },
                          #[error("{address} is closed: {}", shortened(.reason.trim()))]
                          Closed { address: SocketAddr, reason: String },
                          #[error("{} is refused", .call.reason())]
                          Refused { call: Call, reason: String },
                      }"#;

        assert_eq!(
            lines(text),
            ["crates/gaslightmq_listener/src/error.rs:7: \
              the message of `ListenError::Refused` does not write its `reason`; \
              a message writes what it holds once"]
        );
    }

    #[test]
    fn a_variant_without_a_message_has_the_message_of_its_enum() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      #[error("the listener failed")]
                      pub enum ListenError {
                          NotOpened { reason: String },
                          #[error("the listener is closed: {reason}")]
                          Closed { reason: String },
                      }"#;

        assert_eq!(
            lines(text),
            ["crates/gaslightmq_listener/src/error.rs:2: \
              the message of `ListenError::NotOpened` does not write its `reason`; \
              a message writes what it holds once"]
        );
    }

    #[test]
    fn a_field_with_another_name_is_written_as_often_as_the_message_likes() {
        let text = r#"#[derive(Debug, thiserror::Error)]
                      pub enum ListenError {
                          #[error("{address} cannot be opened, and {address} stays closed")]
                          NotOpened { address: SocketAddr, attempts: u32 },
                      }"#;

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_message_that_no_derive_writes_passes() {
        let text = "pub enum ListenError { NotOpened { reason: String } }
                    impl std::error::Error for ListenError {}
                    #[derive(Debug, thiserror::Error)]
                    #[error(fmt = closed)]
                    pub struct Closed { reason: String }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_type_that_is_no_error_type_passes() {
        let text = r#"#[derive(Debug, Display)]
                      #[error("refused")]
                      pub struct Refusal { reason: String }"#;

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_type_in_test_code_passes() {
        let text = r#"#[cfg(test)]
                      mod tests {
                          #[derive(Debug, thiserror::Error)]
                          #[error("failed")]
                          struct TestError { reason: String }
                      }"#;

        assert_eq!(lines(text), [""; 0]);
    }
}
