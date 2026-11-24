//! The check that no error type is `#[non_exhaustive]`.
//!
//! `crates.md` says that a new variant is a new decision for every caller:
//! without the attribute, a new variant breaks every `match` that decides on the error, and someone decides.
//!
//! [`error_types`] says what an error type is.
//! The attribute fails on an error struct, on an error enum and on a variant of one.
//!
//! What the check does not catch:
//!
//! - An attribute that a `#[cfg_attr(...)]` adds.
//! - What no check of the error rules catches. See [`error_types`].
//!
//! [`error_types`]: crate::error_types

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use syn::Attribute;

use crate::error_types::{self, ErrorType};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::Workspace;

/// The attribute that lets a crate add a variant or a field without breaking its callers.
const NON_EXHAUSTIVE: &str = "non_exhaustive";

/// Every error type of the workspace that is `#[non_exhaustive]`.
pub fn violations(workspace: &Workspace) -> Result<Vec<NonExhaustiveError>, SourceError> {
    let mut violations = Vec::new();
    for member in &workspace.members {
        violations.extend(in_files(&source::of(&workspace.root, member)?));
    }
    Ok(violations)
}

/// The violations in the source of one crate.
fn in_files(files: &[SourceFile]) -> Vec<NonExhaustiveError> {
    let own: BTreeSet<String> = error_types::names_in(files);
    let mut violations = Vec::new();
    for file in files {
        for error_type in file.items().into_iter().filter_map(|item| ErrorType::of(item, &own)) {
            let on_type = non_exhaustive(error_type.attributes).map(|line| (line, error_type.name.clone()));
            let on_variants = error_type
                .holders
                .iter()
                .filter_map(|holder| Some((non_exhaustive(holder.attributes)?, holder.name.clone())));
            for (line, holder) in on_type.into_iter().chain(on_variants) {
                violations.push(NonExhaustiveError {
                    path: file.path.clone(),
                    line,
                    holder,
                });
            }
        }
    }
    violations
}

/// The line of a `#[non_exhaustive]` among attributes.
fn non_exhaustive(attributes: &[Attribute]) -> Option<usize> {
    attributes
        .iter()
        .find_map(|attribute| attribute.path().get_ident().filter(|name| *name == NON_EXHAUSTIVE))
        .map(|name| name.span().start().line)
}

/// An error type, or a variant of one, that is `#[non_exhaustive]`. `holder` is `ClientError::Refused`.
#[derive(Debug, PartialEq)]
pub struct NonExhaustiveError {
    path: PathBuf,
    line: usize,
    holder: String,
}

impl fmt::Display for NonExhaustiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: `{}` is `#[{NON_EXHAUSTIVE}]`; a new variant or field of an error breaks the code that decides on it",
            self.path.display(),
            self.line,
            self.holder
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The violations in one file of one crate, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        let file = SourceFile::parse(Path::new("crates/gaslightmq_wire"), Path::new("error.rs"), text).unwrap();
        in_files(&[file]).iter().map(ToString::to_string).collect()
    }

    #[test]
    fn an_error_type_without_the_attribute_passes() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ReadError { Truncated { length: usize }, Empty }
                    #[derive(Debug, thiserror::Error)]
                    pub struct WriteError { reason: String }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_non_exhaustive_error_enum_fails() {
        let text = "#[derive(Debug, thiserror::Error)]
                    #[non_exhaustive]
                    pub enum ReadError { Truncated { length: usize }, Empty }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_wire/src/error.rs:2: `ReadError` is `#[non_exhaustive]`; \
              a new variant or field of an error breaks the code that decides on it"
            ]
        );
    }

    #[test]
    fn a_non_exhaustive_variant_fails() {
        let text = "#[derive(Debug, thiserror::Error)]
                    pub enum ReadError {
                        #[non_exhaustive]
                        Truncated { length: usize },
                        Empty,
                    }";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_wire/src/error.rs:3: `ReadError::Truncated` is `#[non_exhaustive]`; \
              a new variant or field of an error breaks the code that decides on it"
            ]
        );
    }

    #[test]
    fn a_non_exhaustive_error_struct_fails() {
        let text = "#[non_exhaustive]
                    pub struct WriteError { reason: String }
                    impl std::error::Error for WriteError {}";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_wire/src/error.rs:1: `WriteError` is `#[non_exhaustive]`; \
              a new variant or field of an error breaks the code that decides on it"
            ]
        );
    }

    #[test]
    fn a_non_exhaustive_type_that_is_no_error_type_passes() {
        let text = "#[non_exhaustive]
                    pub struct Configuration { pub request_timeout: Duration }
                    #[derive(Debug)]
                    #[non_exhaustive]
                    pub enum Event { Opened }";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn an_error_type_in_test_code_passes() {
        let text = "#[cfg(test)]
                    mod tests {
                        #[derive(Debug, thiserror::Error)]
                        #[non_exhaustive]
                        enum TestError { Failed }
                    }";

        assert_eq!(lines(text), [""; 0]);
    }
}
