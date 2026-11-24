//! The check that nothing names `dyn Error`.
//!
//! `crates.md` says that nothing names `dyn Error`: no signature, no field, no doc example and no test.
//! A function returns one error type, by its name, and so does a doc example.
//!
//! The check reads every `.rs` file of a crate: its source with its test code,
//! and its `build.rs`, `tests/`, `benches/` and `examples/`. See [`source::texts_of`].
//! It reads the code as words and signs, so it reads the call and the definition of a macro too.
//! In a doc comment it reads the examples, between two lines that start with three backticks or three tildes.
//!
//! A `dyn` fails when one of the traits after it has `Error` as the last name of its path:
//! `dyn Error`, `dyn std::error::Error + Send + Sync` and `dyn Send + core::error::Error + 'static`.
//!
//! The text of a doc comment outside an example passes, and so does a comment that is no doc comment and a string.
//!
//! What the check does not catch:
//!
//! - The trait under another name: `use std::error::Error as Failure`.
//! - A type that hides the trait object: a type alias of a library, or `anyhow::Error`.
//! - A trait after one with type arguments: `dyn Iterator<Item = u8> + Error`.
//!
//! What the check refuses although it is no code:
//!
//! - A comment or a string inside a doc example that names `dyn Error`.

use std::fmt;
use std::path::{Path, PathBuf};

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use syn::Lit;

use crate::source::{self, SourceError};
use crate::workspace::Workspace;

/// The word that makes a trait a type.
const DYN: &str = "dyn";

/// The trait that no `dyn` names, by the last name of its path.
const ERROR_TRAIT: &str = "Error";

/// The attribute that a doc comment is.
const DOC: &str = "doc";

/// What a line of a doc comment starts with, when it opens or closes an example.
const FENCES: [&str; 2] = ["```", "~~~"];

/// Every place of the workspace that names `dyn Error`.
pub fn violations(workspace: &Workspace) -> Result<Vec<DynError>, SourceError> {
    let mut violations = Vec::new();
    for member in &workspace.members {
        for (path, text) in source::texts_of(&workspace.root, member)? {
            violations.extend(in_text(&path, &text)?);
        }
    }
    Ok(violations)
}

/// The violations in the text of one file.
fn in_text(path: &Path, text: &str) -> Result<Vec<DynError>, SourceError> {
    let tokens: TokenStream = text
        .parse()
        .map_err(|error: proc_macro2::LexError| SourceError::NotRust {
            path: path.to_path_buf(),
            line: error.span().start().line,
            reason: error.to_string(),
        })?;
    let mut file = File::default();
    file.read(tokens);
    let mut lines = named(&file.code);
    lines.extend(named(&file.examples));
    lines.sort_unstable();
    Ok(lines
        .into_iter()
        .map(|line| DynError {
            path: path.to_path_buf(),
            line,
        })
        .collect())
}

/// A word or a sign of the code, or of an example in a doc comment, with its line.
struct Piece {
    line: usize,
    kind: Kind,
}

enum Kind {
    /// A name or a keyword: `dyn`, `std`, `Error`.
    Word(String),
    /// Anything else, one character each: `:`, `+`, `<`. A literal is a `"`.
    Sign(char),
}

/// The pieces of one file.
#[derive(Default)]
struct File {
    code: Vec<Piece>,
    /// The examples of every doc comment, with a sign between two of them.
    examples: Vec<Piece>,
    /// Whether the doc comment that is being read is inside an example.
    in_example: bool,
}

impl File {
    /// Reads code: what is no doc comment becomes pieces of code, and a doc comment is read for its examples.
    fn read(&mut self, tokens: TokenStream) {
        for tree in tokens {
            let line = tree.span().start().line;
            match tree {
                TokenTree::Group(group) => {
                    if let Some(doc) = Doc::of(&group) {
                        self.read_doc(&doc);
                        continue;
                    }
                    self.in_example = false;
                    let (open, close) = match group.delimiter() {
                        Delimiter::Parenthesis => ('(', ')'),
                        Delimiter::Brace => ('{', '}'),
                        Delimiter::Bracket => ('[', ']'),
                        Delimiter::None => (' ', ' '),
                    };
                    self.code.push(Piece {
                        line,
                        kind: Kind::Sign(open),
                    });
                    self.read(group.stream());
                    self.code.push(Piece {
                        line: group.span_close().start().line,
                        kind: Kind::Sign(close),
                    });
                }
                TokenTree::Ident(word) => {
                    self.in_example = false;
                    self.code.push(Piece {
                        line,
                        kind: Kind::Word(word.to_string()),
                    });
                }
                // The `#` and the `!` before a doc comment are signs, and they end no example.
                TokenTree::Punct(sign) => self.code.push(Piece {
                    line,
                    kind: Kind::Sign(sign.as_char()),
                }),
                TokenTree::Literal(_) => {
                    self.in_example = false;
                    self.code.push(Piece {
                        line,
                        kind: Kind::Sign('"'),
                    });
                }
            }
        }
    }

    /// Reads the text of one doc comment: the lines inside an example become pieces.
    /// An example goes on in the doc comment on the next line, and ends with the item that the comments describe.
    fn read_doc(&mut self, doc: &Doc) {
        for (below, written) in doc.text.lines().enumerate() {
            // A `#[doc = "..."]` on one line of the file may hold several lines of text.
            let line = (doc.first_line + below).min(doc.last_line);
            if FENCES.iter().any(|fence| written.trim_start().starts_with(fence)) {
                self.in_example = !self.in_example;
                // No trait of one example goes on in the next one.
                self.examples.push(Piece {
                    line,
                    kind: Kind::Sign('`'),
                });
            } else if self.in_example {
                read_example(written, line, &mut self.examples);
            }
        }
    }
}

/// One doc comment, which is a `#[doc = "..."]` attribute to the code that reads it.
struct Doc {
    text: String,
    /// The lines of the file on which the text starts and ends.
    first_line: usize,
    last_line: usize,
}

impl Doc {
    /// The doc comment that a group in square brackets is: `[doc = "..."]`.
    fn of(group: &proc_macro2::Group) -> Option<Doc> {
        if group.delimiter() != Delimiter::Bracket {
            return None;
        }
        let trees: Vec<TokenTree> = group.stream().into_iter().collect();
        let [
            TokenTree::Ident(name),
            TokenTree::Punct(equals),
            TokenTree::Literal(text),
        ] = trees.as_slice()
        else {
            return None;
        };
        let Lit::Str(written) = Lit::new(text.clone()) else {
            return None;
        };
        (name == DOC && equals.as_char() == '=').then(|| Doc {
            text: written.value(),
            first_line: text.span().start().line,
            last_line: text.span().end().line,
        })
    }
}

/// Reads one line of an example as words and signs. An example need not be whole Rust, so no parser reads it.
fn read_example(written: &str, line: usize, pieces: &mut Vec<Piece>) {
    let mut word = String::new();
    for character in written.chars().chain([' ']) {
        if character.is_alphanumeric() || character == '_' {
            word.push(character);
            continue;
        }
        if !word.is_empty() {
            pieces.push(Piece {
                line,
                kind: Kind::Word(std::mem::take(&mut word)),
            });
        }
        if !character.is_whitespace() {
            pieces.push(Piece {
                line,
                kind: Kind::Sign(character),
            });
        }
    }
}

/// The line of every `dyn` that names `Error`.
fn named(pieces: &[Piece]) -> Vec<usize> {
    pieces
        .iter()
        .enumerate()
        .filter(|(_, piece)| matches!(&piece.kind, Kind::Word(word) if word == DYN))
        .filter(|(at, _)| names_error(&pieces[at + 1..]))
        .map(|(_, piece)| piece.line)
        .collect()
}

/// Whether the traits after a `dyn` hold `Error`: paths and lifetimes with a `+` between them.
fn names_error(after_dyn: &[Piece]) -> bool {
    // The last name of the path that is being read.
    let mut last: Option<&str> = None;
    let mut in_path = false;
    let mut in_lifetime = false;
    for piece in after_dyn {
        match &piece.kind {
            Kind::Word(_) if in_lifetime => in_lifetime = false,
            // A word after a whole path is no trait any more: `&dyn Error as _`.
            Kind::Word(_) if last.is_some() && !in_path => break,
            Kind::Word(word) => {
                last = Some(word);
                in_path = false;
            }
            Kind::Sign(':') => in_path = true,
            Kind::Sign('\'') => in_lifetime = true,
            Kind::Sign('?') => {}
            Kind::Sign('+') => {
                if last == Some(ERROR_TRAIT) {
                    return true;
                }
                last = None;
            }
            Kind::Sign(_) => break,
        }
    }
    last == Some(ERROR_TRAIT)
}

/// A place that names `dyn Error`.
#[derive(Debug, PartialEq)]
pub struct DynError {
    path: PathBuf,
    line: usize,
}

impl fmt::Display for DynError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: names `{DYN} {ERROR_TRAIT}`; an error is one error type, by its name",
            self.path.display(),
            self.line
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The violations in one file, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        in_text(Path::new("crates/gaslightmq_wire/tests/frames.rs"), text)
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// The lines of one file that name `dyn Error`.
    fn found(text: &str) -> Vec<usize> {
        in_text(Path::new("crates/gaslightmq_wire/src/lib.rs"), text)
            .unwrap()
            .iter()
            .map(|violation| violation.line)
            .collect()
    }

    #[test]
    fn code_that_names_one_error_type_passes() {
        let text = "pub fn read(frame: &[u8]) -> Result<Frame, ReadError> { todo!() }
                    pub struct Failure { error: Box<ReadError>, reason: String }
                    pub fn each(visit: &mut dyn FnMut(&Frame) -> Result<(), ReadError>) {}
                    pub type Frames = Box<dyn Iterator<Item = Result<Frame, ReadError>> + Send>;
                    impl std::error::Error for ReadError {}
                    pub fn show(error: &impl std::error::Error) {}";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_signature_that_names_dyn_error_fails() {
        let text = "pub fn read(frame: &[u8]) -> Result<Frame, Box<dyn std::error::Error>> { todo!() }";

        assert_eq!(
            lines(text),
            ["crates/gaslightmq_wire/tests/frames.rs:1: \
              names `dyn Error`; an error is one error type, by its name"]
        );
    }

    #[test]
    fn dyn_error_fails_however_it_is_written() {
        let text = "type A = Box<dyn Error>;
                    type B = Box<dyn core::error::Error + Send + Sync + 'static>;
                    type C = Arc<dyn Send + Sync + std::error::Error>;
                    type D = &'static (dyn 'static + ::std::error::Error);
                    fn e(error: &dyn Error) {}
                    fn f() { let error = &failure as &dyn Error as *const _; }
                    type G = Box<
                        dyn std::error::Error
                            + Send,
                    >;";

        assert_eq!(found(text), [1, 2, 3, 4, 5, 6, 8]);
    }

    #[test]
    fn dyn_error_fails_in_a_field_in_a_body_in_a_macro_and_in_test_code() {
        let text = "pub struct Failure { error: Box<dyn Error> }
                    fn run() { let failures: Vec<Box<dyn Error>> = Vec::new(); }
                    macro_rules! failure { ($name:ident) => { type $name = Box<dyn Error>; }; }
                    failure!(Box<dyn Error>);
                    #[cfg(test)]
                    mod tests {
                        #[test]
                        fn reads() -> Result<(), Box<dyn Error>> { Ok(()) }
                    }";

        assert_eq!(found(text), [1, 2, 3, 4, 8]);
    }

    #[test]
    fn another_trait_object_passes() {
        let text = "type A = Box<dyn Lie>;
                    type B = Box<dyn ErrorHandler + Send>;
                    type C = Box<dyn error::Report>;
                    type D = Box<dyn Fn(Error) -> Error>;
                    fn e(error: &dyn Display, other: Error) {}";

        assert_eq!(found(text), [0; 0]);
    }

    #[test]
    fn a_doc_example_that_names_dyn_error_fails() {
        let text = "//! Reads frames.
                    //!
                    //! ```
                    //! # fn main() -> Result<(), Box<dyn std::error::Error>> {
                    //! let frame = read(&bytes)?;
                    //! # Ok(())
                    //! # }
                    //! ```

                    /// Reads one frame.
                    ///
                    /// ~~~no_run
                    /// let error: Box<dyn Error
                    ///     + Send> = read(&bytes).unwrap_err().into();
                    /// ~~~
                    pub fn read(frame: &[u8]) -> Result<Frame, ReadError> { todo!() }

                    pub struct Frame {
                        /** The length.

                        ```
                        fn length() -> Result<usize, Box<dyn Error>> { Ok(0) }
                        ```
                        */
                        length: usize,
                    }

                    #[doc = \"```\\nlet error: &dyn Error = &error;\\n```\"]
                    pub struct ReadError;";

        assert_eq!(found(text), [4, 13, 22, 28]);
    }

    #[test]
    fn a_doc_example_in_a_macro_fails() {
        let text = "name! {
                        /// ```
                        /// # fn main() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
                        /// ```
                        pub struct TopicName;
                    }";

        assert_eq!(found(text), [3]);
    }

    #[test]
    fn a_doc_example_that_names_one_error_type_passes() {
        let text = "/// ```
                    /// # fn main() -> Result<(), ReadError> {
                    /// let frame = read(&bytes)?;
                    /// # Ok(())
                    /// # }
                    /// ```
                    pub fn read(frame: &[u8]) -> Result<Frame, ReadError> { todo!() }";

        assert_eq!(found(text), [0; 0]);
    }

    #[test]
    fn the_text_of_a_doc_comment_a_comment_and_a_string_pass() {
        let text = "/// Returns a `ReadError`, and no `Box<dyn Error>`.
                    // A `Box<dyn Error>` would hide what went wrong.
                    pub fn read(frame: &[u8]) -> Result<Frame, ReadError> {
                        let message = \"no Box<dyn Error> here\";
                        todo!()
                    }";

        assert_eq!(found(text), [0; 0]);
    }

    #[test]
    fn an_example_ends_with_the_item_that_its_doc_comment_describes() {
        let text = "/// ```
                    /// let frame = read(&bytes);
                    pub struct Frame;

                    /// Returns no `dyn Error`.
                    pub struct ReadError;";

        assert_eq!(found(text), [0; 0]);
    }

    #[test]
    fn text_that_is_not_rust_fails_with_its_line() {
        let error = in_text(Path::new("crates/gaslightmq_wire/src/lib.rs"), "struct A;\nstruct B {")
            .err()
            .unwrap();

        assert!(matches!(error, SourceError::NotRust { line: 2, .. }), "{error}");
    }
}
