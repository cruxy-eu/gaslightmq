//! The text of a definition: the YAML that a file of a workspace holds, read with the path of the key that failed.
//!
//! | Type | Is |
//! |---|---|
//! | [`DefinitionText`] | The text of one definition of a kind, as a file holds it, before anyone read it. |
//! | [`ReadDefinitionError`] | Why the text did not read as a definition: the path of the key that failed, and what is wrong there. |
//!
//! The files of a workspace and the code of a test write a definition in one language.
//! A file holds its YAML, and the code writes the same map with [`definition_text!`], which lets the values of the
//! code in: a Rust expression in parentheses stands in the map as its `serde` form.
//!
//! ```
//! use gaslightmq_definition_text::{DefinitionText, definition_text};
//!
//! #[derive(Debug, PartialEq, serde::Deserialize)]
//! struct Door {
//!     name: String,
//!     opens: Vec<String>,
//!     wide: u32,
//! }
//!
//! let width = 90;
//! let written: DefinitionText<Door> = definition_text! { name: front-door, opens: [in, out], wide: (width) };
//! assert_eq!(written.read()?, Door { name: "front-door".into(), opens: vec!["in".into(), "out".into()], wide: 90 });
//!
//! // The same definition, as a file holds it.
//! let pasted = DefinitionText::<Door>::new("name: front-door\nopens: [in, out]\nwide: 90\n");
//! assert_eq!(pasted.read()?, written.read()?);
//!
//! // What does not read names the key.
//! let error = DefinitionText::<Door>::new("name: back-door\nopens: in\nwide: 90\n").read().unwrap_err();
//! assert_eq!(error.path.as_deref(), Some("opens"));
//! # Ok::<(), gaslightmq_definition_text::ReadDefinitionError>(())
//! ```
//!
//! # The words of the macro
//!
//! [`definition_text!`] takes the entries of a map, `key: value`, with a comma between them.
//!
//! | A value | Is | Example |
//! |---|---|---|
//! | `{ … }` | A map of its own entries. | `selector: { api: Produce }` |
//! | `[ … ]` | A list of values, with a comma between them. | `arm: [slow-producers]` |
//! | `( … )` | A Rust expression: its `serde` form. | `first: (TRIES_REFUSED)` |
//! | A literal | The YAML scalar it reads as: a number, or a text in quotes. | `3`, `"orders.*"` |
//! | Words | A text: the tokens up to the next comma, without the spaces between them. | `Produce`, `750ms`, `orders.*`, `40%`, `client.id` |
//!
//! A key is words, or a text in quotes. `true`, `false` and `null` are what YAML reads them as.
//! A text with a space in it stands in quotes: `"until disarmed"`.
//! A constant of the code and a word look the same to the macro, so a constant stands in parentheses: `(TRIES_REFUSED)`.
//!
//! The macro checks nothing while the code compiles. It writes the text, and [`read`](DefinitionText::read) reads it
//! as a file is read.
//!
//! # No IO
//!
//! The crate reads and writes text, and nothing else. It compiles to WebAssembly.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// The text of one definition of the kind `D`, as a file of a workspace holds it, before anyone read it.
///
/// It is YAML: the block form of a file, or the flow form that [`definition_text!`] writes.
/// Nothing checks it until someone [reads](Self::read) it.
pub struct DefinitionText<D> {
    text: String,
    kind: PhantomData<fn() -> D>,
}

impl<D> DefinitionText<D> {
    /// The text of a definition: the YAML of its map.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            kind: PhantomData,
        }
    }

    /// The text, as it was written.
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

impl<D: DeserializeOwned> DefinitionText<D> {
    /// Reads the definition, as the files of a workspace are read.
    ///
    /// The text is YAML 1.2: `true` and `false` are the only booleans, so a name such as `on` stays a name.
    ///
    /// # Errors
    ///
    /// [`ReadDefinitionError`] with the path of the key that failed, when the reader knows it, and what is wrong there.
    pub fn read(&self) -> Result<D, ReadDefinitionError> {
        read(&self.text)
    }

    /// Reads the definition, with `name` as its name when its text names none.
    ///
    /// Whoever hands over a definition without a name, where its kind needs one, names it so.
    ///
    /// # Errors
    ///
    /// [`ReadDefinitionError`] as for [`read`](Self::read).
    pub fn read_named(&self, name: &str) -> Result<D, ReadDefinitionError> {
        let mut map: serde_json::Value = read(&self.text)?;
        if let serde_json::Value::Object(entries) = &mut map
            && !entries.contains_key("name")
        {
            entries.insert("name".to_owned(), serde_json::Value::String(name.to_owned()));
        }
        serde_path_to_error::deserialize(map).map_err(|error| ReadDefinitionError {
            path: known(error.path().to_string()),
            reason: error.into_inner().to_string(),
        })
    }

    /// Whether the text names its definition: whether its map has the key `name`.
    ///
    /// # Errors
    ///
    /// [`ReadDefinitionError`] when the text is no YAML.
    pub fn names_itself(&self) -> Result<bool, ReadDefinitionError> {
        let map: serde_json::Value = read(&self.text)?;
        Ok(map.get("name").is_some())
    }
}

impl<D: Serialize> DefinitionText<D> {
    /// The text that a file holds for `definition`.
    ///
    /// # Errors
    ///
    /// [`ReadDefinitionError`] without a path, with what the writer said, when it could not write the definition.
    pub fn of(definition: &D) -> Result<Self, ReadDefinitionError> {
        serde_saphyr::to_string(definition)
            .map(Self::new)
            .map_err(|error| ReadDefinitionError {
                path: None,
                reason: error.to_string(),
            })
    }
}

impl<D> Clone for DefinitionText<D> {
    fn clone(&self) -> Self {
        Self::new(self.text.clone())
    }
}

impl<D> fmt::Debug for DefinitionText<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DefinitionText").field(&self.text).finish()
    }
}

impl<D> PartialEq for DefinitionText<D> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl<D> Eq for DefinitionText<D> {}

impl<D> From<&str> for DefinitionText<D> {
    fn from(text: &str) -> Self {
        Self::new(text)
    }
}

impl<D> From<String> for DefinitionText<D> {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

/// Why a text did not read as a definition: the path of the key that failed, and what is wrong there.
///
/// ```
/// use gaslightmq_definition_text::ReadDefinitionError;
///
/// let error = ReadDefinitionError { path: Some("trigger.first".to_owned()), reason: "a count is at least one".to_owned() };
/// assert_eq!(error.to_string(), "trigger.first: a count is at least one");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}", located(.path.as_deref(), .reason))]
pub struct ReadDefinitionError {
    /// The path of the key that failed, such as `trigger.first`, or `None` when the text as a whole did not read.
    pub path: Option<String>,
    /// What the type of the value said, or what the reader of YAML said.
    pub reason: String,
}

/// The path, then what is wrong there.
fn located(path: Option<&str>, reason: &str) -> String {
    match path {
        Some(path) => format!("{path}: {reason}"),
        None => reason.to_owned(),
    }
}

/// A path that names a key, or `None` for the whole value.
fn known(path: String) -> Option<String> {
    (!matches!(path.as_str(), "." | "?")).then_some(path)
}

/// Reads `text` as the YAML of one value of `T`, as a file of a workspace is read.
///
/// # Errors
///
/// [`ReadDefinitionError`] with the path of the key that failed, and what the type of the value said there.
pub fn read<T: DeserializeOwned>(text: &str) -> Result<T, ReadDefinitionError> {
    // YAML 1.2 knows `true` and `false`, so a selector named `on` stays a name.
    let options = serde_saphyr::options! {
        strict_booleans: true,
        with_snippet: false,
    };
    let mut path = None;
    serde_saphyr::with_deserializer_from_str_with_options(text, options, |deserializer| {
        serde_path_to_error::deserialize(deserializer).map_err(|error| {
            path = known(error.path().to_string());
            error.into_inner()
        })
    })
    .map_err(|error| ReadDefinitionError {
        path,
        reason: match error {
            // What the type of the value said, without where the reader was: the path says that.
            serde_saphyr::Error::Message { msg, .. } => msg,
            other => other.to_string(),
        },
    })
}

/// What [`definition_text!`] writes: a map in the flow form of YAML, whose keys and texts stand in quotes.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Writer {
    text: String,
}

#[doc(hidden)]
impl Writer {
    pub fn open_map(&mut self) {
        self.text.push('{');
    }

    pub fn close_map(&mut self) {
        self.text.push('}');
    }

    pub fn open_list(&mut self) {
        self.text.push('[');
    }

    pub fn close_list(&mut self) {
        self.text.push(']');
    }

    pub fn next(&mut self) {
        self.text.push_str(", ");
    }

    /// A key: words, or a text in quotes.
    pub fn key(&mut self, written: &str) {
        let key = match unquoted(written) {
            Some(text) => text,
            None => written.to_owned(),
        };
        self.text.push_str(&quoted(&key));
        self.text.push_str(": ");
    }

    /// A value of words or a literal, as the tokens of the code wrote it.
    pub fn scalar(&mut self, written: &str) {
        let scalar = match unquoted(written) {
            Some(text) => quoted(&text),
            None if matches!(written, "true" | "false" | "null") => written.to_owned(),
            None if is_number(written) => written.replace('_', ""),
            None => quoted(written),
        };
        self.text.push_str(&scalar);
    }

    /// A value of the code, as its `serde` form: JSON, which the flow form of YAML reads as it stands.
    pub fn value<T: Serialize + ?Sized>(&mut self, value: &T) {
        match serde_json::to_string(value) {
            Ok(json) => self.text.push_str(&json),
            // The reader names the key, and says that no value stands there.
            Err(error) => self.text.push_str(&quoted(&format!("no value: {error}"))),
        }
    }

    pub fn into_text<D>(self) -> DefinitionText<D> {
        DefinitionText::new(self.text)
    }
}

/// The text of a string or a character literal, or `None` when the tokens are no such literal.
fn unquoted(written: &str) -> Option<String> {
    if let Some(raw) = written.strip_prefix('r') {
        let hashes = raw.len() - raw.trim_start_matches('#').len();
        let inner = raw.get(hashes..raw.len().checked_sub(hashes)?)?;
        return inner.strip_prefix('"')?.strip_suffix('"').map(str::to_owned);
    }
    let quote = written.chars().next().filter(|first| matches!(first, '"' | '\''))?;
    let inner = written.strip_prefix(quote)?.strip_suffix(quote)?;
    serde_json::from_str(&format!("\"{}\"", inner.replace("\\'", "'"))).ok()
}

/// Whether the tokens are a number: digits, with a sign, a point and an exponent where a number has them.
fn is_number(written: &str) -> bool {
    let digits = written.strip_prefix('-').unwrap_or(written).replace('_', "");
    !digits.is_empty() && digits.starts_with(|first: char| first.is_ascii_digit()) && digits.parse::<f64>().is_ok()
}

/// The text in double quotes, as JSON and YAML both read it.
fn quoted(text: &str) -> String {
    serde_json::Value::String(text.to_owned()).to_string()
}

/// The text of a definition, written as the entries of its map in the code: see the [crate](crate).
///
/// ```
/// use gaslightmq_definition_text::{DefinitionText, definition_text};
///
/// let written: DefinitionText<serde_json::Value> = definition_text! {
///     selector: { api: Produce, topic: orders.* },
///     scope: { client.id: analytics-etl },
///     trigger: { chance: 40% },
///     lifetime: "3 firings",
/// };
/// assert_eq!(
///     written.as_str(),
///     r#"{"selector": {"api": "Produce", "topic": "orders.*"}, "scope": {"client.id": "analytics-etl"}, "trigger": {"chance": "40%"}, "lifetime": "3 firings"}"#
/// );
/// ```
#[macro_export]
macro_rules! definition_text {
    ($($entries:tt)*) => {{
        let mut writer = $crate::Writer::default();
        writer.open_map();
        $crate::__definition_map!(writer; $($entries)*);
        writer.close_map();
        writer.into_text()
    }};
}

/// The entries of a map, up to the end of the tokens.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_map {
    ($writer:ident;) => {};
    ($writer:ident; $($rest:tt)+) => {
        $crate::__definition_key!($writer; []; $($rest)+);
    };
}

/// The tokens of a key, up to its `:`.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_key {
    ($writer:ident; [$($key:tt)+]; : $($rest:tt)*) => {
        $writer.key(::core::concat!($(::core::stringify!($key)),+));
        $crate::__definition_value!($writer; map; []; $($rest)*);
    };
    ($writer:ident; [$($key:tt)*]; $next:tt $($rest:tt)*) => {
        $crate::__definition_key!($writer; [$($key)* $next]; $($rest)*);
    };
}

/// The tokens of a value, up to the next comma or the end, in a map or in a list.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_value {
    ($writer:ident; $within:ident; [$($value:tt)*]; , $($rest:tt)*) => {
        $crate::__definition_scalar!($writer; $($value)*);
        $crate::__definition_next!($writer; $within; $($rest)*);
    };
    ($writer:ident; $within:ident; [$($value:tt)*];) => {
        $crate::__definition_scalar!($writer; $($value)*);
    };
    ($writer:ident; $within:ident; [$($value:tt)*]; $next:tt $($rest:tt)*) => {
        $crate::__definition_value!($writer; $within; [$($value)* $next]; $($rest)*);
    };
}

/// The entry or the item after a comma, unless the comma was the last token.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_next {
    ($writer:ident; $within:ident;) => {};
    ($writer:ident; map; $($rest:tt)+) => {
        $writer.next();
        $crate::__definition_map!($writer; $($rest)+);
    };
    ($writer:ident; list; $($rest:tt)+) => {
        $writer.next();
        $crate::__definition_list!($writer; $($rest)+);
    };
}

/// The items of a list, up to the end of the tokens.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_list {
    ($writer:ident;) => {};
    ($writer:ident; $($rest:tt)+) => {
        $crate::__definition_value!($writer; list; []; $($rest)+);
    };
}

/// One value: a map, a list, a value of the code, or the words of a scalar.
#[doc(hidden)]
#[macro_export]
macro_rules! __definition_scalar {
    ($writer:ident; { $($entries:tt)* }) => {
        $writer.open_map();
        $crate::__definition_map!($writer; $($entries)*);
        $writer.close_map();
    };
    ($writer:ident; [ $($items:tt)* ]) => {
        $writer.open_list();
        $crate::__definition_list!($writer; $($items)*);
        $writer.close_list();
    };
    ($writer:ident; ( $value:expr )) => {
        $writer.value(&$value);
    };
    ($writer:ident; $($words:tt)+) => {
        $writer.scalar(::core::concat!($(::core::stringify!($words)),+));
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_word_a_number_and_a_quoted_text_each_stand_as_yaml_reads_them() {
        let written: DefinitionText<serde_json::Value> = definition_text! {
            api: Produce,
            count: 3,
            negative: -2,
            delay: 750ms,
            pattern: "orders.*",
            raw: r"a \ b",
            flag: true,
            nothing: null,
        };

        assert_eq!(
            written.read().unwrap(),
            serde_json::json!({
                "api": "Produce", "count": 3, "negative": -2, "delay": "750ms", "pattern": "orders.*",
                "raw": "a \\ b", "flag": true, "nothing": null,
            })
        );
    }

    #[test]
    fn a_value_of_the_code_stands_in_its_serde_form() {
        let topics = vec!["orders.eu", "orders.us"];
        let tries: u64 = 2;

        let written: DefinitionText<serde_json::Value> = definition_text! {
            topics: (topics),
            trigger: { first: (tries) },
            steps: [{}, { title: Recovery }],
        };

        assert_eq!(
            written.read().unwrap(),
            serde_json::json!({
                "topics": ["orders.eu", "orders.us"],
                "trigger": { "first": 2 },
                "steps": [{}, { "title": "Recovery" }],
            })
        );
    }

    #[test]
    fn a_definition_without_a_name_reads_with_the_name_it_is_given() {
        let named: DefinitionText<serde_json::Value> = definition_text! { name: churn, mode: sequence };
        let unnamed: DefinitionText<serde_json::Value> = definition_text! { mode: sequence };

        assert_eq!(named.read_named("scenario").unwrap()["name"], "churn");
        assert_eq!(unnamed.read_named("scenario").unwrap()["name"], "scenario");
        assert!(named.names_itself().unwrap());
        assert!(!unnamed.names_itself().unwrap());
    }
}
