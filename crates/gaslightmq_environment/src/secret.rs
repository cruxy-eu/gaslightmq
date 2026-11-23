use std::fmt;
use std::sync::Arc;

use crate::text::kept_text;

/// Where a secret lives: a file, or an environment variable.
///
/// A secret is a password, a private key or a token.
/// A definition never holds one.
/// It holds a reference, so a secret cannot land in git by accident.
///
/// | Reference | Names | Form |
/// |---|---|---|
/// | [`File`](Self::File) | The file that holds the secret. Files are the default: platforms mount secrets as files. | `{ file: /run/secrets/kafka-password }` |
/// | [`Variable`](Self::Variable) | The environment variable that holds the secret, for quick local runs. | `{ env: KAFKA_PASSWORD }` |
///
/// This type only names the place.
/// Nothing in this crate opens the file or looks at the variable, and no type here holds a secret.
/// Whoever connects reads the secret when it needs it, and reads it again for every new connection.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let password = SecretRef::File("/run/secrets/kafka-password".parse().expect("a file path"));
/// assert_eq!(password.to_string(), "the file /run/secrets/kafka-password");
///
/// let password = SecretRef::Variable("KAFKA_PASSWORD".parse()?);
/// assert_eq!(password.to_string(), "the environment variable KAFKA_PASSWORD");
/// # Ok::<(), VariableNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecretRef {
    /// The file that holds the secret.
    File(FilePath),
    /// The environment variable that holds the secret.
    Variable(VariableName),
}

impl SecretRef {
    /// The path of the file, when the reference names a file.
    pub fn file(&self) -> Option<&FilePath> {
        match self {
            Self::File(path) => Some(path),
            Self::Variable(_) => None,
        }
    }

    /// The name of the environment variable, when the reference names one.
    pub fn variable(&self) -> Option<&VariableName> {
        match self {
            Self::File(_) => None,
            Self::Variable(name) => Some(name),
        }
    }
}

/// Writes what the reference names, for a person: `the file secrets/kafka-password`.
impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(f, "the file {path}"),
            Self::Variable(name) => write!(f, "the environment variable {name}"),
        }
    }
}

/// Why what a secret field holds is not a [`SecretRef`].
///
/// No variant holds what was read, and no message repeats it, because it can be a secret.
/// The same holds for the errors of what a reference is made of: [`FilePathError`] and [`VariableNameError`].
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// assert_eq!(
///     SecretRefError::Literal.to_string(),
///     "a secret is a reference and never the value: write { file: … } or { env: … }"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SecretRefError {
    /// The field holds a value, and not a reference to where the value lives.
    #[error("a secret is a reference and never the value: write {{ file: … }} or {{ env: … }}")]
    Literal,
    /// The reference names neither a file nor an environment variable.
    #[error("a secret reference names a file or an environment variable: {{ file: … }} or {{ env: … }}")]
    NoPlace,
    /// The reference names a file and an environment variable, or one of them twice.
    #[error("a secret reference names one place: a file or an environment variable, and not both")]
    TwoPlaces,
    /// The reference has a key that is neither `file` nor `env`.
    #[error("a secret reference has the key `file` or the key `env`")]
    UnknownKey,
    /// The path of the file or the name of the variable is not text.
    #[error("a secret reference names its file or its environment variable as text")]
    NotText,
}

/// The path of a file, as someone wrote it: `/run/secrets/kafka-ca.pem` or `secrets/kafka-password`.
///
/// A path has at least one character, and it holds no control character.
///
/// A path is text here.
/// Nothing in this crate opens it, and what a relative path resolves against is for whoever reads the file to say.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let ca: FilePath = "/run/secrets/kafka-ca.pem".parse()?;
/// assert_eq!(ca, "/run/secrets/kafka-ca.pem");
/// assert_eq!(FilePath::new(""), Err(FilePathError::Empty));
/// # Ok::<(), gaslightmq_environment::FilePathError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FilePath(Arc<str>);

impl FilePath {
    /// Checks that the path has a character, and no control character.
    ///
    /// # Errors
    ///
    /// [`FilePathError::Empty`] for no text, and [`FilePathError::ControlCharacter`] for a text that holds one.
    pub fn new(path: impl AsRef<str>) -> Result<Self, FilePathError> {
        let path = path.as_ref();
        if path.is_empty() {
            Err(FilePathError::Empty)
        } else if let Some(character) = path.chars().find(|character| character.is_control()) {
            Err(FilePathError::ControlCharacter { character })
        } else {
            Ok(Self(path.into()))
        }
    }
}

kept_text!(FilePath, FilePathError);

/// Why a text is not a [`FilePath`].
///
/// The error does not repeat the text.
/// A secret reference holds a path, and someone who writes the secret where the path belongs wrote a secret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FilePathError {
    /// The text has no character.
    #[error("file path \"\" is empty")]
    Empty,
    /// The text holds a control character, such as a line break.
    #[error("a file path holds the control character {character:?}")]
    ControlCharacter { character: char },
}

/// The name of an environment variable, such as `KAFKA_PASSWORD`.
///
/// A name has at least one character.
/// It holds no `=`, no white space and no control character.
///
/// Nothing in this crate looks at the variable.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let variable: VariableName = "KAFKA_PASSWORD".parse()?;
/// assert_eq!(variable, "KAFKA_PASSWORD");
/// assert!(VariableName::new("KAFKA_PASSWORD=hunter2").is_err());
/// # Ok::<(), VariableNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VariableName(Arc<str>);

impl VariableName {
    /// Checks that the name has a character, and none that the name of a variable cannot hold.
    ///
    /// # Errors
    ///
    /// [`VariableNameError::Empty`] for no text,
    /// and [`VariableNameError::InvalidCharacter`] for a `=`, white space or a control character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, VariableNameError> {
        let name = name.as_ref();
        let invalid = |character: &char| *character == '=' || character.is_whitespace() || character.is_control();
        if name.is_empty() {
            Err(VariableNameError::Empty)
        } else if let Some(character) = name.chars().find(invalid) {
            Err(VariableNameError::InvalidCharacter { character })
        } else {
            Ok(Self(name.into()))
        }
    }
}

kept_text!(VariableName, VariableNameError);

/// Why a text is not a [`VariableName`].
///
/// The error does not repeat the text.
/// Someone who writes `KAFKA_PASSWORD=hunter2` where a name belongs wrote a secret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VariableNameError {
    /// The text has no character.
    #[error("the name of an environment variable is empty")]
    Empty,
    /// The text holds a `=`, white space or a control character.
    #[error("the name of an environment variable holds the character {character:?}")]
    InvalidCharacter { character: char },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_names_a_file_or_a_variable() {
        let path = FilePath::new("secrets/kafka-password").unwrap();
        let name = VariableName::new("KAFKA_PASSWORD").unwrap();
        let file = SecretRef::File(path.clone());
        let variable = SecretRef::Variable(name.clone());

        assert_eq!(file.file(), Some(&path));
        assert_eq!(file.variable(), None);
        assert_eq!(variable.variable(), Some(&name));
        assert_eq!(variable.file(), None);
        assert_eq!(file.to_string(), "the file secrets/kafka-password");
        assert_eq!(variable.to_string(), "the environment variable KAFKA_PASSWORD");
    }

    #[test]
    fn a_path_keeps_its_text() {
        for text in [
            "/run/secrets/kafka-ca.pem",
            "secrets/kafka password",
            "C:\\secrets\\ca.pem",
            "ca.pem",
        ] {
            let path = FilePath::new(text).unwrap();
            assert_eq!(path.as_str(), text);
            assert_eq!(path.to_string(), text);
            assert_eq!(text.parse::<FilePath>(), Ok(path));
        }
    }

    #[test]
    fn a_path_has_a_character_and_no_control_character() {
        assert_eq!(FilePath::new(""), Err(FilePathError::Empty));
        assert_eq!(
            FilePath::new("secrets/ca\n.pem"),
            Err(FilePathError::ControlCharacter { character: '\n' })
        );
        assert_eq!(FilePath::new("").unwrap_err().to_string(), "file path \"\" is empty");
        assert_eq!(
            FilePath::new("ca\0.pem").unwrap_err().to_string(),
            "a file path holds the control character '\\0'"
        );
    }

    #[test]
    fn the_error_of_a_path_does_not_repeat_the_text() {
        // A secret reference holds a path, and someone can write the secret where the path belongs.
        for (text, character) in [("hunter2\n", '\n'), ("hunter2\0", '\0'), ("hun\tter2", '\t')] {
            let error = FilePath::new(text).unwrap_err();
            assert_eq!(error, FilePathError::ControlCharacter { character });
            assert!(!error.to_string().contains("hun"), "{error}");
            assert!(!format!("{error:?}").contains("hun"), "{error:?}");
        }
    }

    #[test]
    fn a_variable_name_keeps_its_text() {
        for text in ["KAFKA_PASSWORD", "kafka.password", "_", "GMQ-TOKEN-1"] {
            let name = VariableName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(text.parse::<VariableName>(), Ok(name));
        }
    }

    #[test]
    fn the_error_of_a_variable_name_does_not_repeat_the_text() {
        assert_eq!(VariableName::new(""), Err(VariableNameError::Empty));
        for (text, character) in [
            ("KAFKA_PASSWORD=hunter2", '='),
            ("KAFKA PASSWORD", ' '),
            ("KAFKA\tPASSWORD", '\t'),
            ("hunter2\0", '\0'),
        ] {
            let error = VariableName::new(text).unwrap_err();
            assert_eq!(error, VariableNameError::InvalidCharacter { character });
            assert!(!error.to_string().contains("hunter2"));
            assert!(!format!("{error:?}").contains("hunter2"));
        }
        assert_eq!(
            VariableName::new("A=B").unwrap_err().to_string(),
            "the name of an environment variable holds the character '='"
        );
    }
}
