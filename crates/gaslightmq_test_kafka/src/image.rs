//! The image a cluster runs.

use std::fmt;
use std::str::FromStr;

/// The image the brokers of a cluster run, as `repository:tag`: `apache/kafka-native:4.4.0`.
///
/// The default is Apache Kafka as a native binary, at the version this workspace tests against.
/// Another tag of that repository runs another version of Kafka.
/// Another repository has to hold the same image, as a mirror does:
/// the cluster starts the broker the way `apache/kafka-native` lays it out.
///
/// ```
/// use gaslightmq_test_kafka::KafkaImage;
///
/// let image = KafkaImage::default();
/// assert_eq!(image.to_string(), "apache/kafka-native:4.4.0");
///
/// let older: KafkaImage = "apache/kafka-native:4.1.0".parse()?;
/// assert_eq!(older.repository(), "apache/kafka-native");
/// assert_eq!(older.tag(), "4.1.0");
/// # Ok::<(), gaslightmq_test_kafka::KafkaImageError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaImage {
    repository: String,
    tag: String,
}

impl KafkaImage {
    /// The repository of the default image: Apache Kafka as a native binary.
    pub const REPOSITORY: &'static str = "apache/kafka-native";

    /// The tag of the default image: the version of Kafka this workspace tests against.
    pub const TAG: &'static str = "4.4.0";

    /// The environment variable that names the image for every cluster of a test run.
    ///
    /// It holds an image as `repository:tag`, such as `apache/kafka-native:4.1.0`.
    /// A cluster whose configuration names an image runs that one, whatever the variable says.
    pub const VARIABLE: &'static str = "GASLIGHTMQ_TEST_KAFKA_IMAGE";

    /// Keep `repository` and `tag` as an image.
    ///
    /// # Errors
    ///
    /// The [`KafkaImageError`] that says which part is missing, or which character an image name cannot hold.
    pub fn new(repository: impl Into<String>, tag: impl Into<String>) -> Result<Self, KafkaImageError> {
        let (repository, tag) = (repository.into(), tag.into());
        let input = || format!("{repository}:{tag}");
        if repository.is_empty() {
            return Err(KafkaImageError::MissingRepository { input: input() });
        }
        if tag.is_empty() {
            return Err(KafkaImageError::MissingTag { input: input() });
        }
        if let Some(character) = repository.chars().chain(tag.chars()).find(|c| !is_allowed(*c)) {
            return Err(KafkaImageError::InvalidCharacter {
                input: input(),
                character,
            });
        }
        Ok(Self { repository, tag })
    }

    /// The default repository at another tag, which is another version of Kafka.
    ///
    /// # Errors
    ///
    /// The [`KafkaImageError`] that says what is wrong with `tag`.
    pub fn version(tag: impl Into<String>) -> Result<Self, KafkaImageError> {
        Self::new(Self::REPOSITORY, tag)
    }

    /// The name without the tag, with the registry in front when the image names one.
    pub fn repository(&self) -> &str {
        &self.repository
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }
}

/// The characters of a registry, a repository and a tag.
fn is_allowed(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | '/' | ':')
}

/// `apache/kafka-native` at the version this workspace tests against.
impl Default for KafkaImage {
    fn default() -> Self {
        Self {
            repository: Self::REPOSITORY.to_owned(),
            tag: Self::TAG.to_owned(),
        }
    }
}

impl FromStr for KafkaImage {
    type Err = KafkaImageError;

    /// Reads `repository:tag`. The tag stands after the last `:`, so a registry may name its port.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.rsplit_once(':') {
            // A `/` after the last `:` belongs to a repository behind a registry with a port, and no tag follows.
            Some((repository, tag)) if !tag.contains('/') => Self::new(repository, tag),
            _ => Err(KafkaImageError::MissingTag { input: text.to_owned() }),
        }
    }
}

impl fmt::Display for KafkaImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.repository, self.tag)
    }
}

/// Why a text is not a [`KafkaImage`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaImageError {
    /// Nothing stands before the tag.
    MissingRepository { input: String },
    /// The text names no tag, so it does not say which version of Kafka to run.
    MissingTag { input: String },
    /// The text holds a character that no image name holds.
    InvalidCharacter { input: String, character: char },
}

impl fmt::Display for KafkaImageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::MissingRepository { input } | Self::MissingTag { input } | Self::InvalidCharacter { input, .. }) =
            self;
        write!(f, "invalid Kafka image `{input}`: ")?;
        match self {
            Self::MissingRepository { .. } => f.write_str("missing repository, expected repository:tag"),
            Self::MissingTag { .. } => f.write_str("missing tag, expected repository:tag"),
            Self::InvalidCharacter { character, .. } => write!(f, "an image name cannot hold {character:?}"),
        }
    }
}

impl std::error::Error for KafkaImageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_kafka_native_at_the_tested_version() {
        let image = KafkaImage::default();

        assert_eq!(image.repository(), "apache/kafka-native");
        assert_eq!(image.tag(), "4.4.0");
        assert_eq!(image.to_string(), "apache/kafka-native:4.4.0");
    }

    #[test]
    fn an_image_reads_back_from_its_text_form() {
        let image: KafkaImage = "apache/kafka-native:4.1.0".parse().unwrap();

        assert_eq!(image, KafkaImage::version("4.1.0").unwrap());
        assert_eq!(image.to_string().parse(), Ok(image));
    }

    #[test]
    fn a_registry_may_name_its_port() {
        let image: KafkaImage = "localhost:5000/apache/kafka-native:4.4.0".parse().unwrap();

        assert_eq!(image.repository(), "localhost:5000/apache/kafka-native");
        assert_eq!(image.tag(), "4.4.0");
    }

    #[test]
    fn an_image_without_a_tag_names_no_version() {
        let missing = |input: &str| KafkaImageError::MissingTag {
            input: input.to_owned(),
        };

        assert_eq!(
            "apache/kafka-native".parse::<KafkaImage>(),
            Err(missing("apache/kafka-native"))
        );
        assert_eq!(
            "apache/kafka-native:".parse::<KafkaImage>(),
            Err(missing("apache/kafka-native:"))
        );
        assert_eq!(
            "localhost:5000/kafka-native".parse::<KafkaImage>(),
            Err(missing("localhost:5000/kafka-native"))
        );
    }

    #[test]
    fn an_error_names_the_text_and_what_is_wrong_with_it() {
        assert_eq!(
            ":4.4.0".parse::<KafkaImage>().unwrap_err().to_string(),
            "invalid Kafka image `:4.4.0`: missing repository, expected repository:tag"
        );
        assert_eq!(
            "kafka native:4.4.0".parse::<KafkaImage>().unwrap_err().to_string(),
            "invalid Kafka image `kafka native:4.4.0`: an image name cannot hold ' '"
        );
        assert_eq!(
            "apache/kafka-native".parse::<KafkaImage>().unwrap_err().to_string(),
            "invalid Kafka image `apache/kafka-native`: missing tag, expected repository:tag"
        );
    }
}
