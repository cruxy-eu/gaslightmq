//! The image that a container of GaslightMQ runs.

use std::fmt;
use std::str::FromStr;

/// The image of GaslightMQ that a container runs, as `repository:tag`: `ghcr.io/cruxy-eu/gaslightmq:0.1.0`.
///
/// The default is the release of GaslightMQ that this crate belongs to.
/// Another tag of that repository runs another release, and `latest` the one that `just image` builds.
/// Another repository has to hold the same image, as a mirror does: the container runs `gmq` as the image lays it out.
///
/// ```
/// use gaslightmq_testcontainers::GaslightMqImage;
///
/// let image = GaslightMqImage::default();
/// assert_eq!(image.repository(), "ghcr.io/cruxy-eu/gaslightmq");
/// assert_eq!(image.tag(), env!("CARGO_PKG_VERSION"));
///
/// let built: GaslightMqImage = "ghcr.io/cruxy-eu/gaslightmq:latest".parse()?;
/// assert_eq!(built, GaslightMqImage::version("latest")?);
/// # Ok::<(), gaslightmq_testcontainers::GaslightMqImageError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GaslightMqImage {
    repository: String,
    tag: String,
}

impl GaslightMqImage {
    /// The repository of the default image: the one that the releases of GaslightMQ are published to.
    pub const REPOSITORY: &'static str = "ghcr.io/cruxy-eu/gaslightmq";

    /// The tag of the default image: the release that this crate belongs to.
    pub const TAG: &'static str = env!("CARGO_PKG_VERSION");

    /// The environment variable that names the image for every container of a test run.
    ///
    /// It holds an image as `repository:tag`, such as `ghcr.io/cruxy-eu/gaslightmq:latest`.
    /// A container whose configuration names an image runs that one, whatever the variable says.
    pub const VARIABLE: &'static str = "GASLIGHTMQ_IMAGE";

    /// Keep `repository` and `tag` as an image.
    ///
    /// # Errors
    ///
    /// The [`GaslightMqImageError`] that says which part is missing, or which character an image name cannot hold.
    pub fn new(repository: impl Into<String>, tag: impl Into<String>) -> Result<Self, GaslightMqImageError> {
        let (repository, tag) = (repository.into(), tag.into());
        let input = || format!("{repository}:{tag}");
        if repository.is_empty() {
            return Err(GaslightMqImageError::MissingRepository { input: input() });
        }
        if tag.is_empty() {
            return Err(GaslightMqImageError::MissingTag { input: input() });
        }
        if let Some(character) = repository.chars().chain(tag.chars()).find(|c| !is_allowed(*c)) {
            return Err(GaslightMqImageError::InvalidCharacter {
                input: input(),
                character,
            });
        }
        Ok(Self { repository, tag })
    }

    /// The default repository at another tag, which is another release of GaslightMQ.
    ///
    /// # Errors
    ///
    /// The [`GaslightMqImageError`] that says what is wrong with `tag`.
    pub fn version(tag: impl Into<String>) -> Result<Self, GaslightMqImageError> {
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

/// The image of the release that this crate belongs to.
impl Default for GaslightMqImage {
    fn default() -> Self {
        Self {
            repository: Self::REPOSITORY.to_owned(),
            tag: Self::TAG.to_owned(),
        }
    }
}

impl FromStr for GaslightMqImage {
    type Err = GaslightMqImageError;

    /// Reads `repository:tag`. The tag stands after the last `:`, so a registry may name its port.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.rsplit_once(':') {
            // A `/` after the last `:` belongs to a repository behind a registry with a port, and no tag follows.
            Some((repository, tag)) if !tag.contains('/') => Self::new(repository, tag),
            _ => Err(GaslightMqImageError::MissingTag { input: text.to_owned() }),
        }
    }
}

impl fmt::Display for GaslightMqImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.repository, self.tag)
    }
}

/// Why a text is not a [`GaslightMqImage`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GaslightMqImageError {
    /// Nothing stands before the tag.
    #[error("invalid GaslightMQ image `{input}`: missing repository, expected repository:tag")]
    MissingRepository { input: String },
    /// The text names no tag, so it does not say which release of GaslightMQ to run.
    #[error("invalid GaslightMQ image `{input}`: missing tag, expected repository:tag")]
    MissingTag { input: String },
    /// The text holds a character that no image name holds.
    #[error("invalid GaslightMQ image `{input}`: an image name cannot hold {character:?}")]
    InvalidCharacter { input: String, character: char },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_the_release_of_this_crate() {
        let image = GaslightMqImage::default();

        assert_eq!(image.repository(), "ghcr.io/cruxy-eu/gaslightmq");
        assert_eq!(image.tag(), env!("CARGO_PKG_VERSION"));
        assert_eq!(
            image.to_string(),
            format!("ghcr.io/cruxy-eu/gaslightmq:{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn an_image_reads_back_from_its_text_form() {
        let image: GaslightMqImage = "ghcr.io/cruxy-eu/gaslightmq:latest".parse().unwrap();

        assert_eq!(image, GaslightMqImage::version("latest").unwrap());
        assert_eq!(image.to_string().parse(), Ok(image));
    }

    #[test]
    fn a_registry_may_name_its_port() {
        let image: GaslightMqImage = "localhost:5000/gaslightmq:latest".parse().unwrap();

        assert_eq!(image.repository(), "localhost:5000/gaslightmq");
        assert_eq!(image.tag(), "latest");
    }

    #[test]
    fn an_image_without_a_tag_names_no_release() {
        let missing = |input: &str| GaslightMqImageError::MissingTag {
            input: input.to_owned(),
        };

        assert_eq!(
            "ghcr.io/cruxy-eu/gaslightmq".parse::<GaslightMqImage>(),
            Err(missing("ghcr.io/cruxy-eu/gaslightmq"))
        );
        assert_eq!("gaslightmq:".parse::<GaslightMqImage>(), Err(missing("gaslightmq:")));
        assert_eq!(
            "localhost:5000/gaslightmq".parse::<GaslightMqImage>(),
            Err(missing("localhost:5000/gaslightmq"))
        );
    }

    #[test]
    fn an_error_names_the_text_and_what_is_wrong_with_it() {
        assert_eq!(
            ":latest".parse::<GaslightMqImage>().unwrap_err().to_string(),
            "invalid GaslightMQ image `:latest`: missing repository, expected repository:tag"
        );
        assert_eq!(
            "gaslight mq:latest".parse::<GaslightMqImage>().unwrap_err().to_string(),
            "invalid GaslightMQ image `gaslight mq:latest`: an image name cannot hold ' '"
        );
    }
}
