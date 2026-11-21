use std::fmt;

use gaslightmq_kernel::prelude::*;

/// Why a text cannot become a field or a field value.
///
/// `what` names the value, such as `endpoint` or `connection number`.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let error = "client.name".parse::<Field>().unwrap_err();
/// assert_eq!(error, FieldError::UnknownField { name: "client.name".to_owned() });
/// assert_eq!(error.to_string(), "no field is named \"client.name\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FieldError {
    /// No field has this name.
    UnknownField { name: String },
    /// `what` has a fixed set of names, and the text is none of them.
    UnknownName { what: &'static str, input: String },
    /// The text does not have the form of `what`, such as `broker 2`.
    Malformed {
        what: &'static str,
        input: String,
        expected: &'static str,
    },
    /// The text is a number that `what` cannot hold.
    OutOfRange {
        what: &'static str,
        input: String,
        min: i64,
        max: u64,
    },
    /// The field holds a value object of the kernel, and that type refused the text.
    Kernel(KernelError),
}

impl FieldError {
    pub(crate) fn unknown_name(what: &'static str, input: &str) -> Self {
        Self::UnknownName {
            what,
            input: input.to_owned(),
        }
    }

    pub(crate) fn malformed(what: &'static str, input: &str, expected: &'static str) -> Self {
        Self::Malformed {
            what,
            input: input.to_owned(),
            expected,
        }
    }

    pub(crate) fn out_of_range(what: &'static str, input: impl fmt::Display, min: i64, max: u64) -> Self {
        Self::OutOfRange {
            what,
            input: input.to_string(),
            min,
            max,
        }
    }
}

impl fmt::Display for FieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownField { name } => write!(f, "no field is named {name:?}"),
            Self::UnknownName { what, input } => write!(f, "unknown {what}: {input:?}"),
            Self::Malformed { what, input, expected } => {
                write!(f, "{what} {input:?} does not have the form {expected}")
            }
            Self::OutOfRange { what, input, min, max } => {
                write!(f, "{what} {input} is not between {min} and {max}")
            }
            Self::Kernel(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for FieldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}

impl From<KernelError> for FieldError {
    fn from(error: KernelError) -> Self {
        Self::Kernel(error)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                FieldError::UnknownField {
                    name: "client.name".into(),
                },
                "no field is named \"client.name\"",
            ),
            (FieldError::unknown_name("outcome", "okay"), "unknown outcome: \"okay\""),
            (
                FieldError::malformed("endpoint", "broker", "bootstrap or broker N"),
                "endpoint \"broker\" does not have the form bootstrap or broker N",
            ),
            (
                FieldError::out_of_range("API version", "40000", 0, 32_767),
                "API version 40000 is not between 0 and 32767",
            ),
            (
                FieldError::Kernel(KernelError::Empty { what: "topic name" }),
                "topic name is empty",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_kernel_error_is_the_source() {
        let kernel = TopicName::new("orders/eu").unwrap_err();
        let error = FieldError::from(kernel.clone());

        assert_eq!(error.to_string(), kernel.to_string());
        assert_eq!(error.source().map(ToString::to_string), Some(kernel.to_string()));
        assert!(FieldError::unknown_name("outcome", "okay").source().is_none());
    }
}
