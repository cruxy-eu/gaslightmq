use std::fmt;

use gaslightmq_field::prelude::*;

use crate::SelectorName;

/// Why a value cannot become a part of a selector, or why a set of selectors does not hold together.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let error = "".parse::<Glob>().unwrap_err();
/// assert_eq!(error, SelectorError::EmptyPattern);
/// assert_eq!(error.to_string(), "a pattern is empty");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SelectorError {
    /// A pattern has no character, so it matches no value.
    EmptyPattern,
    /// A selector name breaks the rule for names.
    InvalidName { input: String, reason: &'static str },
    /// No operator has this name.
    UnknownOperator { name: String },
    /// A condition names no value to test against.
    NoValues,
    /// The values of one condition belong to different fields.
    MixedFields { first: Field, other: Field },
    /// A list of values holds a pattern. Only `matches` reads a pattern, and it takes one.
    PatternAmongValues { field: Field, pattern: String },
    /// The text is not a value of the field.
    Value { field: Field, error: FieldError },
    /// A scope tests a field that does not describe the connection.
    NotAConnectionField { field: Field },
    /// Two selectors of one set have the same name.
    DuplicateName { name: SelectorName },
    /// No selector of the set has this name.
    UnknownSelector { name: SelectorName },
    /// A selector includes a name that no selector of the set has.
    UnknownInclude {
        selector: SelectorName,
        include: SelectorName,
    },
    /// A selector includes itself, directly or through other selectors.
    /// The path starts and ends with the same name.
    IncludeCycle { path: Vec<SelectorName> },
}

impl fmt::Display for SelectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPattern => f.write_str("a pattern is empty"),
            Self::InvalidName { input, reason } => write!(f, "selector name {input:?} {reason}"),
            Self::UnknownOperator { name } => write!(f, "no operator is named {name:?}"),
            Self::NoValues => f.write_str("a condition needs at least one value"),
            Self::MixedFields { first, other } => {
                write!(
                    f,
                    "one condition tests one field, and its values are of {first} and of {other}"
                )
            }
            Self::PatternAmongValues { field, pattern } => write!(
                f,
                "{field}: {pattern:?} is a pattern, and a list holds values. \
                 Write the pattern alone, or use is-any-of for the text itself"
            ),
            Self::Value { field, error } => write!(f, "{field}: {error}"),
            Self::NotAConnectionField { field } => write!(
                f,
                "a scope only tests connection fields, and {field} is a {} field",
                field.group()
            ),
            Self::DuplicateName { name } => write!(f, "two selectors are named {:?}", name.as_str()),
            Self::UnknownSelector { name } => write!(f, "no selector is named {:?}", name.as_str()),
            Self::UnknownInclude { selector, include } => write!(
                f,
                "selector {:?} includes {:?}, and no selector has that name",
                selector.as_str(),
                include.as_str()
            ),
            Self::IncludeCycle { path } => {
                f.write_str("selectors include each other in a circle: ")?;
                for (index, name) in path.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" includes ")?;
                    }
                    write!(f, "{:?}", name.as_str())?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for SelectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Value { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    fn name(text: &str) -> SelectorName {
        text.parse().unwrap()
    }

    #[test]
    fn every_variant_names_what_was_wrong() {
        let not_an_api = Field::Api.parse_value("Prodcue").unwrap_err();
        let cases = [
            (SelectorError::EmptyPattern, "a pattern is empty"),
            (
                SelectorError::InvalidName {
                    input: " x".into(),
                    reason: "starts or ends with white space",
                },
                "selector name \" x\" starts or ends with white space",
            ),
            (
                SelectorError::UnknownOperator { name: "isnt".into() },
                "no operator is named \"isnt\"",
            ),
            (SelectorError::NoValues, "a condition needs at least one value"),
            (
                SelectorError::MixedFields {
                    first: Field::Topic,
                    other: Field::Api,
                },
                "one condition tests one field, and its values are of topic and of api",
            ),
            (
                SelectorError::PatternAmongValues {
                    field: Field::Topic,
                    pattern: "orders.*".into(),
                },
                "topic: \"orders.*\" is a pattern, and a list holds values. \
                 Write the pattern alone, or use is-any-of for the text itself",
            ),
            (
                SelectorError::NotAConnectionField { field: Field::Topic },
                "a scope only tests connection fields, and topic is a request field",
            ),
            (
                SelectorError::DuplicateName { name: name("A") },
                "two selectors are named \"A\"",
            ),
            (
                SelectorError::UnknownSelector { name: name("A") },
                "no selector is named \"A\"",
            ),
            (
                SelectorError::UnknownInclude {
                    selector: name("A"),
                    include: name("B"),
                },
                "selector \"A\" includes \"B\", and no selector has that name",
            ),
            (
                SelectorError::IncludeCycle {
                    path: vec![name("A"), name("B"), name("A")],
                },
                "selectors include each other in a circle: \"A\" includes \"B\" includes \"A\"",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
        assert_eq!(
            SelectorError::Value {
                field: Field::Api,
                error: not_an_api.clone(),
            }
            .to_string(),
            format!("api: {not_an_api}")
        );
    }

    #[test]
    fn the_error_of_the_field_is_the_source() {
        let field = Field::Api.parse_value("Prodcue").unwrap_err();
        let error = SelectorError::Value {
            field: Field::Api,
            error: field.clone(),
        };

        assert_eq!(error.source().map(ToString::to_string), Some(field.to_string()));
        assert!(SelectorError::NoValues.source().is_none());
    }
}
