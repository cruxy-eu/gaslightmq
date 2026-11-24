//! What an index is configured with.
//!
//! A property is a public field, and each one has a default.

use gaslightmq_field::Field;

/// What one [`CallIndex`](crate::CallIndex) is configured with.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_index::prelude::*;
///
/// let mut configuration = IndexConfiguration::new();
/// assert_eq!(configuration.facets, IndexConfiguration::FACETS);
/// assert_eq!(configuration.pattern_separators, ['.', '-', '_']);
/// assert_eq!(configuration.pattern_min_values, 2);
///
/// // A workspace whose topics are `orders/eu` and `orders/us`, and that also groups by error code.
/// configuration.pattern_separators = vec!['/'];
/// configuration.facets.push(Field::ErrorCode);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexConfiguration {
    /// Default: [`IndexConfiguration::FACETS`].
    ///
    /// The fields that [`CallIndex::facets`](crate::CallIndex::facets) groups the calls by, in this order.
    /// Every field can have a facet, and [`CallIndex::facet`](crate::CallIndex::facet) gives the facet of one field
    /// whether this list names it or not.
    pub facets: Vec<Field>,
    /// Default: `.`, `-` and `_`, the punctuation of a topic name.
    ///
    /// The characters behind which a value is cut to suggest a pattern:
    /// `orders.eu` is cut behind its `.`, which suggests `orders.*`.
    /// Without a separator, no facet suggests a pattern.
    pub pattern_separators: Vec<char>,
    /// Default: 2.
    ///
    /// How many values of a facet a pattern stands for at least.
    /// A pattern that fewer values share is not suggested, because it says no more than those values do.
    pub pattern_min_values: usize,
}

impl IndexConfiguration {
    /// The fields that an index groups by unless someone says otherwise:
    /// topics, APIs, consumer groups, transactional IDs, clients, brokers and outcomes.
    pub const FACETS: [Field; 7] = [
        Field::Topic,
        Field::Api,
        Field::Group,
        Field::TransactionalId,
        Field::ClientId,
        Field::Endpoint,
        Field::Outcome,
    ];

    /// The defaults.
    pub fn new() -> Self {
        Self {
            facets: Self::FACETS.to_vec(),
            pattern_separators: vec!['.', '-', '_'],
            pattern_min_values: 2,
        }
    }
}

/// The defaults.
impl Default for IndexConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_starts_with_the_defaults() {
        let configuration = IndexConfiguration::new();

        assert_eq!(configuration.facets, IndexConfiguration::FACETS);
        assert_eq!(configuration.pattern_separators, ['.', '-', '_']);
        assert_eq!(configuration.pattern_min_values, 2);
        assert_eq!(configuration, IndexConfiguration::default());
    }

    #[test]
    fn the_default_facets_are_those_of_the_spec() {
        let names: Vec<&str> = IndexConfiguration::FACETS.iter().map(|field| field.name()).collect();

        assert_eq!(
            names,
            [
                "topic",
                "api",
                "group",
                "transactional.id",
                "client.id",
                "endpoint",
                "outcome"
            ]
        );
    }
}
