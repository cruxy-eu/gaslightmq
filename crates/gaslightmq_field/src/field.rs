use std::fmt;
use std::str::FromStr;

use crate::text::text_form;

/// A named property of a connection or a call, such as `client.id` or `topic`.
///
/// The fields are the whole vocabulary of targeting.
/// A filter in Observation, a selector of a rule and a facet of the index all name fields from this list.
///
/// | Group | Fields |
/// |---|---|
/// | [Connection](FieldGroup::Connection) | `connection`, `address`, `host`, `endpoint`, `client.id`, `client.software` |
/// | [Request](FieldGroup::Request) | `api`, `api.version`, `topic`, `partition`, `acks` |
/// | [Learned](FieldGroup::Learned) | `group`, `member.id`, `group.instance.id`, `transactional.id` |
/// | [Response](FieldGroup::Response) | `outcome`, `error.code`, `latency` |
///
/// The list is closed on purpose.
/// A new field changes every district that speaks this vocabulary,
/// so every `match` over the fields stops compiling until it handles the new one.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let field: Field = "client.id".parse()?;
/// assert_eq!(field, Field::ClientId);
/// assert_eq!(field.to_string(), "client.id");
/// assert_eq!(field.group(), FieldGroup::Connection);
///
/// // A rule that tests a response field can only match after the backend answered.
/// let selector = [Field::Api, Field::Topic, Field::Outcome];
/// assert!(selector.iter().any(|field| field.group() == FieldGroup::Response));
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Field {
    /// `connection`: the number of the connection, such as `#7`.
    Connection,
    /// `address`: the address the client connects from, such as `10.0.4.27:51459`.
    Address,
    /// `host`: the IP address the client connects from, such as `10.0.4.27`.
    Host,
    /// `endpoint`: the endpoint the client connected to, such as `broker 2`.
    Endpoint,
    /// `client.id`: the client ID the requests carry, such as `order-ingest`.
    ClientId,
    /// `client.software`: the name and the version of the client library, such as `librdkafka 2.6.1`.
    ClientSoftware,
    /// `api`: the API of the request, such as `Produce`.
    Api,
    /// `api.version`: the version of the API the request uses, such as `11`.
    ApiVersion,
    /// `topic`: a topic the request names, such as `orders.eu`.
    Topic,
    /// `partition`: a partition the request names, such as `3`.
    Partition,
    /// `acks`: the acknowledgements a Produce request asks for, such as `-1`.
    Acks,
    /// `group`: the consumer group of the caller, such as `analytics-etl`.
    Group,
    /// `member.id`: the ID of the caller as a member of its group.
    MemberId,
    /// `group.instance.id`: the instance ID of a static member, such as `etl-0`.
    GroupInstanceId,
    /// `transactional.id`: the transactional ID of the caller, such as `payments-tx-1`.
    TransactionalId,
    /// `outcome`: how the backend answered, such as `ok`.
    Outcome,
    /// `error.code`: the error the backend answered with, such as `NOT_LEADER_OR_FOLLOWER`.
    ErrorCode,
    /// `latency`: how long the backend took to answer, such as `14ms`.
    Latency,
}

impl Field {
    /// Every field, group by group.
    pub const ALL: [Field; 18] = [
        Field::Connection,
        Field::Address,
        Field::Host,
        Field::Endpoint,
        Field::ClientId,
        Field::ClientSoftware,
        Field::Api,
        Field::ApiVersion,
        Field::Topic,
        Field::Partition,
        Field::Acks,
        Field::Group,
        Field::MemberId,
        Field::GroupInstanceId,
        Field::TransactionalId,
        Field::Outcome,
        Field::ErrorCode,
        Field::Latency,
    ];

    /// The name of the field, as a selector writes it.
    pub const fn name(self) -> &'static str {
        match self {
            Field::Connection => "connection",
            Field::Address => "address",
            Field::Host => "host",
            Field::Endpoint => "endpoint",
            Field::ClientId => "client.id",
            Field::ClientSoftware => "client.software",
            Field::Api => "api",
            Field::ApiVersion => "api.version",
            Field::Topic => "topic",
            Field::Partition => "partition",
            Field::Acks => "acks",
            Field::Group => "group",
            Field::MemberId => "member.id",
            Field::GroupInstanceId => "group.instance.id",
            Field::TransactionalId => "transactional.id",
            Field::Outcome => "outcome",
            Field::ErrorCode => "error.code",
            Field::Latency => "latency",
        }
    }

    /// The group of the field, which says where its values come from.
    pub const fn group(self) -> FieldGroup {
        match self {
            Field::Connection
            | Field::Address
            | Field::Host
            | Field::Endpoint
            | Field::ClientId
            | Field::ClientSoftware => FieldGroup::Connection,
            Field::Api | Field::ApiVersion | Field::Topic | Field::Partition | Field::Acks => FieldGroup::Request,
            Field::Group | Field::MemberId | Field::GroupInstanceId | Field::TransactionalId => FieldGroup::Learned,
            Field::Outcome | Field::ErrorCode | Field::Latency => FieldGroup::Response,
        }
    }

    /// Whether each [part](crate::Part) of a call can have its own value of the field.
    ///
    /// `topic` and `partition` name the part itself.
    /// `outcome` and `error.code` are the truth of the part, when the backend answers per partition.
    /// Conditions on these fields apply to the same part.
    pub const fn is_per_part(self) -> bool {
        matches!(
            self,
            Field::Topic | Field::Partition | Field::Outcome | Field::ErrorCode
        )
    }
}

/// Reads the name of a field, as [`Display`](fmt::Display) writes it.
impl FromStr for Field {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Field::ALL
            .into_iter()
            .find(|field| field.name() == text)
            .ok_or_else(|| FieldError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(Field, FieldError);

/// Why a text is not a [`Field`].
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let error = "client.name".parse::<Field>().unwrap_err();
/// assert_eq!(error, FieldError::Unknown { name: "client.name".to_owned() });
/// assert_eq!(error.to_string(), "no field is named \"client.name\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FieldError {
    /// No field has this name.
    #[error("no field is named {name:?}")]
    Unknown { name: String },
}

/// Where the values of a field come from.
///
/// | Group | Describes | Comes from |
/// |---|---|---|
/// | `Connection` | Who is calling. | The identity of the session. |
/// | `Request` | What is being asked. | The decoded request. |
/// | `Learned` | Which group or transaction the caller belongs to. | The request, or else the client context. |
/// | `Response` | What the truth was. | The backend's response, before any lie applies. |
///
/// A scope is a selector that only uses connection fields.
/// A selector that uses a response field can only match after the backend answered.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// assert_eq!(FieldGroup::Learned.fields(), [
///     Field::Group,
///     Field::MemberId,
///     Field::GroupInstanceId,
///     Field::TransactionalId,
/// ]);
/// assert_eq!(FieldGroup::Response.to_string(), "response");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldGroup {
    /// The fields that describe who is calling.
    Connection,
    /// The fields that describe what is being asked.
    Request,
    /// The fields that describe which group or transaction the caller belongs to.
    Learned,
    /// The fields that describe what the truth was.
    Response,
}

impl FieldGroup {
    /// Every group, in the order a call passes through them.
    pub const ALL: [FieldGroup; 4] = [
        FieldGroup::Connection,
        FieldGroup::Request,
        FieldGroup::Learned,
        FieldGroup::Response,
    ];

    /// The name of the group in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            FieldGroup::Connection => "connection",
            FieldGroup::Request => "request",
            FieldGroup::Learned => "learned",
            FieldGroup::Response => "response",
        }
    }

    /// The fields of the group.
    pub const fn fields(self) -> &'static [Field] {
        let (start, end) = match self {
            FieldGroup::Connection => (0, 6),
            FieldGroup::Request => (6, 11),
            FieldGroup::Learned => (11, 15),
            FieldGroup::Response => (15, 18),
        };
        Field::ALL.as_slice().split_at(end).0.split_at(start).1
    }
}

/// Reads the name of a group, as [`Display`](fmt::Display) writes it.
impl FromStr for FieldGroup {
    type Err = FieldGroupError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        FieldGroup::ALL
            .into_iter()
            .find(|group| group.name() == text)
            .ok_or_else(|| FieldGroupError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for FieldGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(FieldGroup, FieldGroupError);

/// Why a text is not a [`FieldGroup`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FieldGroupError {
    /// No group of fields has this name.
    #[error("unknown field group: {name:?}")]
    Unknown { name: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_field_has_the_name_the_spec_gives_it() {
        let names: Vec<&str> = Field::ALL.iter().map(|field| field.name()).collect();

        assert_eq!(
            names,
            [
                "connection",
                "address",
                "host",
                "endpoint",
                "client.id",
                "client.software",
                "api",
                "api.version",
                "topic",
                "partition",
                "acks",
                "group",
                "member.id",
                "group.instance.id",
                "transactional.id",
                "outcome",
                "error.code",
                "latency",
            ]
        );
    }

    #[test]
    fn a_name_reads_back_as_its_field() {
        for field in Field::ALL {
            assert_eq!(field.to_string().parse::<Field>(), Ok(field));
            assert_eq!(Field::try_from(field.name()), Ok(field));
            assert_eq!(String::from(field), field.name());
        }
        assert_eq!(Field::ALL.into_iter().collect::<BTreeSet<_>>().len(), Field::ALL.len());
    }

    #[test]
    fn a_name_is_exact() {
        for text in ["", "Topic", "client_id", "client", "client.id ", "clientid", "error"] {
            assert_eq!(
                text.parse::<Field>(),
                Err(FieldError::Unknown { name: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn every_field_is_in_the_group_the_spec_puts_it_in() {
        let fields = |group: FieldGroup| -> Vec<&str> { group.fields().iter().map(|field| field.name()).collect() };

        assert_eq!(
            fields(FieldGroup::Connection),
            [
                "connection",
                "address",
                "host",
                "endpoint",
                "client.id",
                "client.software"
            ]
        );
        assert_eq!(
            fields(FieldGroup::Request),
            ["api", "api.version", "topic", "partition", "acks"]
        );
        assert_eq!(
            fields(FieldGroup::Learned),
            ["group", "member.id", "group.instance.id", "transactional.id"]
        );
        assert_eq!(fields(FieldGroup::Response), ["outcome", "error.code", "latency"]);
    }

    #[test]
    fn the_fields_of_a_group_are_the_fields_that_name_the_group() {
        for group in FieldGroup::ALL {
            let named: Vec<Field> = Field::ALL.into_iter().filter(|field| field.group() == group).collect();
            assert_eq!(group.fields(), named, "{group}");
        }
    }

    #[test]
    fn the_fields_follow_the_order_of_the_groups() {
        let mut sorted = Field::ALL;
        sorted.sort();
        assert_eq!(sorted, Field::ALL);
        assert!(Field::ALL.is_sorted_by_key(|field| field.group()));
    }

    #[test]
    fn a_part_has_its_own_topic_partition_and_truth() {
        let per_part: Vec<Field> = Field::ALL.into_iter().filter(|field| field.is_per_part()).collect();
        assert_eq!(
            per_part,
            [Field::Topic, Field::Partition, Field::Outcome, Field::ErrorCode]
        );
    }

    #[test]
    fn a_group_name_reads_back_as_its_group() {
        for group in FieldGroup::ALL {
            assert_eq!(group.to_string().parse::<FieldGroup>(), Ok(group));
        }
        assert_eq!(
            "Response".parse::<FieldGroup>(),
            Err(FieldGroupError::Unknown {
                name: "Response".to_owned()
            })
        );
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(
            format!("{:>10}|{:<9}|", Field::Topic, FieldGroup::Request),
            "     topic|request  |"
        );
    }
}
