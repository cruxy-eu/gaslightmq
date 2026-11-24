use std::fmt;
use std::str::FromStr;

use ::uuid::{Builder, Uuid, Variant};

/// The UUID of a resource, such as `0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10`.
///
/// A UUID is the identity of a resource inside the workspace.
/// It lets the workspace track and store resources without one overwriting another.
/// Nothing else references a resource by its UUID: that is what its name is for.
///
/// A resource UUID does not say which kind of resource it identifies.
/// Every kind has a UUID type of its own that wraps this one, and code speaks in those.
///
/// # The rule for UUIDs
///
/// A UUID is a UUID of version 7: the time someone created the resource, and random bits after it.
/// So UUIDs sort in the order their resources were created, to the millisecond.
///
/// As text, a UUID is [`TEXT_LENGTH`](Self::TEXT_LENGTH) characters, which is 36: 32 hexadecimal digits in five
/// groups with a hyphen between them.
/// Reading takes the digits in upper and in lower case, and writing gives them in lower case.
///
/// # Making one
///
/// Nobody writes a UUID by hand.
/// [`new`](Self::new) makes one from the time and from random bits, and its caller hands both in.
/// This crate reads no clock and has no randomness, so the same time and the same bits make the same UUID.
///
/// ```
/// use gaslightmq_resource::{ResourceUuid, ResourceUuidError};
///
/// // The edge reads the time and the random bits, and hands them in.
/// let unix_millis = 1_733_938_011_966;
/// let random = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
/// let uuid = ResourceUuid::new(unix_millis, random);
/// assert_eq!(uuid.to_string(), "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10");
///
/// // A file holds the UUID as text.
/// assert_eq!("0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10".parse::<ResourceUuid>()?, uuid);
///
/// // A UUID of another version is no UUID of a resource.
/// assert!("550e8400-e29b-41d4-a716-446655440000".parse::<ResourceUuid>().is_err());
/// # Ok::<(), ResourceUuidError>(())
/// ```
///
/// With the `serde` feature, a UUID serializes as its text.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceUuid(Uuid);

impl ResourceUuid {
    /// The number of characters of a UUID as text.
    pub const TEXT_LENGTH: usize = 36;

    /// The latest time a UUID holds, in milliseconds since the Unix epoch: it has 48 bits for the time.
    pub const LATEST_UNIX_MILLIS: u64 = (1 << 48) - 1;

    /// The version of every UUID of a resource.
    const VERSION: usize = 7;

    /// The UUID of a resource that someone creates at `unix_millis`, with those random bits.
    ///
    /// `unix_millis` is the time in milliseconds since the Unix epoch.
    /// A time after [`LATEST_UNIX_MILLIS`](Self::LATEST_UNIX_MILLIS), which is in the year 10889, counts as that time.
    ///
    /// A UUID keeps 74 of the 80 bits of `random`.
    /// They tell the UUIDs of one millisecond apart, so they come from a source that does not repeat itself.
    pub const fn new(unix_millis: u64, random: [u8; 10]) -> Self {
        let unix_millis = if unix_millis > Self::LATEST_UNIX_MILLIS {
            Self::LATEST_UNIX_MILLIS
        } else {
            unix_millis
        };
        Self(Builder::from_unix_timestamp_millis(unix_millis, &random).into_uuid())
    }
}

/// Reads the 36 characters of a UUID of version 7.
impl FromStr for ResourceUuid {
    type Err = ResourceUuidError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // The length leaves the one form with hyphens: the library reads a UUID in braces and as a URN too.
        let uuid = (text.len() == Self::TEXT_LENGTH)
            .then(|| Uuid::try_parse(text).ok())
            .flatten()
            .filter(|uuid| uuid.get_version_num() == Self::VERSION && uuid.get_variant() == Variant::RFC4122);
        match uuid {
            Some(uuid) => Ok(Self(uuid)),
            None => Err(ResourceUuidError { input: text.to_owned() }),
        }
    }
}

impl TryFrom<&str> for ResourceUuid {
    type Error = ResourceUuidError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl TryFrom<String> for ResourceUuid {
    type Error = ResourceUuidError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<ResourceUuid> for String {
    fn from(uuid: ResourceUuid) -> Self {
        uuid.to_string()
    }
}

/// Writes the 36 characters of the UUID, with its digits in lower case.
impl fmt::Display for ResourceUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.0.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
    }
}

impl fmt::Debug for ResourceUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResourceUuid({self})")
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for ResourceUuid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ResourceUuid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Why a text is not a [`ResourceUuid`]: it is no UUID of version 7.
///
/// Whoever reads a UUID decides one thing: the text is one, or it is not.
/// So the error has no variants, and it holds the text.
/// The UUID of every kind of resource has an error of its own of the same shape.
///
/// ```
/// use gaslightmq_resource::{ResourceUuid, ResourceUuidError};
///
/// let error = "order-producers".parse::<ResourceUuid>().unwrap_err();
/// assert_eq!(error, ResourceUuidError { input: "order-producers".to_owned() });
/// assert_eq!(error.to_string(), "UUID \"order-producers\" is no UUID of version 7");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("UUID {input:?} is no UUID of version 7")]
pub struct ResourceUuidError {
    /// The text that is no UUID.
    pub input: String,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// The time someone created the resource of [`TEXT`]: 11 December 2024, in milliseconds since the Unix epoch.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of [`TEXT`].
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
    /// The UUID of [`CREATED`] and [`RANDOM`].
    const TEXT: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";
    /// Random bits that are all zeros, and all ones.
    const NO_BITS: [u8; 10] = [0x00; 10];
    const EVERY_BIT: [u8; 10] = [0xff; 10];

    #[test]
    fn a_uuid_is_its_time_and_its_random_bits() {
        let uuid = ResourceUuid::new(CREATED, RANDOM);

        assert_eq!(uuid.to_string(), TEXT);
        assert_eq!(TEXT.parse::<ResourceUuid>(), Ok(uuid));
        assert_eq!(ResourceUuid::try_from(TEXT), Ok(uuid));
        assert_eq!(ResourceUuid::try_from(TEXT.to_owned()), Ok(uuid));
        assert_eq!(String::from(uuid), TEXT);
        assert_eq!(uuid, ResourceUuid::new(CREATED, RANDOM));
    }

    #[test]
    fn a_uuid_is_of_version_7_whatever_its_random_bits_are() {
        assert_eq!(
            ResourceUuid::new(0, NO_BITS).to_string(),
            "00000000-0000-7000-8000-000000000000"
        );
        assert_eq!(
            ResourceUuid::new(0, EVERY_BIT).to_string(),
            "00000000-0000-7fff-bfff-ffffffffffff"
        );
    }

    #[test]
    fn a_time_after_the_latest_counts_as_the_latest() {
        let latest = ResourceUuid::new(ResourceUuid::LATEST_UNIX_MILLIS, NO_BITS);

        assert_eq!(ResourceUuid::LATEST_UNIX_MILLIS, 0xffff_ffff_ffff);
        assert_eq!(latest.to_string(), "ffffffff-ffff-7000-8000-000000000000");
        assert_eq!(ResourceUuid::new(ResourceUuid::LATEST_UNIX_MILLIS + 1, NO_BITS), latest);
        assert_eq!(ResourceUuid::new(u64::MAX, NO_BITS), latest);
    }

    #[test]
    fn uuids_sort_in_the_order_their_resources_were_created() {
        let first = ResourceUuid::new(CREATED, EVERY_BIT);
        let second = ResourceUuid::new(CREATED + 1, NO_BITS);
        let third = ResourceUuid::new(CREATED + 1, RANDOM);

        assert!(first < second);
        // The random bits order the UUIDs of one millisecond.
        assert!(second < third);
        assert_eq!(
            BTreeSet::from([third, first, second]).into_iter().collect::<Vec<_>>(),
            [first, second, third]
        );
    }

    #[test]
    fn a_uuid_reads_in_upper_case_and_writes_in_lower_case() {
        let uuid: ResourceUuid = TEXT.to_uppercase().parse().unwrap();

        assert_eq!(uuid, ResourceUuid::new(CREATED, RANDOM));
        assert_eq!(uuid.to_string(), TEXT);
    }

    #[test]
    fn a_text_that_is_no_uuid_of_version_7_is_an_error() {
        let texts = [
            "",
            "order-producers",
            // Too short and too long by one digit.
            "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d1",
            "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d100",
            // No hexadecimal digit, and a hyphen at another place.
            "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7dxx",
            "0193b6c25-f3e-7a41-9c2d-8e4b1f6a7d10",
            // The other forms of a UUID: without hyphens, in braces and as a URN.
            "0193b6c25f3e7a419c2d8e4b1f6a7d10",
            "{0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10}",
            "urn:uuid:0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10",
            " 0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d1",
            // Version 4, version 1, the UUID of all zeros and the one of all ones.
            "550e8400-e29b-41d4-a716-446655440000",
            "c232ab00-9414-11ec-b3c8-9f6bdeced846",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            // Version 7 in a variant that is not the one of the standard.
            "0193b6c2-5f3e-7a41-1c2d-8e4b1f6a7d10",
            "0193b6c2-5f3e-7a41-dc2d-8e4b1f6a7d10",
        ];

        for text in texts {
            let error = ResourceUuidError { input: text.to_owned() };
            assert_eq!(text.parse::<ResourceUuid>(), Err(error.clone()), "{text:?}");
            assert_eq!(ResourceUuid::try_from(text), Err(error.clone()), "{text:?}");
            assert_eq!(ResourceUuid::try_from(text.to_owned()), Err(error), "{text:?}");
        }
    }

    #[test]
    fn the_error_says_which_text_is_no_uuid() {
        let cases = [
            ("", "UUID \"\" is no UUID of version 7"),
            ("order\nproducers", "UUID \"order\\nproducers\" is no UUID of version 7"),
            (
                "550e8400-e29b-41d4-a716-446655440000",
                "UUID \"550e8400-e29b-41d4-a716-446655440000\" is no UUID of version 7",
            ),
        ];

        for (text, message) in cases {
            assert_eq!(text.parse::<ResourceUuid>().unwrap_err().to_string(), message);
        }
    }

    #[test]
    fn a_uuid_has_36_characters() {
        assert_eq!(ResourceUuid::TEXT_LENGTH, 36);
        assert_eq!(ResourceUuid::new(CREATED, RANDOM).to_string().len(), 36);
    }

    #[test]
    fn a_uuid_pads_like_text() {
        assert_eq!(
            format!("{:>38}|", ResourceUuid::new(CREATED, RANDOM)),
            format!("  {TEXT}|")
        );
    }

    #[test]
    fn a_uuid_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", ResourceUuid::new(CREATED, RANDOM)),
            format!("ResourceUuid({TEXT})")
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_uuid_is_its_text_in_serde() {
        let uuid = ResourceUuid::new(CREATED, RANDOM);

        assert_eq!(serde_json::to_string(&uuid).unwrap(), format!("\"{TEXT}\""));
        assert_eq!(
            serde_json::from_str::<ResourceUuid>(&format!("\"{TEXT}\"")).unwrap(),
            uuid
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_text_that_is_no_uuid_does_not_read_as_one() {
        let error = serde_json::from_str::<ResourceUuid>("\"order-producers\"").unwrap_err();

        assert_eq!(error.to_string(), "UUID \"order-producers\" is no UUID of version 7");
        assert!(serde_json::from_str::<ResourceUuid>("7").is_err());
    }
}
