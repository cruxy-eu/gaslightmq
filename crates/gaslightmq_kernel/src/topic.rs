use std::fmt::{self, Write};
use std::str::FromStr;

use crate::text::{self, text_form, text_value};
use crate::{KernelError, PartitionNumber, TopicPartition};

text_value! {
    /// The name of a topic.
    ///
    /// A name follows Kafka's rule:
    /// 1 to 249 characters of `[a-zA-Z0-9._-]`, and not `.` or `..`.
    /// A pattern such as `orders.*` is therefore not a topic name.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let orders = TopicName::new("orders.eu")?;
    /// assert_eq!(orders, "orders.eu");
    /// assert!(TopicName::new("orders/eu").is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    TopicName, "topic name", max: text::MAX_NAME_LEN, rule: text::name
}

impl TopicName {
    /// The partition of this topic with that number.
    pub fn partition(&self, partition: PartitionNumber) -> TopicPartition {
        TopicPartition::new(self.clone(), partition)
    }
}

/// The ID of a topic: the UUID a cluster gives a topic when it creates it.
///
/// Newer versions of the Kafka protocol name a topic by this ID instead of by its [`TopicName`].
/// Kafka writes "no topic ID" as the UUID of all zeros.
/// That UUID is not a `TopicId`: [`from_wire`](Self::from_wire) reads it as `None`.
///
/// The text form is the one Kafka's own tools print: 22 characters of URL-safe base64.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let id: TopicId = "Zm9vYmFyYmF6cXV4MTIzNA".parse()?;
/// assert_eq!(id.as_bytes(), b"foobarbazqux1234");
/// assert_eq!(id.to_string(), "Zm9vYmFyYmF6cXV4MTIzNA");
/// assert_eq!(TopicId::from_wire([0; 16]), None);
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId([u8; 16]);

const WHAT: &str = "topic ID";
const TEXT_LEN: usize = 22;
const TEXT_FORM: &str = "22 characters of URL-safe base64";
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

impl TopicId {
    /// Keep `bytes` as a topic ID, unless they are all zero.
    ///
    /// # Errors
    ///
    /// [`KernelError::Reserved`] when `bytes` are all zero.
    pub fn new(bytes: [u8; 16]) -> Result<Self, KernelError> {
        Self::from_wire(bytes).ok_or_else(|| KernelError::Reserved {
            what: WHAT,
            value: Self(bytes).to_string(),
        })
    }

    /// The 16 bytes of the UUID, in the order Kafka writes them.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Read the UUID Kafka writes, in which all zeros means "no topic ID".
    pub fn from_wire(raw: [u8; 16]) -> Option<Self> {
        (raw != [0; 16]).then_some(Self(raw))
    }

    /// The UUID Kafka writes: the topic ID, or all zeros for `None`.
    pub fn to_wire(id: Option<Self>) -> [u8; 16] {
        id.map_or([0; 16], |id| id.0)
    }

    /// The six bits that one character of the text form stands for.
    fn sextet(character: char, position: usize) -> Result<u8, KernelError> {
        let byte = u8::try_from(character).ok();
        (0u8..)
            .zip(ALPHABET)
            .find_map(|(index, &letter)| (Some(letter) == byte).then_some(index))
            .ok_or(KernelError::InvalidCharacter {
                what: WHAT,
                character,
                position,
            })
    }
}

impl TryFrom<[u8; 16]> for TopicId {
    type Error = KernelError;

    fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl From<TopicId> for [u8; 16] {
    fn from(id: TopicId) -> Self {
        id.0
    }
}

/// Reads the UUID as one number, with the first byte as the most significant.
impl TryFrom<u128> for TopicId {
    type Error = KernelError;

    fn try_from(value: u128) -> Result<Self, Self::Error> {
        Self::new(value.to_be_bytes())
    }
}

/// Gives the UUID as one number, with the first byte as the most significant.
impl From<TopicId> for u128 {
    fn from(id: TopicId) -> Self {
        u128::from_be_bytes(id.0)
    }
}

/// Reads the text form: 22 characters of URL-safe base64 without padding.
impl FromStr for TopicId {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || KernelError::Malformed {
            what: WHAT,
            input: text.to_owned(),
            expected: TEXT_FORM,
        };
        if text.is_empty() {
            return Err(KernelError::Empty { what: WHAT });
        }
        let mut sextets = [0u8; TEXT_LEN];
        let mut count = 0;
        for (position, character) in text.chars().enumerate() {
            let sextet = Self::sextet(character, position)?;
            *sextets.get_mut(position).ok_or_else(malformed)? = sextet;
            count += 1;
        }
        // 22 characters carry 132 bits, and a UUID has 128: the last four bits are zero.
        if count != TEXT_LEN || sextets[TEXT_LEN - 1] & 0b1111 != 0 {
            return Err(malformed());
        }

        let mut bytes = [0u8; 16];
        for (group, chunk) in sextets.chunks(4).enumerate() {
            let mut bits = 0u32;
            for index in 0..4 {
                bits = bits << 6 | u32::from(chunk.get(index).copied().unwrap_or(0));
            }
            let [_, first, second, third] = bits.to_be_bytes();
            for (index, byte) in [first, second, third].into_iter().enumerate() {
                if let Some(slot) = bytes.get_mut(group * 3 + index) {
                    *slot = byte;
                }
            }
        }
        Self::new(bytes)
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for chunk in self.0.chunks(3) {
            let mut bits = 0u32;
            for index in 0..3 {
                bits = bits << 8 | u32::from(chunk.get(index).copied().unwrap_or(0));
            }
            // Three bytes give four characters, and the last byte of the UUID gives two.
            for index in 0..=chunk.len() {
                let sextet = (bits >> (18 - 6 * index)) & 0b11_1111;
                f.write_char(char::from(ALPHABET[sextet as usize]))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TopicId({self})")
    }
}

text_form!(TopicId);

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use super::*;

    const BYTES: [u8; 16] = *b"foobarbazqux1234";
    const TEXT: &str = "Zm9vYmFyYmF6cXV4MTIzNA";

    #[test]
    fn a_topic_name_keeps_its_text() {
        let name = TopicName::new("orders.eu").unwrap();

        assert_eq!(name.as_str(), "orders.eu");
        assert_eq!(name.to_string(), "orders.eu");
        assert_eq!(format!("{name:?}"), "TopicName(\"orders.eu\")");
        assert_eq!(format!("{name:>11}|"), "  orders.eu|");
    }

    #[test]
    fn a_topic_name_follows_kafkas_rule() {
        assert_eq!(TopicName::MAX_LEN, 249);
        assert!(TopicName::new("a".repeat(249)).is_ok());
        assert_eq!(
            TopicName::new("a".repeat(250)),
            Err(KernelError::TooLong {
                what: "topic name",
                len: 250,
                max: 249
            })
        );
        assert_eq!(TopicName::new(""), Err(KernelError::Empty { what: "topic name" }));
        assert_eq!(
            TopicName::new(".."),
            Err(KernelError::Reserved {
                what: "topic name",
                value: "..".into()
            })
        );
        assert_eq!(
            TopicName::new("orders.*"),
            Err(KernelError::InvalidCharacter {
                what: "topic name",
                character: '*',
                position: 7
            })
        );
    }

    #[test]
    fn a_topic_name_converts_from_and_to_text() {
        let name = TopicName::new("orders.eu").unwrap();

        assert_eq!("orders.eu".parse::<TopicName>(), Ok(name.clone()));
        assert_eq!(TopicName::try_from("orders.eu"), Ok(name.clone()));
        assert_eq!(TopicName::try_from(String::from("orders.eu")), Ok(name.clone()));
        assert_eq!(String::from(name.clone()), "orders.eu");
        assert_eq!(AsRef::<str>::as_ref(&name), "orders.eu");
        assert!(TopicName::try_from("orders eu").is_err());
        assert!(TopicName::try_from(String::from("orders eu")).is_err());
        assert!("orders eu".parse::<TopicName>().is_err());
    }

    #[test]
    fn a_topic_name_compares_with_text() {
        let name = TopicName::new("orders.eu").unwrap();

        assert_eq!(name, "orders.eu");
        assert_eq!(name, *"orders.eu");
        assert_eq!(name, String::from("orders.eu"));
        assert_eq!("orders.eu", name);
        assert_eq!(*"orders.eu", name);
        assert_eq!(String::from("orders.eu"), name);
        assert_ne!(name, "orders.us");
    }

    #[test]
    fn topic_names_order_as_their_text() {
        let names: BTreeSet<TopicName> = ["payments", "orders.us", "orders.eu"]
            .into_iter()
            .map(|name| name.parse().unwrap())
            .collect();

        let sorted: Vec<&str> = names.iter().map(TopicName::as_str).collect();
        assert_eq!(sorted, ["orders.eu", "orders.us", "payments"]);
    }

    #[test]
    fn a_map_finds_a_topic_name_by_text() {
        let partitions = HashMap::from([(TopicName::new("orders.eu").unwrap(), 6)]);

        assert_eq!(partitions.get("orders.eu"), Some(&6));
        assert_eq!(partitions.get("orders.us"), None);
    }

    #[test]
    fn a_topic_names_its_partitions() {
        let orders = TopicName::new("orders.eu").unwrap();

        let partition = orders.partition(PartitionNumber::from(3));

        assert_eq!(partition.to_string(), "orders.eu/3");
    }

    #[test]
    fn a_topic_id_keeps_its_bytes() {
        let id = TopicId::new(BYTES).unwrap();

        assert_eq!(id.as_bytes(), &BYTES);
        assert_eq!(<[u8; 16]>::from(id), BYTES);
        assert_eq!(TopicId::try_from(BYTES), Ok(id));
    }

    #[test]
    fn a_topic_id_is_not_all_zeros() {
        let reserved = KernelError::Reserved {
            what: "topic ID",
            value: "AAAAAAAAAAAAAAAAAAAAAA".into(),
        };

        assert_eq!(TopicId::new([0; 16]), Err(reserved.clone()));
        assert_eq!(TopicId::try_from([0; 16]), Err(reserved.clone()));
        assert_eq!(TopicId::try_from(0u128), Err(reserved.clone()));
        assert_eq!("AAAAAAAAAAAAAAAAAAAAAA".parse::<TopicId>(), Err(reserved));
    }

    #[test]
    fn the_wire_writes_no_topic_id_as_all_zeros() {
        let id = TopicId::new(BYTES).unwrap();

        assert_eq!(TopicId::from_wire([0; 16]), None);
        assert_eq!(TopicId::from_wire(BYTES), Some(id));
        assert_eq!(TopicId::to_wire(None), [0; 16]);
        assert_eq!(TopicId::to_wire(Some(id)), BYTES);
    }

    #[test]
    fn a_topic_id_converts_from_and_to_one_number() {
        let id = TopicId::try_from(1u128).unwrap();

        assert_eq!(id.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(u128::from(id), 1);
        assert_eq!(u128::from(TopicId::new(BYTES).unwrap()), u128::from_be_bytes(BYTES));
    }

    #[test]
    fn a_topic_id_prints_as_url_safe_base64() {
        assert_eq!(TopicId::new(BYTES).unwrap().to_string(), TEXT);
        assert_eq!(TopicId::new([0xFF; 16]).unwrap().to_string(), "_____________________w");
        assert_eq!(TopicId::new([0xFB; 16]).unwrap().to_string(), "-_v7-_v7-_v7-_v7-_v7-w");
        assert_eq!(TopicId::try_from(1u128).unwrap().to_string(), "AAAAAAAAAAAAAAAAAAAAAQ");
        assert_eq!(
            format!("{:?}", TopicId::new(BYTES).unwrap()),
            format!("TopicId({TEXT})")
        );
    }

    #[test]
    fn a_topic_id_reads_what_it_prints() {
        for bytes in [
            BYTES,
            [0xFF; 16],
            [0xFB; 16],
            [1; 16],
            *b"\x00\x10\x83\x10\x51\x87\x20\x92\x8b\x30\xd3\x8f\x41\x14\x93\xff",
        ] {
            let id = TopicId::new(bytes).unwrap();

            assert_eq!(id.to_string().parse::<TopicId>(), Ok(id));
            assert_eq!(TopicId::try_from(id.to_string()), Ok(id));
            assert_eq!(TopicId::try_from(id.to_string().as_str()), Ok(id));
            assert_eq!(String::from(id), id.to_string());
        }
    }

    #[test]
    fn a_topic_id_has_22_characters() {
        for text in ["Zm9v", "Zm9vYmFyYmF6cXV4MTIzN", "Zm9vYmFyYmF6cXV4MTIzNAA"] {
            assert_eq!(
                text.parse::<TopicId>(),
                Err(KernelError::Malformed {
                    what: "topic ID",
                    input: text.to_owned(),
                    expected: "22 characters of URL-safe base64"
                }),
                "{text:?}"
            );
        }
        assert_eq!("".parse::<TopicId>(), Err(KernelError::Empty { what: "topic ID" }));
    }

    #[test]
    fn a_topic_id_takes_only_the_url_safe_alphabet() {
        for (text, character, position) in [
            ("Zm9vYmFyYmF6cXV4MTIz+A", '+', 20),
            ("/m9vYmFyYmF6cXV4MTIzNA", '/', 0),
            ("Zm9vYmFyYmF6cXV4MTIzN=", '=', 21),
            ("Zm9vYmFyYmF6cXV4MTIzNé", 'é', 21),
        ] {
            assert_eq!(
                text.parse::<TopicId>(),
                Err(KernelError::InvalidCharacter {
                    what: "topic ID",
                    character,
                    position
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_topic_id_has_one_text_form() {
        // `B` ends in the bits 0001, which no UUID has room for.
        assert!(matches!(
            "Zm9vYmFyYmF6cXV4MTIzNB".parse::<TopicId>(),
            Err(KernelError::Malformed { .. })
        ));
    }

    #[test]
    fn topic_ids_order_as_their_bytes() {
        assert!(TopicId::try_from(1u128).unwrap() < TopicId::try_from(2u128).unwrap());
        assert!(TopicId::new([1; 16]).unwrap() < TopicId::new([2; 16]).unwrap());
    }
}
