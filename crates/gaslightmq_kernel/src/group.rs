use crate::KernelError;
use crate::text::{self, text_value};

text_value! {
    /// The ID of a consumer group.
    ///
    /// Kafka does not restrict the characters of a group ID.
    /// A group ID has at least one byte and at most 32,767, the longest string a Kafka message carries.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let group = GroupId::new("analytics-etl")?;
    /// assert_eq!(group, "analytics-etl");
    /// assert!(GroupId::new("").is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    GroupId, "group ID", max: text::MAX_STRING_LEN, rule: text::non_empty
}

text_value! {
    /// The ID a group coordinator gives a member of a group.
    ///
    /// A member ID has at least one byte and at most 32,767, the longest string a Kafka message carries.
    ///
    /// Kafka writes "no member ID yet" as an empty string, as in a member's first request to join.
    /// That string is not a `MemberId`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let member = MemberId::from_wire("consumer-1-4f2a")?;
    /// assert_eq!(member, Some(MemberId::new("consumer-1-4f2a")?));
    /// assert_eq!(MemberId::from_wire("")?, None);
    /// assert_eq!(MemberId::to_wire(None), "");
    /// # Ok::<(), KernelError>(())
    /// ```
    MemberId, "member ID", max: text::MAX_STRING_LEN, rule: text::non_empty
}

impl MemberId {
    /// Read the string Kafka writes, in which an empty string means "no member ID yet".
    ///
    /// # Errors
    ///
    /// The error of [`new`](Self::new), when `raw` is not empty and breaks the rule for a member ID.
    pub fn from_wire(raw: &str) -> Result<Option<Self>, KernelError> {
        if raw.is_empty() {
            Ok(None)
        } else {
            Self::new(raw).map(Some)
        }
    }

    /// The string Kafka writes: the member ID, or an empty string for `None`.
    pub fn to_wire(member: Option<&Self>) -> &str {
        member.map_or("", Self::as_str)
    }
}

text_value! {
    /// The `group.instance.id` of a static member of a group.
    ///
    /// Kafka holds a group instance ID to the rule of a topic name:
    /// 1 to 249 characters of `[a-zA-Z0-9._-]`, and not `.` or `..`.
    ///
    /// A member that is not static has no group instance ID.
    /// That is `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let instance = GroupInstanceId::new("etl-0")?;
    /// assert_eq!(instance, "etl-0");
    /// assert!(GroupInstanceId::new("etl 0").is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    GroupInstanceId, "group instance ID", max: text::MAX_NAME_LEN, rule: text::name
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_group_id_takes_any_text_but_none() {
        for value in ["analytics-etl", "analytics etl/é", "."] {
            assert_eq!(GroupId::new(value).unwrap().as_str(), value);
        }
        assert_eq!(GroupId::new(""), Err(KernelError::Empty { what: "group ID" }));
    }

    #[test]
    fn a_group_id_fits_a_kafka_string() {
        assert_eq!(GroupId::MAX_LEN, 32_767);
        assert!(GroupId::new("a".repeat(32_767)).is_ok());
        assert_eq!(
            GroupId::new("a".repeat(32_768)),
            Err(KernelError::TooLong {
                what: "group ID",
                len: 32_768,
                max: 32_767
            })
        );
    }

    #[test]
    fn a_group_id_converts_from_and_to_text() {
        let group = GroupId::new("analytics-etl").unwrap();

        assert_eq!("analytics-etl".parse::<GroupId>(), Ok(group.clone()));
        assert_eq!(GroupId::try_from("analytics-etl"), Ok(group.clone()));
        assert_eq!(GroupId::try_from(String::from("analytics-etl")), Ok(group.clone()));
        assert_eq!(String::from(group.clone()), "analytics-etl");
        assert_eq!(group.to_string(), "analytics-etl");
        assert_eq!(AsRef::<str>::as_ref(&group), "analytics-etl");
        assert_eq!(group, "analytics-etl");
        assert!(GroupId::try_from("").is_err());
        assert!(GroupId::try_from(String::new()).is_err());
        assert!("".parse::<GroupId>().is_err());
    }

    #[test]
    fn a_map_finds_a_group_id_by_text() {
        let members = BTreeMap::from([(GroupId::new("analytics-etl").unwrap(), 3)]);

        assert_eq!(members.get("analytics-etl"), Some(&3));
    }

    #[test]
    fn a_member_id_takes_any_text_but_none() {
        assert_eq!(MemberId::new("consumer-1-4f2a").unwrap().as_str(), "consumer-1-4f2a");
        assert_eq!(MemberId::new(""), Err(KernelError::Empty { what: "member ID" }));
        assert_eq!(MemberId::MAX_LEN, 32_767);
        assert_eq!(
            MemberId::new("a".repeat(32_768)),
            Err(KernelError::TooLong {
                what: "member ID",
                len: 32_768,
                max: 32_767
            })
        );
    }

    #[test]
    fn a_member_id_converts_from_and_to_text() {
        let member = MemberId::new("consumer-1-4f2a").unwrap();

        assert_eq!("consumer-1-4f2a".parse::<MemberId>(), Ok(member.clone()));
        assert_eq!(MemberId::try_from("consumer-1-4f2a"), Ok(member.clone()));
        assert_eq!(MemberId::try_from(String::from("consumer-1-4f2a")), Ok(member.clone()));
        assert_eq!(String::from(member.clone()), "consumer-1-4f2a");
        assert_eq!(member.to_string(), "consumer-1-4f2a");
        assert_eq!(member, "consumer-1-4f2a");
        assert!(MemberId::try_from("").is_err());
    }

    #[test]
    fn the_wire_writes_no_member_id_as_an_empty_string() {
        let member = MemberId::new("consumer-1-4f2a").unwrap();

        assert_eq!(MemberId::from_wire(""), Ok(None));
        assert_eq!(MemberId::from_wire("consumer-1-4f2a"), Ok(Some(member.clone())));
        assert_eq!(
            MemberId::from_wire(&"a".repeat(32_768)),
            Err(KernelError::TooLong {
                what: "member ID",
                len: 32_768,
                max: 32_767
            })
        );
        assert_eq!(MemberId::to_wire(None), "");
        assert_eq!(MemberId::to_wire(Some(&member)), "consumer-1-4f2a");
    }

    #[test]
    fn a_group_instance_id_follows_the_rule_of_a_topic_name() {
        assert_eq!(GroupInstanceId::new("etl-0").unwrap().as_str(), "etl-0");
        assert_eq!(GroupInstanceId::MAX_LEN, 249);
        assert!(GroupInstanceId::new("a".repeat(249)).is_ok());
        assert_eq!(
            GroupInstanceId::new("a".repeat(250)),
            Err(KernelError::TooLong {
                what: "group instance ID",
                len: 250,
                max: 249
            })
        );
        assert_eq!(
            GroupInstanceId::new(""),
            Err(KernelError::Empty {
                what: "group instance ID"
            })
        );
        assert_eq!(
            GroupInstanceId::new("."),
            Err(KernelError::Reserved {
                what: "group instance ID",
                value: ".".into()
            })
        );
        assert_eq!(
            GroupInstanceId::new("etl 0"),
            Err(KernelError::InvalidCharacter {
                what: "group instance ID",
                character: ' ',
                position: 3
            })
        );
    }

    #[test]
    fn a_group_instance_id_converts_from_and_to_text() {
        let instance = GroupInstanceId::new("etl-0").unwrap();

        assert_eq!("etl-0".parse::<GroupInstanceId>(), Ok(instance.clone()));
        assert_eq!(GroupInstanceId::try_from("etl-0"), Ok(instance.clone()));
        assert_eq!(GroupInstanceId::try_from(String::from("etl-0")), Ok(instance.clone()));
        assert_eq!(String::from(instance.clone()), "etl-0");
        assert_eq!(instance.to_string(), "etl-0");
        assert_eq!(instance, "etl-0");
        assert!(GroupInstanceId::try_from("etl/0").is_err());
    }
}
