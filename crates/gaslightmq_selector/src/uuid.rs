gaslightmq_resource::uuid! {
    /// It tells one selector from every other selector that a process holds, or held, in one run.
    /// It is live state: the workspace gives a selector its UUID when it first holds it, and keeps it in memory,
    /// next to the name. No [`Selector`](crate::Selector) and no file holds one.
    SelectorUuid, SelectorUuidError, "selector UUID"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time someone created the selector of [`ORDER_PRODUCERS`], in milliseconds since the Unix epoch.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of [`ORDER_PRODUCERS`].
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
    /// The UUID of [`CREATED`] and [`RANDOM`], as text.
    const ORDER_PRODUCERS: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

    #[test]
    fn a_uuid_is_its_time_and_its_random_bits() {
        let uuid = SelectorUuid::new(CREATED, RANDOM);

        assert_eq!(uuid.to_string(), ORDER_PRODUCERS);
        assert_eq!(ORDER_PRODUCERS.parse::<SelectorUuid>(), Ok(uuid));
        assert_eq!(SelectorUuid::try_from(ORDER_PRODUCERS), Ok(uuid));
        assert_eq!(SelectorUuid::try_from(ORDER_PRODUCERS.to_owned()), Ok(uuid));
        assert_eq!(String::from(uuid), ORDER_PRODUCERS);
    }

    #[test]
    fn uuids_sort_in_the_order_their_selectors_were_created() {
        assert!(SelectorUuid::new(CREATED, RANDOM) < SelectorUuid::new(CREATED + 1, RANDOM));
    }

    #[test]
    fn a_text_that_is_no_uuid_of_version_7_is_an_error() {
        // A name, and a UUID of version 4.
        for text in ["order-producers", "550e8400-e29b-41d4-a716-446655440000"] {
            let error = text.parse::<SelectorUuid>().unwrap_err();

            assert_eq!(error, SelectorUuidError { input: text.to_owned() });
            assert_eq!(
                error.to_string(),
                format!("selector UUID \"{text}\" is no UUID of version 7")
            );
        }
    }

    #[test]
    fn a_uuid_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", SelectorUuid::new(CREATED, RANDOM)),
            format!("SelectorUuid({ORDER_PRODUCERS})")
        );
    }
}
