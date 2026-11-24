gaslightmq_resource::uuid! {
    /// It tells the rules of a workspace apart, and the live rules of an environment, whatever their names are.
    /// A [`Rule`](crate::Rule) has it as an optional part, next to its name:
    /// a rule has none until the workspace or the engine gives it one.
    RuleUuid, RuleUuidError, "rule UUID"
}

gaslightmq_resource::uuid! {
    /// It tells the presets of a workspace apart, whatever their names are.
    /// A [`Preset`](crate::Preset) has it as an optional part, next to its name:
    /// a preset has none until the workspace gives it one.
    PresetUuid, PresetUuidError, "preset UUID"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time someone created the rule of [`SLOW_ORDER_PRODUCERS`], in milliseconds since the Unix epoch.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of [`SLOW_ORDER_PRODUCERS`].
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
    /// The UUID of [`CREATED`] and [`RANDOM`], as a file holds it.
    const SLOW_ORDER_PRODUCERS: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

    #[test]
    fn a_uuid_is_its_time_and_its_random_bits() {
        let rule = RuleUuid::new(CREATED, RANDOM);
        let preset = PresetUuid::new(CREATED, RANDOM);

        assert_eq!(rule.to_string(), SLOW_ORDER_PRODUCERS);
        assert_eq!(SLOW_ORDER_PRODUCERS.parse::<RuleUuid>(), Ok(rule));
        assert_eq!(RuleUuid::try_from(SLOW_ORDER_PRODUCERS), Ok(rule));
        assert_eq!(RuleUuid::try_from(SLOW_ORDER_PRODUCERS.to_owned()), Ok(rule));
        assert_eq!(String::from(rule), SLOW_ORDER_PRODUCERS);
        assert_eq!(preset.to_string(), SLOW_ORDER_PRODUCERS);
        assert_eq!(SLOW_ORDER_PRODUCERS.parse::<PresetUuid>(), Ok(preset));
    }

    #[test]
    fn uuids_sort_in_the_order_their_rules_were_created() {
        assert!(RuleUuid::new(CREATED, RANDOM) < RuleUuid::new(CREATED + 1, RANDOM));
        assert!(PresetUuid::new(CREATED, RANDOM) < PresetUuid::new(CREATED + 1, RANDOM));
    }

    #[test]
    fn a_text_that_is_no_uuid_of_version_7_is_an_error() {
        // A name, and a UUID of version 4.
        for text in ["slow-order-producers", "550e8400-e29b-41d4-a716-446655440000"] {
            let rule = text.parse::<RuleUuid>().unwrap_err();
            let preset = text.parse::<PresetUuid>().unwrap_err();

            assert_eq!(rule, RuleUuidError { input: text.to_owned() });
            assert_eq!(
                rule.to_string(),
                format!("rule UUID \"{text}\" is no UUID of version 7")
            );
            assert_eq!(preset, PresetUuidError { input: text.to_owned() });
            assert_eq!(
                preset.to_string(),
                format!("preset UUID \"{text}\" is no UUID of version 7")
            );
        }
    }

    #[test]
    fn a_uuid_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", RuleUuid::new(CREATED, RANDOM)),
            format!("RuleUuid({SLOW_ORDER_PRODUCERS})")
        );
        assert_eq!(
            format!("{:?}", PresetUuid::new(CREATED, RANDOM)),
            format!("PresetUuid({SLOW_ORDER_PRODUCERS})")
        );
    }
}
