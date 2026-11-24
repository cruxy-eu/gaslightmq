gaslightmq_resource::uuid! {
    /// It tells the environments of a workspace apart, whatever their names are.
    /// An [`Environment`](crate::Environment) has it as an optional part, next to its name:
    /// an environment has none until the workspace gives it one.
    EnvironmentUuid, EnvironmentUuidError, "environment UUID"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time someone created the environment of [`STAGING_PROXY`], in milliseconds since the Unix epoch.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of [`STAGING_PROXY`].
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
    /// The UUID of [`CREATED`] and [`RANDOM`], as a file holds it.
    const STAGING_PROXY: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

    #[test]
    fn a_uuid_is_its_time_and_its_random_bits() {
        let uuid = EnvironmentUuid::new(CREATED, RANDOM);

        assert_eq!(uuid.to_string(), STAGING_PROXY);
        assert_eq!(STAGING_PROXY.parse::<EnvironmentUuid>(), Ok(uuid));
        assert_eq!(EnvironmentUuid::try_from(STAGING_PROXY), Ok(uuid));
        assert_eq!(EnvironmentUuid::try_from(STAGING_PROXY.to_owned()), Ok(uuid));
        assert_eq!(String::from(uuid), STAGING_PROXY);
    }

    #[test]
    fn uuids_sort_in_the_order_their_environments_were_created() {
        assert!(EnvironmentUuid::new(CREATED, RANDOM) < EnvironmentUuid::new(CREATED + 1, RANDOM));
    }

    #[test]
    fn a_text_that_is_no_uuid_of_version_7_is_an_error() {
        // A name, and a UUID of version 4.
        for text in ["staging-proxy", "550e8400-e29b-41d4-a716-446655440000"] {
            let error = text.parse::<EnvironmentUuid>().unwrap_err();

            assert_eq!(error, EnvironmentUuidError { input: text.to_owned() });
            assert_eq!(
                error.to_string(),
                format!("environment UUID \"{text}\" is no UUID of version 7")
            );
        }
    }

    #[test]
    fn a_uuid_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", EnvironmentUuid::new(CREATED, RANDOM)),
            format!("EnvironmentUuid({STAGING_PROXY})")
        );
    }
}
