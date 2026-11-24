gaslightmq_resource::name!(EnvironmentName, EnvironmentNameError, "environment name");

gaslightmq_resource::name! {
    /// The broker of an environment names the world it answers from.
    /// An environment only names a world.
    /// Whether a world of that name exists is for whoever holds the world definitions to check.
    WorldName, WorldNameError, "world name"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["staging-proxy", "x", "7", "orders-sim", "shop-eu-2"] {
            let name = EnvironmentName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(text.parse::<EnvironmentName>(), Ok(name.clone()));
            assert_eq!(EnvironmentName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        let input = |text: &str| text.to_owned();
        let long = "a".repeat(EnvironmentName::MAX_LENGTH + 1);

        assert_eq!(EnvironmentName::new(""), Err(EnvironmentNameError::Empty));
        assert_eq!(
            EnvironmentName::new("Staging"),
            Err(EnvironmentNameError::InvalidCharacter {
                input: input("Staging"),
                character: 'S',
                position: 0
            })
        );
        assert_eq!(
            EnvironmentName::new("orders sim"),
            Err(EnvironmentNameError::InvalidCharacter {
                input: input("orders sim"),
                character: ' ',
                position: 6
            })
        );
        assert_eq!(
            EnvironmentName::new("shop-"),
            Err(EnvironmentNameError::SurroundingHyphen { input: input("shop-") })
        );
        assert_eq!(
            EnvironmentName::new(&long),
            Err(EnvironmentNameError::TooLong { length: 64 })
        );
        assert_eq!(
            WorldName::new("shop (EU)"),
            Err(WorldNameError::InvalidCharacter {
                input: input("shop (EU)"),
                character: ' ',
                position: 4
            })
        );
        assert_eq!(
            WorldName::new("-shop"),
            Err(WorldNameError::SurroundingHyphen { input: input("-shop") })
        );
    }

    #[test]
    fn the_error_says_whose_name_breaks_which_part_of_the_rule() {
        let environment = |text: &str| EnvironmentName::new(text).unwrap_err().to_string();
        let world = |text: &str| WorldName::new(text).unwrap_err().to_string();
        let long = "a".repeat(WorldName::MAX_LENGTH + 1);

        assert_eq!(environment(""), "environment name \"\" is empty");
        assert_eq!(
            environment("Staging"),
            "environment name \"Staging\" holds 'S' at position 0, which is no lowercase letter, digit or hyphen"
        );
        assert_eq!(
            environment("shop-"),
            "environment name \"shop-\" starts or ends with a hyphen"
        );
        assert_eq!(world(&long), "world name of 64 characters is longer than 63 characters");
        assert_eq!(world(""), "world name \"\" is empty");
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", EnvironmentName::new("shop").unwrap()),
            "EnvironmentName(\"shop\")"
        );
        assert_eq!(format!("{:?}", WorldName::new("shop").unwrap()), "WorldName(\"shop\")");
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(EnvironmentName::new("shop").unwrap(), 1);

        assert_eq!(by_name.get("shop"), Some(&1));
        assert_eq!(by_name.get("staging"), None);
    }
}
