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
        for text in ["staging-proxy", "x", "Ünïcode ✓", "orders sim", "shop (EU)"] {
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

        assert_eq!(EnvironmentName::new(""), Err(EnvironmentNameError::Empty));
        assert_eq!(
            EnvironmentName::new(" shop"),
            Err(EnvironmentNameError::SurroundingWhiteSpace { input: input(" shop") })
        );
        assert_eq!(
            EnvironmentName::new("shop\n"),
            Err(EnvironmentNameError::SurroundingWhiteSpace { input: input("shop\n") })
        );
        assert_eq!(
            EnvironmentName::new("orders\tsim"),
            Err(EnvironmentNameError::ControlCharacter {
                input: input("orders\tsim")
            })
        );
        assert_eq!(
            EnvironmentName::new("orders\u{200b}sim"),
            Err(EnvironmentNameError::FormatCharacter {
                input: input("orders\u{200b}sim")
            })
        );
        assert_eq!(
            WorldName::new("\u{feff}shop"),
            Err(WorldNameError::FormatCharacter {
                input: input("\u{feff}shop")
            })
        );
    }

    #[test]
    fn the_error_says_whose_name_breaks_which_part_of_the_rule() {
        let environment = |text: &str| EnvironmentName::new(text).unwrap_err().to_string();
        let world = |text: &str| WorldName::new(text).unwrap_err().to_string();

        assert_eq!(environment(""), "environment name \"\" is empty");
        assert_eq!(
            environment("shop "),
            "environment name \"shop \" starts or ends with white space"
        );
        assert_eq!(
            environment("orders\nsim"),
            "environment name \"orders\\nsim\" holds a control character"
        );
        assert_eq!(
            world("orders\u{2060}sim"),
            "world name \"orders\\u{2060}sim\" holds a format character"
        );
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
    fn a_name_is_exact() {
        assert_ne!(
            EnvironmentName::new("Staging").unwrap(),
            EnvironmentName::new("staging").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(EnvironmentName::new("shop").unwrap(), 1);

        assert_eq!(by_name.get("shop"), Some(&1));
        assert_eq!(by_name.get("staging"), None);
    }
}
