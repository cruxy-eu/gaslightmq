gaslightmq_resource::name! {
    /// A pad layout is a definition of the workspace, and its name is its identity:
    /// the control plane lists it by this name, and a [`Pad`](crate::Pad) names its layout by it.
    LayoutName, LayoutNameError, "layout name"
}

gaslightmq_resource::title! {
    /// A layout can have a title, such as *Order desk*, which a surface shows where it has one.
    LayoutTitle, LayoutTitleError, "layout title"
}

gaslightmq_resource::uuid! {
    /// It tells one layout from every other layout that a process holds, or held, in one run.
    /// It is live state: the workspace gives a layout its UUID when it first holds it, and keeps it in memory,
    /// next to the name. No [`PadLayout`](crate::PadLayout) and no file holds one.
    LayoutUuid, LayoutUuidError, "layout UUID"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time someone created the layout of [`ORDER_DESK`], in milliseconds since the Unix epoch.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of [`ORDER_DESK`].
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
    /// The UUID of [`CREATED`] and [`RANDOM`], as text.
    const ORDER_DESK: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

    #[test]
    fn a_name_and_a_title_keep_their_text() {
        let name = LayoutName::new("order-desk").unwrap();
        let title = LayoutTitle::new("Order desk").unwrap();

        assert_eq!(name, "order-desk");
        assert_eq!(name.to_string(), "order-desk");
        assert_eq!("order-desk".parse::<LayoutName>(), Ok(name.clone()));
        assert_eq!(format!("{name:?}"), "LayoutName(\"order-desk\")");
        assert_eq!(title.as_str(), "Order desk");
        assert_eq!("Order desk".parse::<LayoutTitle>(), Ok(title));
    }

    #[test]
    fn a_text_that_breaks_the_rule_for_names_or_for_titles_is_the_error_of_its_kind() {
        assert_eq!(LayoutName::new(""), Err(LayoutNameError::Empty));
        assert_eq!(
            LayoutName::new("Order desk").unwrap_err().to_string(),
            "layout name \"Order desk\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen"
        );
        assert_eq!(LayoutTitle::new(""), Err(LayoutTitleError::Empty));
        assert_eq!(
            LayoutTitle::new("Order desk ").unwrap_err().to_string(),
            "layout title \"Order desk \" starts or ends with white space"
        );
    }

    #[test]
    fn a_uuid_is_its_time_and_its_random_bits() {
        let uuid = LayoutUuid::new(CREATED, RANDOM);

        assert_eq!(uuid.to_string(), ORDER_DESK);
        assert_eq!(ORDER_DESK.parse::<LayoutUuid>(), Ok(uuid));
        assert_eq!(LayoutUuid::try_from(ORDER_DESK), Ok(uuid));
        assert_eq!(String::from(uuid), ORDER_DESK);
        assert_eq!(format!("{uuid:?}"), format!("LayoutUuid({ORDER_DESK})"));
        assert!(uuid < LayoutUuid::new(CREATED + 1, RANDOM));
    }

    #[test]
    fn a_text_that_is_no_uuid_of_version_7_is_the_error_of_its_kind() {
        // A name, and a UUID of version 4.
        for text in ["order-desk", "550e8400-e29b-41d4-a716-446655440000"] {
            let error = text.parse::<LayoutUuid>().unwrap_err();

            assert_eq!(error, LayoutUuidError { input: text.to_owned() });
            assert_eq!(
                error.to_string(),
                format!("layout UUID \"{text}\" is no UUID of version 7")
            );
        }
    }
}
