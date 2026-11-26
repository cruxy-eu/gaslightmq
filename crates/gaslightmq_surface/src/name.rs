gaslightmq_resource::name! {
    /// A pad layout is a definition of the workspace, and its name is its identity:
    /// the control plane lists it by this name, and a [`Pad`](crate::Pad) names its layout by it.
    LayoutName, LayoutNameError, "layout name"
}

gaslightmq_resource::title! {
    /// A layout can have a title, such as *Order desk*, which a surface shows where it has one.
    LayoutTitle, LayoutTitleError, "layout title"
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
