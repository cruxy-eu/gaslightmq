//! The names and the titles of a scenario and of its steps.

gaslightmq_resource::name! {
    /// A scenario is a definition of the workspace, and its name is its identity:
    /// the control plane lists it by this name, and someone activates it by it.
    ScenarioName, ScenarioNameError, "scenario name"
}

gaslightmq_resource::title! {
    /// A scenario can have a title, such as *Leader election*, which says what failure story it tells.
    ScenarioTitle, ScenarioTitleError, "scenario title"
}

gaslightmq_resource::name! {
    /// A step is part of its scenario, and its name is unique among the steps of that scenario.
    /// The journal and the UI name a step by it.
    StepName, StepNameError, "step name"
}

gaslightmq_resource::title! {
    /// A step can have a title, such as *Slow recovery of the producer*, which the UI shows where it has one.
    StepTitle, StepTitleError, "step title"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_and_a_title_keep_their_text() {
        let scenario = ScenarioName::new("leader-election").unwrap();
        let step = StepName::new("warm-up").unwrap();

        assert_eq!(scenario, "leader-election");
        assert_eq!(format!("{scenario:?}"), "ScenarioName(\"leader-election\")");
        assert_eq!(step.to_string(), "warm-up");
        assert_eq!(
            ScenarioTitle::new("Leader election").unwrap().as_str(),
            "Leader election"
        );
        assert_eq!(StepTitle::new("Warm up").unwrap().as_str(), "Warm up");
    }

    #[test]
    fn a_text_that_breaks_the_rule_for_names_or_for_titles_is_the_error_of_its_kind() {
        assert_eq!(ScenarioName::new(""), Err(ScenarioNameError::Empty));
        assert_eq!(
            StepName::new("Warm up").unwrap_err().to_string(),
            "step name \"Warm up\" holds 'W' at position 0, which is no lowercase letter, digit or hyphen"
        );
        assert_eq!(ScenarioTitle::new(""), Err(ScenarioTitleError::Empty));
        assert_eq!(
            StepTitle::new(" Warm up").unwrap_err().to_string(),
            "step title \" Warm up\" starts or ends with white space"
        );
    }
}
