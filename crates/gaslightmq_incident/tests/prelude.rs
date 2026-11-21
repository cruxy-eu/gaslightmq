//! The prelude holds every type, and its names clash with none of `std` or of the kernel's prelude.

/// Imports the prelude next to the standard prelude, the kernel's prelude
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_and_the_kernel {
    #![allow(unused_imports)]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;

    struct Empty;

    impl Topology for Empty {
        fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
            vec![]
        }

        fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
            vec![]
        }
    }

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let definition = IncidentDefinition::PartitionAdded {
            topic: None,
            count: PartitionCount::ONE,
        };
        let missing: IncidentError = definition.resolve(&Subjects::new()).unwrap_err();
        let incident = Incident::RecordsAppended {
            partition: "orders.eu/3".parse().unwrap(),
            count: RecordCount::ONE,
        };
        let expanded: ExpandedIncident = incident.clone().expand(&Empty);
        let consequences: &[Consequence] = expanded.consequences();
        let requirements = Requirements::from(&definition);
        let requirement: Requirement = requirements.iter().next().unwrap();
        let unsupported: UnsupportedIncidents = IncidentSupport::none().check(&requirements).unwrap_err();
        let coordinator = Coordinator::Group(GroupId::new("payments").unwrap());

        vec![
            IncidentKind::LeaderMoves.to_string(),
            Support::Emulated.to_string(),
            missing.to_string(),
            unsupported.to_string(),
            format!("{incident:?} {consequences:?} {requirement:?} {coordinator:?}"),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_and_the_kernel() {
    let items = next_to_std_and_the_kernel::every_item();
    assert_eq!(items[0], "leader-moves");
    assert_eq!(items[1], "emulated");
    assert_eq!(items[2], "partition-added needs a topic, and none is named");
    assert_eq!(items[3], "the backend cannot apply or emulate: partition-added");
}
