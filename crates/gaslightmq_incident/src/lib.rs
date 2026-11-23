//! The incident vocabulary: what changes the truth, what follows from it, and what a backend can do.
//!
//! | Type | Is |
//! |---|---|
//! | [`IncidentKind`] | Which of the thirteen incidents, without parameters. |
//! | [`IncidentDefinition`] | An incident as a rule's action or a preset writes it. It may leave its subject to the call. |
//! | [`Subjects`] | What a call is about: the partitions, topics, broker, groups and transactional IDs it offers. |
//! | [`Incident`] | A change to the truth, with everything it needs to happen. |
//! | [`Coordinator`] | Whose coordinator: the one of a group, or the one of a transactional producer. |
//! | [`PartitionCount`], [`RecordCount`] | How many partitions or records an incident adds. |
//! | [`Topology`] | What an expansion asks the cluster: who leads what, and who hosts which coordinator. |
//! | [`Consequence`] | What an incident brings about besides its own effect. |
//! | [`ExpandedIncident`] | An incident with its consequences. |
//! | [`Support`] | What a backend does with a kind of incident: apply it, emulate it, or neither. |
//! | [`IncidentSupport`] | The support a backend declares for every kind. |
//! | [`Requirements`], [`Requirement`] | The kinds of incidents a scenario needs, and why. |
//! | [`UnsupportedIncidents`] | The incidents a scenario needs and its backend cannot do. |
//!
//! A type that checks its value has an error of its own, named after it.
//! The error holds only what can go wrong with that value.
//!
//! | Error | Says |
//! |---|---|
//! | [`IncidentKindError`] | Why a text is not an [`IncidentKind`]: no incident has that name. |
//! | [`IncidentError`] | Why an [`IncidentDefinition`] does not become an [`Incident`]: it names no subject, and the call offers none. |
//! | [`PartitionCountError`], [`RecordCountError`] | Why a number is not a [`PartitionCount`] or a [`RecordCount`]: it is zero, or too high. |
//!
//! [`UnsupportedIncidents`] is an error too: [`IncidentSupport::check`] returns it.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! The [`prelude`] holds every type of both tables.
//!
//! # From a definition to a change
//!
//! An incident passes through three types on its way from a file to the cluster.
//!
//! ```
//! use gaslightmq_incident::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//!
//! // A rule's action: "the broker of this call goes down, and its partitions fail over".
//! let action = IncidentDefinition::BrokerGoesDown { broker: None, failover: true };
//!
//! // The scenario loads only on a backend that can do the incident and its consequences.
//! let backend = IncidentSupport::none().applies(IncidentKind::ALL);
//! backend.check(&Requirements::from(&action)).expect("the backend applies every incident");
//!
//! // The rule fires on a call to broker 2.
//! let call = Subjects::new().with_broker(BrokerId::from(2));
//! let incidents = action.resolve(&call).expect("the call names its broker");
//! assert_eq!(incidents, [Incident::BrokerGoesDown { broker: BrokerId::from(2), failover: true }]);
//!
//! // The cluster says what broker 2 leads and hosts, and the incident gets its consequences.
//! struct Cluster;
//!
//! impl Topology for Cluster {
//!     fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
//!         vec!["orders.eu/3".parse().unwrap()]
//!     }
//!
//!     fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
//!         vec![]
//!     }
//! }
//!
//! for incident in incidents {
//!     let expanded = incident.expand(&Cluster);
//!     assert_eq!(
//!         expanded.consequences(),
//!         [Consequence::Incident(Incident::LeaderMoves { partition: "orders.eu/3".parse()?, to: None })]
//!     );
//! }
//! # Ok::<(), TopicPartitionError>(())
//! ```
//!
//! # The `serde` feature
//!
//! With the `serde` feature, a definition reads and writes the map a rule or a preset holds.
//! The `incident` key names the kind, and the other keys are the parameters:
//!
//! ```yaml
//! { incident: leader-moves, partition: orders.eu/3, to: 2 }
//! { incident: broker-goes-down, broker: 2, failover: true }
//! { incident: coordinator-moves, group: payments }
//! { incident: partition-added, topic: orders.eu, count: 2 }
//! { incident: records-expired, partition: orders.eu/3, to: 5000 }
//! { incident: committed-offset-reset, group: payments, partition: orders.eu/3, to: 0 }
//! { incident: producer-fenced, transactional.id: payments-tx-1 }
//! ```
//!
//! | Key | Holds | On |
//! |---|---|---|
//! | `partition` | The partition, such as `orders.eu/3`. | `leader-moves`, `records-appended`, `records-expired`, `log-truncated`, `committed-offset-reset` |
//! | `topic` | The topic. | `partition-added`, `topic-deleted` |
//! | `broker` | The broker ID. | `broker-goes-down`, `broker-goes-unresponsive`, `broker-comes-back` |
//! | `group` | The consumer group. | `rebalance`, `coordinator-moves`, `committed-offset-reset` |
//! | `transactional.id` | The transactional ID. | `coordinator-moves`, `producer-fenced` |
//! | `to` | The broker something moves to. Optional. | `leader-moves`, `coordinator-moves` |
//! | `to` | The offset something moves to. Required. | `records-expired`, `log-truncated`, `committed-offset-reset` |
//! | `count` | How many to add. One when left out. | `partition-added`, `records-appended` |
//! | `failover` | Whether leaders and coordinators move away. `false` when left out. | `broker-goes-down` |
//!
//! The keys `partition`, `topic`, `broker`, `group` and `transactional.id` name the subject.
//! A definition that leaves its subject out takes it from the call that fires the rule.
//! `coordinator-moves` takes `group` or `transactional.id`, and not both.
//!
//! Reading rejects a key that does not belong to the incident.
//! A misspelt `partition` would otherwise silently mean "the partition of the call".
//!
//! The other types write these forms:
//!
//! | Type | Form |
//! |---|---|
//! | [`IncidentKind`] | Its name, such as `leader-moves`. |
//! | [`Incident`] | The map of a definition that names every subject. |
//! | [`Consequence`] | `{ consequence: leaderless, partition: orders.eu/3 }`, `{ consequence: coordinator-unavailable, group: payments }`, or the map of the incident that follows. |
//! | [`ExpandedIncident`] | The map of the incident, with a `consequences` list when it has any. |
//! | [`Support`] | `applied`, `emulated` or `unsupported`. |
//! | [`IncidentSupport`] | A map of every kind to its support. Reading takes a kind that is left out as unsupported. |
//! | [`PartitionCount`], [`RecordCount`] | A number. |
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_kernel` alone.
//! It has no async code, no IO, no clock and no randomness, and it compiles to WebAssembly.

mod consequence;
mod count;
mod definition;
#[cfg(feature = "serde")]
mod form;
mod incident;
mod kind;
mod requirement;
mod support;

pub use consequence::{Consequence, ExpandedIncident, Topology};
pub use count::{PartitionCount, PartitionCountError, RecordCount, RecordCountError};
pub use definition::{IncidentDefinition, Subjects};
pub use incident::{Coordinator, Incident, IncidentError};
pub use kind::{IncidentKind, IncidentKindError};
pub use requirement::{Requirement, Requirements};
pub use support::{IncidentSupport, Support, UnsupportedIncidents};

/// Every type of this crate, and the error of each type that has one.
///
/// Import it whole wherever code speaks in incidents, next to the prelude of `gaslightmq_kernel`.
/// No name in it is a name of that prelude or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_incident::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let incident = Incident::LeaderMoves { partition: "orders.eu/3".parse()?, to: None };
/// assert_eq!(incident.kind(), IncidentKind::LeaderMoves);
/// # Ok::<(), TopicPartitionError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Consequence, Coordinator, ExpandedIncident, Incident, IncidentDefinition, IncidentError, IncidentKind,
        IncidentKindError, IncidentSupport, PartitionCount, PartitionCountError, RecordCount, RecordCountError,
        Requirement, Requirements, Subjects, Support, Topology, UnsupportedIncidents,
    };
}
