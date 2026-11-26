//! Revisions and runs, between the host and the control plane.
//!
//! The host and the control plane each have a revision and a run ID of their own type, which mean the same:
//! a revision is its number on both sides, and a run ID is its 128 bits.

use gaslightmq_control_api::prelude::*;

/// The revision of the host, as the control plane hands it out.
pub(crate) const fn revision(of_the_host: gaslightmq_workspace::Revision) -> Revision {
    Revision::new(of_the_host.number())
}

/// What a change answers with: the revision that the host says it left.
pub(crate) const fn revised(left: gaslightmq_workspace::Revision) -> Revised {
    Revised {
        revision: revision(left),
    }
}

/// The run of the host, as the control plane hands it out.
pub(crate) const fn run(of_the_host: gaslightmq_host::RunId) -> RunId {
    RunId::from_random_bits(of_the_host.bits())
}

/// What a change of the control plane expects, as the host takes it.
pub(crate) fn expected(expects: Option<Expects>) -> Option<gaslightmq_host::Expects> {
    expects.map(|Expects { run, revision }| gaslightmq_host::Expects {
        run: gaslightmq_host::RunId::from_random_bits(run.bits()),
        revision: gaslightmq_workspace::Revision::new(revision.number()),
    })
}
