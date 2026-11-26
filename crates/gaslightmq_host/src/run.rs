//! The run of a host, and what a change expects of what it changes.

use gaslightmq_workspace::Revision;

gaslightmq_resource::run_id! {
    /// A host holds the ID of its run, which it is handed when it is made: see [`Host::new`](crate::Host::new).
    /// Revisions start again with every run, so a change that names the revision it expects names its run too:
    /// see [`Expects`].
    ///
    /// ```
    /// use gaslightmq_host::prelude::*;
    ///
    /// // Whoever puts a process together reads the randomness of the system once, and hands the bits in.
    /// let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
    /// assert_eq!(run.to_string(), "4be0643f1d98573b97cdca98a65347dd");
    ///
    /// // A test names its own.
    /// let named: RunId = "4be0643f1d98573b97cdca98a65347dd".parse()?;
    /// assert_eq!(named, run);
    /// # Ok::<(), RunIdError>(())
    /// ```
    RunId, RunIdError
}

/// What a change expects of what it changes: the run that its sender read from, and the revision that it saw.
///
/// Several clients change one process, and each change can carry what its sender saw.
/// A host changes nothing for a change that expects another run than its own, or another revision than the thing
/// has now: someone else changed it since, or the process started again.
/// A change that carries none changes the thing as it is, so the last change wins.
///
/// | A change of | Expects the revision of |
/// |---|---|
/// | A definition: [`update`](crate::Host::update), [`delete`](crate::Host::delete) | The definition. |
/// | The state of a rule: [`arm`](crate::Host::arm), [`disarm`](crate::Host::disarm) | The rule in its environment. |
/// | A live rule: [`update_live_rule`](crate::Host::update_live_rule), [`delete_live_rule`](crate::Host::delete_live_rule), [`keep`](crate::Host::keep) | The rule in its environment. |
///
/// A create expects nothing: it refuses a name that is taken, so it never overwrites.
///
/// ```
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse()?;
/// // The revision comes from an answer of the host, or from what a client read.
/// let seen = RevisionCounter::FIRST;
///
/// let expects = Expects { run, revision: seen };
/// assert_eq!(expects.to_string(), "revision 1 of run 4be0643f1d98573b97cdca98a65347dd");
/// # Ok::<(), RunIdError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Expects {
    /// The run that the sender read from.
    pub run: RunId,
    /// The revision that the sender saw.
    pub revision: Revision,
}

/// Writes the revision and its run: `revision 7 of run 4be0643f1d98573b97cdca98a65347dd`.
impl std::fmt::Display for Expects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "revision {} of run {}", self.revision, self.run)
    }
}
