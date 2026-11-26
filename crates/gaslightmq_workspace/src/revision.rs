//! The revision of a definition, and the counter of the process that gives every revision.

gaslightmq_resource::revision! {
    /// A workspace keeps the revision of every definition it holds, in memory and beside the definition:
    /// no definition holds one, and no store does.
    /// A definition has the revision of the change that last created or updated it, and a definition that nobody
    /// changed since the workspace loaded has the revision of the load.
    ///
    /// ```
    /// use futures::executor::block_on;
    /// use gaslightmq_lie::prelude::*;
    /// use gaslightmq_rule::prelude::*;
    /// use gaslightmq_workspace::prelude::*;
    ///
    /// let mut workspace = Workspace::new(MemoryContext::new());
    /// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
    ///
    /// block_on(async {
    ///     let created = workspace.create(blackhole.clone()).await?;
    ///     assert_eq!(workspace.revision(blackhole.name().clone()), created);
    ///     // The load was an earlier change, so it has a lower revision.
    ///     assert!(created > workspace.loaded());
    ///     Ok::<(), CreateError>(())
    /// })?;
    /// # Ok::<(), CreateError>(())
    /// ```
    Revision, RevisionCounter
}
