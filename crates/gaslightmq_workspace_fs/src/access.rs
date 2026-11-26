//! Whether the stores of a workspace write back to its files.

/// Whether the stores of a workspace write back to its files.
///
/// In CI the workspace is often mounted read-only. Its stores then read it and read it again, and never write:
/// every create, update and delete fails, and changes nothing.
///
/// ```
/// use gaslightmq_workspace_fs::prelude::*;
///
/// assert_eq!(Access::default(), Access::ReadWrite);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Access {
    /// The stores read the files, and write every change back to them.
    #[default]
    ReadWrite,
    /// The stores read the files, and refuse every change.
    ReadOnly,
}
