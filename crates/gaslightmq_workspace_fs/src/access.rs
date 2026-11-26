//! Whether the stores of a workspace write back to its files, and when.

/// Whether the stores of a workspace write back to its files, and when.
///
/// By default every change of a definition goes to its file as it happens.
/// A workspace can also hold its changes until someone saves them, and write them all in one go.
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
    /// The stores read the files, and write every change back to them as it happens.
    #[default]
    ReadWrite,
    /// The stores read the files, and hold every change in memory until someone saves:
    /// the save writes every file that a change touched, or none of them when someone changed one since.
    WriteOnSave,
    /// The stores read the files, and refuse every change.
    ReadOnly,
}
