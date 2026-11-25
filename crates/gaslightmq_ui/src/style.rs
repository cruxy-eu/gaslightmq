//! The Tailwind classes that several components share, each a whole class list.
//!
//! Every class names a token of the theme in `tailwind.css` by its role, so the same markup serves the dark and the
//! light theme. Tailwind reads the classes from this file as it is written, so a class list is always whole, and
//! never built from pieces.

/// A button with a border, on a raised surface.
pub const BUTTON: &str = "inline-flex h-6.5 shrink-0 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-md border border-edge-strong bg-raised px-2.25 text-small font-medium text-body hover:bg-raised-hover hover:text-strong disabled:cursor-default disabled:opacity-40";

/// The button that does what the view is for.
pub const BUTTON_PRIMARY: &str = "inline-flex h-6.5 shrink-0 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-md border border-edge-strong bg-primary px-2.25 text-small font-medium text-on-primary hover:bg-primary-hover";

/// A button without a border, until the pointer is on it.
pub const BUTTON_GHOST: &str = "inline-flex h-6.5 shrink-0 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-md border border-transparent px-2.25 text-small font-medium text-muted hover:bg-hover hover:text-strong";

/// A button without a border that does something that cannot be taken back.
pub const BUTTON_DANGER: &str = "inline-flex h-6.5 shrink-0 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-md border border-transparent px-2.25 text-small font-medium text-muted hover:bg-hover hover:text-danger";

/// A square button with an icon alone.
pub const ICON_BUTTON: &str =
    "grid size-6.5 shrink-0 cursor-pointer place-items-center rounded-md text-muted hover:bg-hover hover:text-strong";

/// A key of the keyboard, as a hint.
pub const KEY: &str = "inline-flex h-4.25 items-center rounded border border-edge-strong px-1 font-mono text-micro font-medium text-muted";

/// A group of buttons of which one is on.
pub const SEGMENTS: &str = "inline-flex shrink-0 flex-wrap gap-0.5 rounded-md border border-edge bg-inset p-0.5";

/// A button of a group, while it is off.
pub const SEGMENT: &str = "inline-flex h-5.5 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded px-2.25 text-small text-muted hover:text-strong";

/// A button of a group, while it is on.
pub const SEGMENT_ON: &str = "inline-flex h-5.5 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded bg-control px-2.25 text-small text-strong ring-1 ring-edge ring-inset";

/// A tab of a view, while it is off.
pub const TAB: &str = "inline-flex h-6.5 cursor-pointer items-center gap-1.5 rounded-md border border-transparent px-2.5 text-compact text-muted hover:text-strong";

/// A tab of a view, while it is on.
pub const TAB_ON: &str = "inline-flex h-6.5 cursor-pointer items-center gap-1.5 rounded-md border border-edge-strong bg-raised px-2.5 text-compact text-strong";

/// The header of a view.
pub const HEADER: &str = "flex h-11 shrink-0 items-center gap-2 border-b border-edge pr-3 pl-4";

/// The title of a view, in its header.
pub const CRUMB: &str = "flex items-center gap-1.75 whitespace-nowrap font-medium text-strong";

/// What a view says while it has nothing to show.
pub const EMPTY: &str = "flex flex-col items-center gap-3.5 px-6 py-12 text-center text-muted";

/// A short note.
pub const NOTE: &str = "text-caption leading-normal text-subtle";

/// The header of a pane, which stays on top while the pane scrolls.
pub const PANE_HEADER: &str = "sticky top-0 z-3 flex h-8.5 items-center gap-2 whitespace-nowrap border-b border-edge bg-panel px-3.5 text-small font-medium text-muted";

/// The header of a pane on the inset surface, such as an inspector.
pub const PANE_HEADER_INSET: &str = "sticky top-0 z-3 flex h-8.5 items-center gap-2 whitespace-nowrap border-b border-edge bg-inset px-3.5 text-small font-medium text-muted";

/// A small label on a soft surface.
pub const TAG: &str = "inline-flex h-5 shrink-0 items-center gap-1.25 whitespace-nowrap rounded-md bg-selected px-1.75 text-caption text-body";

/// A condition, as a chip.
pub const CHIP: &str =
    "inline-flex h-6 shrink-0 items-stretch overflow-hidden rounded-md border border-edge-strong bg-raised text-small";

/// The field of a condition, in its chip.
pub const CHIP_FIELD: &str =
    "flex items-center whitespace-nowrap border-r border-edge px-1.75 font-mono text-caption text-body";

/// The operator of a condition, in its chip.
pub const CHIP_OPERATOR: &str = "flex items-center whitespace-nowrap border-r border-edge px-1.75 text-subtle";

/// What a condition tests, in its chip.
pub const CHIP_VALUE: &str = "flex items-center whitespace-nowrap px-1.75 text-strong";

/// The button that takes a condition away, in its chip.
pub const CHIP_REMOVE: &str =
    "flex cursor-pointer items-center border-l border-edge px-1.25 text-subtle hover:bg-hover hover:text-strong";

/// A value that filters the traffic when someone clicks it.
pub const FILTER_VALUE: &str = "cursor-pointer rounded underline-offset-3 hover:text-strong hover:underline";

/// A value that the filter holds.
pub const FILTER_VALUE_ON: &str = "cursor-pointer rounded text-accent-text underline-offset-3 hover:underline";

/// A label and a value, side by side, in an inspector.
pub const PROPERTY: &str = "flex min-h-8 items-start gap-2.5 py-0.5";

/// The label of a property.
pub const PROPERTY_LABEL: &str = "w-19 shrink-0 pt-1.5 text-compact text-muted";

/// The values of a property, in a row.
pub const PROPERTY_VALUES: &str = "flex min-h-7.5 min-w-0 flex-1 flex-wrap items-center gap-1.5";

/// The values of a property, one under the other.
pub const PROPERTY_COLUMN: &str = "flex min-h-7.5 min-w-0 flex-1 flex-col items-start gap-1.5";

/// A line between two parts of an inspector.
pub const DIVIDER: &str = "my-3.5 h-px bg-edge";

/// A menu over the view.
pub const MENU: &str = "absolute bottom-full left-0 z-60 mb-1.5 min-w-65 rounded-lg border border-edge-strong bg-popover p-1 shadow-xl shadow-scrim";

/// A line of a menu.
pub const MENU_ITEM: &str = "flex min-h-7.5 cursor-pointer items-center gap-2.25 rounded-md px-2.25 py-1 text-compact text-body hover:bg-selected hover:text-strong";

/// The title of a part of a menu.
pub const MENU_SECTION: &str = "px-2.25 pt-1.5 pb-0.75 text-tiny text-subtle";

/// A switch that is on.
pub const SWITCH_ON: &str =
    "relative block h-3.75 w-6.5 cursor-pointer rounded-full border border-edge bg-primary transition";

/// A switch that is off.
pub const SWITCH_OFF: &str =
    "relative block h-3.75 w-6.5 cursor-pointer rounded-full border border-edge bg-control transition";

/// The knob of a switch that is on.
pub const SWITCH_KNOB_ON: &str = "absolute top-px left-3 size-2.75 rounded-full bg-on-primary transition-all";

/// The knob of a switch that is off.
pub const SWITCH_KNOB_OFF: &str = "absolute top-px left-px size-2.75 rounded-full bg-muted transition-all";
