use std::fmt;

use gaslightmq_rule::RuleState;

use crate::{Layer, LayoutName};

/// Define the place of a line of the grid: a row or a column.
macro_rules! grid_line {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal
    ) => {
        $(#[$meta])*
        ///
        /// Code counts from zero, as a grid does: [`from_index`](Self::from_index).
        /// A person counts from one, and the text does too.
        /// Any index is a value: the layout says whether it has a line there.
        ///
        /// With the `serde` feature, it serializes as its index.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(transparent))]
        pub struct $name(usize);

        impl $name {
            #[doc = concat!("The ", $what, " at `index`, counted from zero.")]
            pub const fn from_index(index: usize) -> Self {
                Self(index)
            }

            #[doc = concat!("Where the ", $what, " stands, counted from zero.")]
            pub const fn index(self) -> usize {
                self.0
            }
        }

        #[doc = concat!("Writes the ", $what, " as a person counts it: `", $what, " 1` is the first.")]
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {}", $what, self.0.saturating_add(1))
            }
        }
    };
}

grid_line! {
    /// A row of the pad grid: the place of one selector of a layout.
    ///
    /// ```
    /// use gaslightmq_surface::prelude::*;
    ///
    /// let first = Row::from_index(0);
    /// assert_eq!(first.index(), 0);
    /// assert_eq!(first.to_string(), "row 1");
    /// ```
    Row, "row"
}

grid_line! {
    /// A column of the pad grid: the place of one preset of a layout.
    ///
    /// ```
    /// use gaslightmq_surface::prelude::*;
    ///
    /// let fourth = Column::from_index(3);
    /// assert_eq!(fourth.index(), 3);
    /// assert_eq!(fourth.to_string(), "column 4");
    /// ```
    Column, "column"
}

/// A pad: the intersection of a row, a column and a layer of one layout.
///
/// A pad is a position, and not a rule.
/// It points at zero or one rule: the rule that has [the name of its position](crate::PadLayout::rule_name), if there is one.
/// Moving a row or a column of the layout changes no rule.
///
/// A pad names its layout and says its layer, so it says where it is without a surface next to it.
/// It checks nothing: [`PadLayout::holds`](crate::PadLayout::holds) says whether the layout has such a pad.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let pad = Pad::new(
///     "order-desk".parse().expect("a layout name"),
///     "order-ingest".parse()?,
///     Row::from_index(0),
///     Column::from_index(3),
/// );
/// assert_eq!(pad.to_string(), "row 1, column 4 on layer order-ingest of layout order-desk");
/// # Ok::<(), LayerError>(())
/// ```
///
/// With the `serde` feature, a pad serializes as a map: `{ layout: order-desk, layer: order-ingest, row: 0, column: 3 }`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Pad {
    layout: LayoutName,
    layer: Layer,
    row: Row,
    column: Column,
}

impl Pad {
    /// The pad at that row and column, on that layer of that layout.
    pub const fn new(layout: LayoutName, layer: Layer, row: Row, column: Column) -> Self {
        Self {
            layout,
            layer,
            row,
            column,
        }
    }

    /// The layout the pad is a position of.
    pub const fn layout(&self) -> &LayoutName {
        &self.layout
    }

    /// The layer the pad is on.
    pub const fn layer(&self) -> &Layer {
        &self.layer
    }

    /// The row of the pad.
    pub const fn row(&self) -> Row {
        self.row
    }

    /// The column of the pad.
    pub const fn column(&self) -> Column {
        self.column
    }
}

/// Writes where the pad is, as a person reads it: `row 1, column 4 on layer order-ingest of layout order-desk`.
impl fmt::Display for Pad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, {} on layer {} of layout {}",
            self.row, self.column, self.layer, self.layout
        )
    }
}

/// What a pad shows: whether it points at a rule, and whether that rule is armed.
///
/// A surface holds no rule.
/// Whoever shows the pads reads the rule of each pad from what the engine publishes,
/// and tells the surface what the pad shows when someone presses it.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// assert_eq!(PadState::of(None), PadState::Empty);
/// assert_eq!(PadState::of(Some(RuleState::Armed)), PadState::Armed);
/// // A rule that is not armed can be armed, whether someone still writes it or disarmed it.
/// assert_eq!(PadState::of(Some(RuleState::Draft)), PadState::Disarmed);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PadState {
    /// The pad points at no rule.
    Empty,
    /// The pad points at a rule that is not armed.
    Disarmed,
    /// The pad points at an armed rule.
    Armed,
}

impl PadState {
    /// What a pad shows that points at a rule in that state, or at no rule.
    pub const fn of(rule: Option<RuleState>) -> Self {
        match rule {
            None => PadState::Empty,
            Some(RuleState::Armed) => PadState::Armed,
            Some(RuleState::Draft | RuleState::Disarmed) => PadState::Disarmed,
        }
    }

    /// Whether the pad points at an armed rule.
    pub const fn is_armed(self) -> bool {
        matches!(self, PadState::Armed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pad() -> Pad {
        Pad::new(
            "order-desk".parse().unwrap(),
            "order-ingest".parse().unwrap(),
            Row::from_index(0),
            Column::from_index(3),
        )
    }

    #[test]
    fn a_row_and_a_column_count_from_zero_and_read_from_one() {
        assert_eq!(Row::from_index(7).index(), 7);
        assert_eq!(Row::from_index(7).to_string(), "row 8");
        assert_eq!(Column::from_index(0).to_string(), "column 1");
        assert!(Row::from_index(0) < Row::from_index(1));
        assert_eq!(Row::from_index(usize::MAX).to_string(), format!("row {}", usize::MAX));
    }

    #[test]
    fn a_pad_says_where_it_is() {
        let pad = pad();

        assert_eq!(pad.layout(), "order-desk");
        assert_eq!(pad.layer().to_string(), "order-ingest");
        assert_eq!(pad.row(), Row::from_index(0));
        assert_eq!(pad.column(), Column::from_index(3));
        assert_eq!(
            pad.to_string(),
            "row 1, column 4 on layer order-ingest of layout order-desk"
        );
    }

    #[test]
    fn a_pad_shows_whether_it_has_a_rule_and_whether_the_rule_is_armed() {
        assert_eq!(PadState::of(None), PadState::Empty);
        assert_eq!(PadState::of(Some(RuleState::Draft)), PadState::Disarmed);
        assert_eq!(PadState::of(Some(RuleState::Disarmed)), PadState::Disarmed);
        assert_eq!(PadState::of(Some(RuleState::Armed)), PadState::Armed);
        assert!(PadState::Armed.is_armed());
        assert!(!PadState::Empty.is_armed() && !PadState::Disarmed.is_armed());
    }
}
