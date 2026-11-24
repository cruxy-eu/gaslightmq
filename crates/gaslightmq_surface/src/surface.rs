use crate::{Column, EncoderTurn, Intent, Layer, Pad, PadLayout, PadState, PlayMode, Row};

/// One control surface: a pad grid that someone plays by mouse or by keyboard.
///
/// A surface turns what someone does into [intents](Intent).
/// It takes what happened as a call, and returns what to send: it does no IO, and it holds no rule.
///
/// | It keeps | Starts as | Changes with |
/// |---|---|---|
/// | The [layout](PadLayout) it shows. | The one it is made with. | [`show`](Self::show) |
/// | The layer it shows, which is the page of the grid. | The first layer of the layout. | [`switch_to`](Self::switch_to) |
/// | The [mode of play](PlayMode). | Latch. | [`set_mode`](Self::set_mode) |
/// | The focused pad, which the encoders tune. | None. | [`focus`](Self::focus), and every press of a pad. |
///
/// A position without a pad is dead: a press there gives no intent.
/// A surface that draws a grid of a fixed size therefore passes on every press, whatever the size of the layout.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let layout = PadLayout::new(
///     "order-desk".parse().expect("a layout name"),
///     ["order-producers".parse().expect("a selector name")],
///     ["delay-1s".parse().expect("a preset name")],
///     [Layer::all_clients(), "order-ingest".parse().expect("a layer")],
/// )?;
/// let mut surface = Surface::new(layout);
/// let (row, column) = (Row::from_index(0), Column::from_index(0));
/// let pad = surface.pad(row, column).expect("the layout has this pad");
///
/// // A press of an empty pad arms it, and the engine creates the live rule.
/// let pressed = surface.press(row, column, PadState::Empty);
/// assert_eq!(pressed, Some(Intent::Arm { pad: pad.clone() }));
///
/// // The pad now shows an armed rule, and in latch a second press disarms it.
/// let pressed = surface.press(row, column, PadState::Armed);
/// assert_eq!(pressed, Some(Intent::Disarm { pad: pad.clone() }));
///
/// // The encoders tune the rule behind the pad that was pressed last.
/// let turn = EncoderTurn::new(Encoder::Probability, Steps::new(3));
/// assert_eq!(surface.turn(turn), Some(Intent::Turn { pad, turn }));
/// # Ok::<(), PadLayoutError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Surface {
    layout: PadLayout,
    layer: Layer,
    mode: PlayMode,
    focus: Option<(Row, Column)>,
    /// The pads that were pressed in hold and not released yet, in the order of their presses.
    held: Vec<Pad>,
}

impl Surface {
    /// A surface that shows the first layer of `layout`, in latch, with no pad focused.
    pub fn new(layout: PadLayout) -> Self {
        Self {
            layer: layout.first_layer().clone(),
            layout,
            mode: PlayMode::default(),
            focus: None,
            held: Vec::new(),
        }
    }

    /// Shows another layout, or the same one after someone changed it.
    ///
    /// The surface stays on its layer when the new layout has that layer, and else shows the first one.
    /// It keeps its focus when the new layout has a pad there.
    pub fn show(&mut self, layout: PadLayout) {
        if !layout.has_layer(&self.layer) {
            self.layer = layout.first_layer().clone();
        }
        self.layout = layout;
        if self.focused().is_none() {
            self.focus = None;
        }
    }

    /// The layout the surface shows.
    pub const fn layout(&self) -> &PadLayout {
        &self.layout
    }

    /// The layer the surface shows.
    pub const fn layer(&self) -> &Layer {
        &self.layer
    }

    /// Shows that layer. The focus stays at its position, on the new layer.
    ///
    /// Returns whether the layout has the layer. Without it, the surface stays on the layer it shows.
    pub fn switch_to(&mut self, layer: &Layer) -> bool {
        let known = self.layout.has_layer(layer);
        if known {
            self.layer = layer.clone();
        }
        known
    }

    /// The mode of play.
    pub const fn mode(&self) -> PlayMode {
        self.mode
    }

    /// Plays in that mode from the next press on.
    /// A pad that is held when the mode changes still disarms on its release.
    pub fn set_mode(&mut self, mode: PlayMode) {
        self.mode = mode;
    }

    /// The pad at that row and column on the layer the surface shows, or `None` when the layout has no pad there.
    pub fn pad(&self, row: Row, column: Column) -> Option<Pad> {
        self.layout.pad(&self.layer, row, column)
    }

    /// The pad whose rule the encoders tune, on the layer the surface shows.
    pub fn focused(&self) -> Option<Pad> {
        let (row, column) = self.focus?;
        self.pad(row, column)
    }

    /// Focuses the pad at that row and column without pressing it, and returns it.
    ///
    /// Returns `None` when the layout has no pad there, and the focus stays where it was.
    pub fn focus(&mut self, row: Row, column: Column) -> Option<Pad> {
        let pad = self.pad(row, column)?;
        self.focus = Some((row, column));
        Some(pad)
    }

    /// Someone pressed the pad at that row and column, which shows `shows`.
    ///
    /// The press focuses the pad.
    ///
    /// | Mode | The pad shows | Intent |
    /// |---|---|---|
    /// | Latch | No rule, or a rule that is not armed. | [`Intent::Arm`] |
    /// | Latch | An armed rule. | [`Intent::Disarm`] |
    /// | Hold | No rule, or a rule that is not armed. | [`Intent::Arm`] |
    /// | Hold | An armed rule. | None: the rule is armed already, and the release disarms it. |
    ///
    /// Returns `None` too when the layout has no pad there.
    pub fn press(&mut self, row: Row, column: Column, shows: PadState) -> Option<Intent> {
        let pad = self.focus(row, column)?;
        match self.mode {
            PlayMode::Latch if shows.is_armed() => Some(Intent::Disarm { pad }),
            PlayMode::Latch => Some(Intent::Arm { pad }),
            PlayMode::Hold => {
                if !self.held.contains(&pad) {
                    self.held.push(pad.clone());
                }
                (!shows.is_armed()).then_some(Intent::Arm { pad })
            }
        }
    }

    /// Someone released the pad at that row and column.
    ///
    /// A pad that was pressed in hold disarms: [`Intent::Disarm`] for the pad that was pressed,
    /// also when the surface shows another layer or plays in another mode by now.
    /// Any other release gives `None`.
    pub fn release(&mut self, row: Row, column: Column) -> Option<Intent> {
        let held = self
            .held
            .iter()
            .rposition(|pad| pad.row() == row && pad.column() == column)?;
        Some(Intent::Disarm {
            pad: self.held.remove(held),
        })
    }

    /// Someone turned an encoder.
    ///
    /// Returns [`Intent::Turn`] for the focused pad, or `None` when no pad is focused.
    /// The surface does not know the rule behind the pad, so it passes on a turn of every encoder:
    /// whoever shows the encoders disables the ones that [do not apply](crate::Encoder::applies_to).
    pub fn turn(&self, turn: EncoderTurn) -> Option<Intent> {
        self.focused().map(|pad| Intent::Turn { pad, turn })
    }
}
