use crate::{Column, EncoderTurn, Hold, Intent, Layer, Pad, PadLayout, PadState, PlayMode, Row};

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
/// | The pads that someone holds, with the [`Hold`] of each. | None. | A press and a release in hold, and [`answered`](Self::answered). |
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
/// // A press of an empty pad wants it armed, and the engine creates the live rule.
/// let pressed = surface.press(row, column, PadState::Empty);
/// assert_eq!(pressed, Some(Intent::Arm { pad: pad.clone() }));
///
/// // The pad now shows an armed rule, and in latch a second press wants it disarmed.
/// let pressed = surface.press(row, column, PadState::Armed);
/// assert_eq!(pressed, Some(Intent::Disarm { pad: pad.clone() }));
///
/// // In hold, a press holds the pad, and its release ends the hold that the engine answered with.
/// surface.set_mode(PlayMode::Hold);
/// assert_eq!(surface.press(row, column, PadState::Disarmed), Some(Intent::Hold { pad: pad.clone() }));
/// assert_eq!(surface.answered(&pad, Some(Hold::new(1))), None);
/// assert_eq!(surface.release(row, column), Some(Intent::Release { pad: pad.clone(), hold: Hold::new(1) }));
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
    /// The pads that were pressed in hold, in the order of their presses, until their release went out.
    held: Vec<Held>,
}

/// A pad that someone pressed in hold, whose release has not gone out yet.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Held {
    pad: Pad,
    /// The hold that the press added, once its answer came.
    hold: Option<Hold>,
    /// Whether someone let go of the pad before the answer came, so the release goes out with it.
    released: bool,
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
    /// A pad that is held when the mode changes still ends its hold on its release.
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
    /// The press focuses the pad, and says the state that someone wants, from what the pad shows.
    ///
    /// | Mode | The pad shows | Intent |
    /// |---|---|---|
    /// | Latch | No rule, or a rule that is not armed. | [`Intent::Arm`] |
    /// | Latch | An armed rule. | [`Intent::Disarm`] |
    /// | Hold | Anything. | [`Intent::Hold`]: the rule stays armed while anyone holds it. |
    ///
    /// A press in hold holds the pad until its release. Whoever sends the intent hands its answer to
    /// [`answered`](Self::answered), so that the release names the [`Hold`] that the press added.
    ///
    /// Returns `None` when the layout has no pad there.
    pub fn press(&mut self, row: Row, column: Column, shows: PadState) -> Option<Intent> {
        let pad = self.focus(row, column)?;
        match self.mode {
            PlayMode::Latch if shows.is_armed() => Some(Intent::Disarm { pad }),
            PlayMode::Latch => Some(Intent::Arm { pad }),
            PlayMode::Hold => {
                self.held.push(Held {
                    pad: pad.clone(),
                    hold: None,
                    released: false,
                });
                Some(Intent::Hold { pad })
            }
        }
    }

    /// The answer to the [hold](Intent::Hold) of `pad` came: the [`Hold`] that the press added,
    /// or `None` when the press failed.
    ///
    /// Returns the [`Intent::Release`] of the hold when someone let go of the pad before the answer came,
    /// and when the surface holds no pad that waits for it. Else the hold waits for the release of its pad.
    pub fn answered(&mut self, pad: &Pad, hold: Option<Hold>) -> Option<Intent> {
        let waiting = self
            .held
            .iter()
            .position(|held| &held.pad == pad && held.hold.is_none());
        let Some(waiting) = waiting else {
            // Nobody holds the pad for this hold, so it ends at once.
            return hold.map(|hold| Intent::Release { pad: pad.clone(), hold });
        };
        let Some(hold) = hold else {
            self.held.remove(waiting);
            return None;
        };
        if self.held[waiting].released {
            self.held.remove(waiting);
            return Some(Intent::Release { pad: pad.clone(), hold });
        }
        self.held[waiting].hold = Some(hold);
        None
    }

    /// Someone released the pad at that row and column.
    ///
    /// A pad that was pressed in hold ends its hold: [`Intent::Release`] for the pad that was pressed,
    /// also when the surface shows another layer or plays in another mode by now.
    /// When the answer to its press has not come yet, the release goes out with [`answered`](Self::answered).
    /// Any other release gives `None`.
    pub fn release(&mut self, row: Row, column: Column) -> Option<Intent> {
        let pressed = self
            .held
            .iter()
            .rposition(|held| !held.released && held.pad.row() == row && held.pad.column() == column)?;
        let Some(hold) = self.held[pressed].hold else {
            self.held[pressed].released = true;
            return None;
        };
        let Held { pad, .. } = self.held.remove(pressed);
        Some(Intent::Release { pad, hold })
    }

    /// Whether someone holds a pad of the surface: they pressed it in hold, and did not let go of it yet.
    pub fn holding(&self) -> bool {
        self.held.iter().any(|held| !held.released)
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
