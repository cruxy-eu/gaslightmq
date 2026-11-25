//! The pads that someone plays in the UI: a surface on a layout of the workspace, against the rules of the
//! environment shown.

use gaslightmq_control_api::lie::LieDefinition;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Action, ActionRef, Preset, Rule};
use gaslightmq_control_api::surface::{
    Column, Encoder, EncoderTurn, Layer, PadLayout, PadState, PlayMode, Row, Steps, Surface,
};
use gaslightmq_control_api::trigger::{Lifetime, Trigger};

/// The pads someone plays: one [`Surface`] on a layout of the workspace, and what the pads of the environment shown
/// point at.
///
/// A pad is a position, and the rule it points at is the rule that has the name of the pad in the environment.
/// A press sends an intent, and the environment makes the live rule of an empty pad.
/// The desk never makes a rule itself, and never holds the state of one: it reads both from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Desk {
    surface: Surface,
}

/// A way to move the focus across the grid, as the arrow keys do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Move {
    Up,
    Down,
    Left,
    Right,
}

impl Desk {
    /// A desk that plays `layout`, on its first layer, in latch.
    pub fn new(layout: PadLayout) -> Self {
        Self {
            surface: Surface::new(layout),
        }
    }

    /// The desk that plays one of `layouts`: the layout it played, as the workspace has it now,
    /// or the first layout when the one it played is gone. `None` when the workspace has no layout.
    pub fn following(desk: Option<Desk>, layouts: &[PadLayout]) -> Option<Desk> {
        let Some(mut desk) = desk else {
            return layouts.first().cloned().map(Desk::new);
        };
        match layouts.iter().find(|layout| layout.name() == desk.layout().name()) {
            Some(layout) if layout == desk.layout() => Some(desk),
            Some(layout) => {
                desk.surface.show(layout.clone());
                Some(desk)
            }
            None => layouts.first().cloned().map(Desk::new),
        }
    }

    /// The surface: the layout, the layer it shows, its mode and its focus.
    pub fn surface(&self) -> &Surface {
        &self.surface
    }

    /// The layout the desk plays.
    pub fn layout(&self) -> &PadLayout {
        self.surface.layout()
    }

    /// The desk plays another layout of the workspace, on its first layer.
    pub fn play(&mut self, layout: PadLayout) {
        self.surface = Surface::new(layout);
    }

    /// The rule that the pad at that row and column points at in the environment, on the layer shown.
    pub fn rule<'a>(&self, row: Row, column: Column, running: &'a RunningEnvironment) -> Option<&'a RuleStatus> {
        let pad = self.surface.pad(row, column)?;
        let name = self.layout().rule_name(&pad)?;
        running.rules.iter().find(|status| status.rule.name() == &name)
    }

    /// What the pad at that row and column shows: no rule, a rule that is not armed, or an armed rule.
    pub fn state(&self, row: Row, column: Column, running: &RunningEnvironment) -> PadState {
        PadState::of(self.rule(row, column, running).map(|status| status.state))
    }

    /// Whether a pad at that row and column has an armed rule on another layer than the one shown.
    pub fn armed_elsewhere(&self, row: Row, column: Column, running: &RunningEnvironment) -> bool {
        let layout = self.layout();
        layout
            .layers()
            .iter()
            .filter(|layer| *layer != self.surface.layer())
            .filter_map(|layer| layout.pad(layer, row, column))
            .filter_map(|pad| layout.rule_name(&pad))
            .any(|name| {
                running
                    .rules
                    .iter()
                    .any(|status| status.rule.name() == &name && status.state.is_armed())
            })
    }

    /// Someone pressed the pad at that row and column: the intent to send to the environment, if any.
    pub fn press(&mut self, row: Row, column: Column, running: &RunningEnvironment) -> Option<SendIntent> {
        let shows = self.state(row, column, running);
        let intent = self.surface.press(row, column, shows)?;
        Some(SendIntent {
            environment: running.environment.clone(),
            intent,
        })
    }

    /// Someone released the pad at that row and column: in hold, the intent that disarms its rule.
    pub fn release(&mut self, row: Row, column: Column, running: &RunningEnvironment) -> Option<SendIntent> {
        let intent = self.surface.release(row, column)?;
        Some(SendIntent {
            environment: running.environment.clone(),
            intent,
        })
    }

    /// Whether the encoder turns the rule behind the focused pad: the pad has a live rule, and the encoder changes a
    /// parameter that the rule's action has. A rule that someone kept changes in the workspace, so it refuses turns.
    pub fn turns(&self, encoder: Encoder, running: &RunningEnvironment, presets: &[Preset]) -> bool {
        let Some(pad) = self.surface.focused() else {
            return false;
        };
        let Some(status) = self.rule(pad.row(), pad.column(), running) else {
            return false;
        };
        status.live && action(&status.rule, presets).is_some_and(|action| encoder.applies_to(&action))
    }

    /// Someone turned an encoder `steps` steps: the intent for the rule behind the focused pad, when the encoder
    /// [turns](Self::turns) it.
    pub fn turn(
        &self,
        encoder: Encoder,
        steps: Steps,
        running: &RunningEnvironment,
        presets: &[Preset],
    ) -> Option<SendIntent> {
        if !self.turns(encoder, running, presets) {
            return None;
        }
        let intent = self.surface.turn(EncoderTurn::new(encoder, steps))?;
        Some(SendIntent {
            environment: running.environment.clone(),
            intent,
        })
    }

    /// The desk shows another layer of its layout. Returns whether the layout has that layer.
    pub fn switch_to(&mut self, layer: &Layer) -> bool {
        self.surface.switch_to(layer)
    }

    /// The desk plays in latch or in hold from here on.
    pub fn set_mode(&mut self, mode: PlayMode) {
        self.surface.set_mode(mode);
    }

    /// The pad at that row and column takes the focus, so the encoders tune its rule.
    pub fn focus(&mut self, row: Row, column: Column) {
        self.surface.focus(row, column);
    }

    /// The focus moves one pad, and stays at the edge of the grid. With no focus, the first pad takes it.
    pub fn move_focus(&mut self, direction: Move) {
        let layout = self.surface.layout();
        let (rows, columns) = (layout.rows().len(), layout.columns().len());
        if rows == 0 || columns == 0 {
            return;
        }
        let (row, column) = self
            .surface
            .focused()
            .map_or((0, 0), |pad| (pad.row().index(), pad.column().index()));
        let (row, column) = match direction {
            Move::Up => (row.saturating_sub(1), column),
            Move::Down => ((row + 1).min(rows - 1), column),
            Move::Left => (row, column.saturating_sub(1)),
            Move::Right => (row, (column + 1).min(columns - 1)),
        };
        self.surface.focus(Row::from_index(row), Column::from_index(column));
    }
}

/// The action of a rule: its own, or that of the preset it names.
pub fn action(rule: &Rule, presets: &[Preset]) -> Option<Action> {
    match rule.action() {
        ActionRef::Inline(action) => Some(action.clone()),
        ActionRef::Preset(name) => presets
            .iter()
            .find(|preset| preset.name() == name)
            .map(|preset| preset.action().clone()),
    }
}

/// What the parameter of an encoder is on a rule now, in a few words, or `None` when the rule has no such parameter.
pub fn encoder_value(encoder: Encoder, rule: &Rule, presets: &[Preset]) -> Option<String> {
    let lie = action(rule, presets).and_then(|action| action.lie().cloned());
    let timing = match &lie {
        Some(LieDefinition::Timing(timing)) => Some(*timing),
        _ => None,
    };
    match encoder {
        Encoder::Probability => match rule.trigger() {
            Trigger::Manual => None,
            Trigger::Parts(parts) => Some(
                parts
                    .chance()
                    .map_or_else(|| "100%".to_owned(), |chance| chance.to_string()),
            ),
        },
        Encoder::Delay => timing.map(|timing| timing.delay().to_string()),
        Encoder::Jitter => timing.map(|timing| timing.jitter().to_string()),
        Encoder::Throttle => timing.map(|timing| timing.throttle().to_string()),
        Encoder::ErrorCode => match &lie {
            Some(LieDefinition::Refusal(refusal)) => Some(refusal.error.to_string()),
            _ => None,
        },
        Encoder::Every => rule.trigger().parts().map(|parts| {
            parts
                .every()
                .map_or_else(|| "each".to_owned(), |every| format!("every {}", every.value()))
        }),
        Encoder::Duration => Some(match rule.lifetime() {
            Lifetime::Duration(span) => span.to_string(),
            _ => "∞".to_owned(),
        }),
        Encoder::Firings => Some(match rule.lifetime() {
            Lifetime::Firings(count) => format!("{}×", count.value()),
            _ => "∞".to_owned(),
        }),
    }
}
