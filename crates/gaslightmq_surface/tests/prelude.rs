//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the preludes of the crates whose types a surface speaks in.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_surface::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// The UUID that a workspace gave the layout `order-desk`.
    const ORDER_DESK_UUID: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let layout_name: LayoutName = LayoutName::new("order-desk").unwrap();
        let layout_uuid: LayoutUuid = ORDER_DESK_UUID.parse().unwrap();
        let layout_title: LayoutTitle = LayoutTitle::new("Order desk").unwrap();
        let layer: Layer = Layer::client(ClientId::new("order-ingest").unwrap());
        let row: Row = Row::from_index(0);
        let column: Column = Column::from_index(3);
        let layout: PadLayout = PadLayout::new(
            layout_name.clone(),
            [SelectorName::new("order-producers").unwrap()],
            ["error-retriable", "error-fatal", "delay-100ms", "delay-1s"].map(|preset| preset.parse().unwrap()),
            [layer.clone()],
        )
        .unwrap()
        .with_uuid(layout_uuid)
        .with_title(layout_title.clone());
        let pad: Pad = layout.pad(&layer, row, column).unwrap();
        let shows: PadState = PadState::of(None);
        let mode: PlayMode = PlayMode::Hold;
        let encoder: Encoder = Encoder::Probability;
        let steps: Steps = Steps::new(3);
        let turn: EncoderTurn = EncoderTurn::new(encoder, steps);
        let mapping: EncoderMapping = EncoderMapping::new();
        let mut surface: Surface = Surface::new(layout.clone());
        let arm: Intent = surface.press(row, column, shows).unwrap();
        let change: Intent = surface.turn(turn).unwrap();

        vec![
            layout_name.to_string(),
            layout_uuid.to_string(),
            layout_title.to_string(),
            layer.to_string(),
            row.to_string(),
            column.to_string(),
            layout.name().to_string(),
            pad.to_string(),
            format!("{shows:?}"),
            mode.to_string(),
            encoder.to_string(),
            steps.to_string(),
            turn.to_string(),
            mapping.delay_step().to_string(),
            surface.layer().to_string(),
            arm.to_string(),
            change.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let layout: LayoutName = "order-desk".parse().unwrap();
        let rule: RuleName = "leader-flap".parse().unwrap();
        vec![
            message_of(LayoutNameError::SurroundingHyphen {
                input: "order-desk-".to_owned(),
            }),
            message_of(LayoutUuidError {
                input: "order-desk".to_owned(),
            }),
            message_of(LayoutTitleError::Empty),
            message_of(LayerError::InvalidClient {
                error: ClientIdError::TooLong {
                    len: ClientId::MAX_LEN + 1,
                    max: ClientId::MAX_LEN,
                },
            }),
            message_of(PadLayoutError::NoLayer { layout: layout.clone() }),
            message_of(PadLayoutError::DuplicateLayer {
                layout: layout.clone(),
                layer: "kafka-ui".parse().unwrap(),
            }),
            message_of(PadLayoutError::DuplicateRow {
                layout: layout.clone(),
                selector: "fetch".parse().unwrap(),
            }),
            message_of(PadLayoutError::DuplicateColumn {
                layout,
                preset: "delay-1s".parse().unwrap(),
            }),
            message_of(EncoderError::Unknown {
                name: "volume".to_owned(),
            }),
            message_of(PlayModeError::Unknown {
                name: "toggle".to_owned(),
            }),
            message_of(TurnError::Disabled {
                encoder: Encoder::Jitter,
                rule: rule.clone(),
            }),
            message_of(TurnError::UnknownPreset {
                rule,
                preset: "delay-30s".parse().unwrap(),
            }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 17);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "layout name \"order-desk-\" starts or ends with a hyphen",
            "layout UUID \"order-desk\" is no UUID of version 7",
            "layout title \"\" is empty",
            "the layer of one client: client ID of 32768 bytes exceeds the limit of 32767",
            "layout \"order-desk\" has no layer",
            "layout \"order-desk\" has the layer \"kafka-ui\" twice",
            "layout \"order-desk\" has two rows for selector \"fetch\"",
            "layout \"order-desk\" has two columns for preset \"delay-1s\"",
            "no encoder is called \"volume\"",
            "no mode of play is called \"toggle\"",
            "the jitter encoder changes the jitter of a timing lie, and rule \"leader-flap\" has none",
            "rule \"leader-flap\": no preset is named \"delay-30s\"",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_play_a_pad() {
    use gaslightmq_surface::prelude::*;

    let layout = PadLayout::new(
        "order-desk".parse().unwrap(),
        ["order-producers".parse().unwrap()],
        ["delay-1s".parse().unwrap()],
        [Layer::all_clients()],
    )
    .unwrap();
    let mut surface = Surface::new(layout);
    let (row, column) = (Row::from_index(0), Column::from_index(0));

    let intent = surface.press(row, column, PadState::Empty).unwrap();

    assert_eq!(
        intent.to_string(),
        "arm the rule at row 1, column 1 on layer All clients of layout order-desk"
    );
}
