//! The `serde` form of a layout, of an encoder mapping and of what a surface sends, and the rules that reading applies.
//!
//! The form of a layout is the YAML of `docs/specs/domains/control-surfaces.md`, written here as JSON.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::fmt::Debug;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A value next to the JSON of a file or of a message.
struct Form<T>(T);

impl<T: Serialize + DeserializeOwned + PartialEq + Debug> Form<T> {
    /// `json` is what the value writes, and what reads back as the same value.
    fn writes(&self, json: &str) {
        assert_eq!(serde_json::to_string(&self.0).unwrap(), json);
        self.reads(json);
    }

    /// `json` reads as the value, although the value writes something else.
    fn reads(&self, json: &str) {
        assert_eq!(serde_json::from_str::<T>(json).unwrap(), self.0, "{json}");
    }

    /// Reading `json` as a value of this type fails, and the message holds `reason`.
    fn rejects(json: &str, reason: &str) {
        let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
        assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
    }
}

/// The pad that the spec presses: row 1, column 4 on layer `order-ingest`, here of the layout `order-desk`.
fn pad() -> Pad {
    Pad::new(
        "order-desk".parse().unwrap(),
        "order-ingest".parse().unwrap(),
        Row::from_index(0),
        Column::from_index(3),
    )
}

/// A UUID, which a layout does not hold.
const ORDER_DESK_UUID: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

/// The JSON of [`pad`].
const PAD: &str = r#"{"layout":"order-desk","layer":"order-ingest","row":0,"column":3}"#;

// A layout

#[test]
fn a_layout_writes_its_name_its_title_its_rows_its_columns_and_its_layers() {
    // name: order-desk
    // title: Order desk
    // rows: [order-producers, fetch, metadata]
    // columns: [error-retriable, delay-1s]
    // layers: [All clients, order-ingest, payments-svc]
    let layout = PadLayout::new(
        "order-desk".parse().unwrap(),
        ["order-producers", "fetch", "metadata"].map(|selector| selector.parse().unwrap()),
        ["error-retriable", "delay-1s"].map(|preset| preset.parse().unwrap()),
        ["All clients", "order-ingest", "payments-svc"].map(|layer| layer.parse().unwrap()),
    )
    .unwrap()
    .with_title("Order desk".parse().unwrap());

    Form(layout).writes(concat!(
        r#"{"name":"order-desk","title":"Order desk","#,
        r#""rows":["order-producers","fetch","metadata"],"#,
        r#""columns":["error-retriable","delay-1s"],"#,
        r#""layers":["All clients","order-ingest","payments-svc"]}"#,
    ));
}

#[test]
fn a_layout_without_a_title_writes_none_and_one_without_rows_and_columns_writes_empty_lists() {
    let layout = PadLayout::new("empty".parse().unwrap(), [], [], [Layer::all_clients()]).unwrap();

    Form(layout).writes(r#"{"name":"empty","rows":[],"columns":[],"layers":["All clients"]}"#);
}

#[test]
fn reading_a_layout_rejects_what_a_layout_never_holds() {
    let rejects = Form::<PadLayout>::rejects;

    rejects(
        r#"{"name":"order-desk","rows":[],"columns":[],"layers":[]}"#,
        "layout \"order-desk\" has no layer",
    );
    rejects(
        r#"{"name":"order-desk","rows":["fetch","fetch"],"columns":[],"layers":["All clients"]}"#,
        "layout \"order-desk\" has two rows for selector \"fetch\"",
    );
    rejects(
        r#"{"name":"order-desk","rows":[],"columns":["delay-1s","delay-1s"],"layers":["All clients"]}"#,
        "layout \"order-desk\" has two columns for preset \"delay-1s\"",
    );
    rejects(
        r#"{"name":"order-desk","rows":[],"columns":[],"layers":["kafka-ui",{"client.id":"kafka-ui"}]}"#,
        "layout \"order-desk\" has the layer \"kafka-ui\" twice",
    );
    rejects(
        r#"{"name":"","rows":[],"columns":[],"layers":["All clients"]}"#,
        "layout name \"\" is empty",
    );
    rejects(
        r#"{"name":"Order desk","rows":[],"columns":[],"layers":["All clients"]}"#,
        "layout name \"Order desk\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen",
    );
    rejects(
        r#"{"name":"order-desk","title":" Order desk","rows":[],"columns":[],"layers":["All clients"]}"#,
        "layout title \" Order desk\" starts or ends with white space",
    );
    // No file holds the UUID of a layout: `uuid` is a key that a layout does not have.
    rejects(
        &format!(
            r#"{{"name":"order-desk","uuid":"{ORDER_DESK_UUID}","rows":[],"columns":[],"layers":["All clients"]}}"#
        ),
        "unknown field `uuid`",
    );
    rejects(
        r#"{"name":"order-desk","rows":["fetch-"],"columns":[],"layers":["All clients"]}"#,
        "selector name \"fetch-\" starts or ends with a hyphen",
    );
}

#[test]
fn reading_a_layout_needs_every_key_but_the_title_and_rejects_one_it_does_not_know() {
    let rejects = Form::<PadLayout>::rejects;

    rejects(
        r#"{"name":"order-desk","rows":[],"columns":[]}"#,
        "missing field `layers`",
    );
    rejects(
        r#"{"name":"order-desk","columns":[],"layers":["All clients"]}"#,
        "missing field `rows`",
    );
    rejects(
        r#"{"name":"order-desk","rows":[],"columns":[],"layers":["All clients"],"encoders":8}"#,
        "unknown field `encoders`",
    );
}

// A layer

#[test]
fn the_layer_of_all_clients_and_the_layer_of_a_client_write_their_text() {
    Form(Layer::all_clients()).writes(r#""All clients""#);
    Form(Layer::client(ClientId::new("order-ingest").unwrap())).writes(r#""order-ingest""#);
    // A `client.id` is no name, so it is any text.
    Form(Layer::client(ClientId::new("Order Ingest (EU)").unwrap())).writes(r#""Order Ingest (EU)""#);
}

#[test]
fn a_layer_of_any_other_scope_writes_the_map_of_its_scope() {
    // - { endpoint: broker 2 }
    let second_broker = Scope::endpoint(Endpoint::Broker(BrokerId::from(2)));
    Form(Layer::new(second_broker)).writes(r#"{"endpoint":"broker 2"}"#);

    // A client whose `client.id` is the text of the layer of all clients.
    let confusing = ClientId::new(Layer::ALL_CLIENTS).unwrap();
    Form(Layer::client(confusing)).writes(r#"{"client.id":"All clients"}"#);
}

#[test]
fn a_map_that_says_what_a_text_says_reads_as_the_same_layer() {
    Form(Layer::all_clients()).reads("{}");
    Form("order-ingest".parse::<Layer>().unwrap()).reads(r#"{"client.id":"order-ingest"}"#);
}

#[test]
fn reading_a_layer_rejects_what_is_no_layer() {
    let rejects = Form::<Layer>::rejects;

    rejects(
        "7",
        "expected `All clients`, the `client.id` of a client, or the map of a scope",
    );
    rejects(
        r#"{"topic":"orders.*"}"#,
        "a scope only tests connection fields, and topic is a request field",
    );
}

// A pad

#[test]
fn a_pad_writes_its_layout_its_layer_and_where_it_stands_counted_from_zero() {
    Form(pad()).writes(PAD);
    Form(Row::from_index(7)).writes("7");
    Form(Column::from_index(0)).writes("0");
}

#[test]
fn a_pad_on_a_layer_without_a_text_writes_the_scope_of_its_layer() {
    let second_broker = Layer::new(Scope::endpoint(Endpoint::Broker(BrokerId::from(2))));
    let pad = Pad::new(
        "order-desk".parse().unwrap(),
        second_broker,
        Row::from_index(0),
        Column::from_index(3),
    );

    Form(pad).writes(r#"{"layout":"order-desk","layer":{"endpoint":"broker 2"},"row":0,"column":3}"#);
}

#[test]
fn reading_a_pad_rejects_a_position_before_the_first_and_a_key_it_does_not_know() {
    let rejects = Form::<Pad>::rejects;

    rejects(
        r#"{"layout":"order-desk","layer":"order-ingest","row":-1,"column":3}"#,
        "invalid value: integer `-1`",
    );
    rejects(
        r#"{"layout":"order-desk","layer":"order-ingest","row":0}"#,
        "missing field `column`",
    );
    rejects(
        r#"{"layout":"order-desk","layer":"order-ingest","row":0,"column":3,"rule":"Flap"}"#,
        "unknown field `rule`",
    );
    rejects(
        r#"{"layout":"","layer":"order-ingest","row":0,"column":3}"#,
        "layout name \"\" is empty",
    );
}

// An encoder and a mode of play

#[test]
fn an_encoder_and_a_mode_of_play_write_their_names() {
    for encoder in Encoder::ALL {
        Form(encoder).writes(&format!("{:?}", encoder.name()));
    }
    for mode in PlayMode::ALL {
        Form(mode).writes(&format!("{:?}", mode.name()));
    }

    Form::<Encoder>::rejects(r#""volume""#, "no encoder is called \"volume\"");
    Form::<PlayMode>::rejects(r#""toggle""#, "no mode of play is called \"toggle\"");
}

#[test]
fn a_turn_writes_its_encoder_and_its_steps() {
    Form(Steps::new(-2)).writes("-2");
    Form(EncoderTurn::new(Encoder::Probability, Steps::new(3))).writes(r#"{"encoder":"probability","steps":3}"#);

    let rejects = Form::<EncoderTurn>::rejects;
    rejects(r#"{"encoder":"probability"}"#, "missing field `steps`");
    rejects(
        r#"{"encoder":"probability","steps":1.5}"#,
        "invalid type: floating point `1.5`",
    );
    rejects(
        r#"{"encoder":"probability","steps":3,"value":43}"#,
        "unknown field `value`",
    );
}

// An intent

#[test]
fn an_intent_writes_what_it_asks_for_and_the_pad_it_is_about() {
    Form(Intent::Arm { pad: pad() }).writes(&format!(r#"{{"intent":"arm","pad":{PAD}}}"#));
    Form(Intent::Disarm { pad: pad() }).writes(&format!(r#"{{"intent":"disarm","pad":{PAD}}}"#));
    Form(Intent::Hold { pad: pad() }).writes(&format!(r#"{{"intent":"hold","pad":{PAD}}}"#));
    Form(Intent::Release {
        pad: pad(),
        hold: Hold::new(7),
    })
    .writes(&format!(r#"{{"intent":"release","pad":{PAD},"hold":7}}"#));

    let turn = EncoderTurn::new(Encoder::Probability, Steps::new(3));
    Form(Intent::Turn { pad: pad(), turn }).writes(&format!(
        r#"{{"intent":"turn","pad":{PAD},"turn":{{"encoder":"probability","steps":3}}}}"#
    ));
}

// A presence

#[test]
fn a_presence_id_writes_its_32_hexadecimal_digits_and_reads_no_other_text() {
    let presence = PresenceId::from_random_bits(0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015);

    Form(presence).writes(r#""9f86d081884c7d659a2feaa0c55ad015""#);
    Form::<PresenceId>::rejects(
        r#""tab-7""#,
        "a presence ID has 32 hexadecimal digits, and not 5 characters",
    );
    Form::<PresenceId>::rejects("7", "invalid type: integer `7`");
}

#[test]
fn an_intent_reads_its_keys_in_any_order() {
    Form(Intent::Arm { pad: pad() }).reads(&format!(r#"{{"pad":{PAD},"intent":"arm"}}"#));
}

#[test]
fn reading_an_intent_rejects_what_no_surface_sends() {
    let rejects = Form::<Intent>::rejects;

    rejects(&format!(r#"{{"intent":"mute","pad":{PAD}}}"#), "unknown variant `mute`");
    rejects(&format!(r#"{{"pad":{PAD}}}"#), "missing field `intent`");
    rejects(r#"{"intent":"arm"}"#, "missing field `pad`");
    rejects(&format!(r#"{{"intent":"turn","pad":{PAD}}}"#), "missing field `turn`");
    // A release names the hold that its press added.
    rejects(
        &format!(r#"{{"intent":"release","pad":{PAD}}}"#),
        "missing field `hold`",
    );
    rejects(
        &format!(r#"{{"intent":"release","pad":{PAD},"hold":-1}}"#),
        "invalid value: integer `-1`",
    );
    // A press says the state it wants, and never that the pad toggles.
    rejects(
        &format!(r#"{{"intent":"toggle","pad":{PAD}}}"#),
        "unknown variant `toggle`",
    );
    // A surface says the state it wants by the intent, and never in a key of its own.
    rejects(
        &format!(r#"{{"intent":"arm","pad":{PAD},"state":"armed"}}"#),
        "unknown field `state`",
    );
}

// An encoder mapping

#[test]
fn an_encoder_mapping_writes_every_property() {
    let mapping = EncoderMapping::new().with_errors([ErrorCode::NOT_LEADER_OR_FOLLOWER, ErrorCode::REQUEST_TIMED_OUT]);

    Form(mapping).writes(concat!(
        r#"{"probability_step":"1%","delay_step":"50ms","jitter_step":"25ms","throttle_step":"100ms","#,
        r#""every_step":1,"duration_step":"5s","firings_step":1,"#,
        r#""errors":["NOT_LEADER_OR_FOLLOWER","REQUEST_TIMED_OUT"]}"#,
    ));
}

#[test]
fn an_encoder_mapping_reads_the_default_of_a_property_that_is_left_out() {
    Form(EncoderMapping::new()).reads("{}");
    Form(EncoderMapping::new().with_delay_step("1s".parse().unwrap())).reads(r#"{"delay_step":"1s"}"#);
}

#[test]
fn an_encoder_mapping_reads_an_error_once_where_it_stands_first() {
    let listed = [ErrorCode::REQUEST_TIMED_OUT, ErrorCode::NOT_LEADER_OR_FOLLOWER];

    Form(EncoderMapping::new().with_errors(listed))
        .reads(r#"{"errors":["REQUEST_TIMED_OUT","NOT_LEADER_OR_FOLLOWER","REQUEST_TIMED_OUT"]}"#);
}

#[test]
fn reading_an_encoder_mapping_rejects_a_step_that_is_none_and_a_key_it_does_not_know() {
    let rejects = Form::<EncoderMapping>::rejects;

    rejects(
        r#"{"probability_step":"140%"}"#,
        "chance `140%` is not between 0% and 100%",
    );
    rejects(r#"{"every_step":0}"#, "a count is at least one");
    rejects(r#"{"delay_step":50}"#, "invalid type: integer `50`");
    rejects(r#"{"volume_step":"1%"}"#, "unknown field `volume_step`");
}
