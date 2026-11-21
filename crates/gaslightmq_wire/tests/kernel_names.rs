//! Keeps the names in `gaslightmq_kernel` in step with the `kafka-protocol` crate.
//!
//! `gaslightmq_kernel` names every Kafka API and every Kafka error in `src/names.rs`.
//! Nobody types that file.
//! This test renders it from the API keys and the error codes of `kafka-protocol`,
//! and fails when the file in the repository differs from what it renders.
//! `gaslightmq_kernel` itself does not depend on `kafka-protocol`, so the check lives here.
//!
//! After an upgrade of `kafka-protocol`, write the file again:
//!
//! ```text
//! GASLIGHTMQ_UPDATE_NAMES=1 cargo test -p gaslightmq_wire --test kernel_names
//! ```
//!
//! Then run the test without the variable, and commit the file.

use std::fmt::Write;
use std::path::PathBuf;

use gaslightmq_kernel::prelude::*;
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::ApiKey as KafkaApiKey;

/// Set this environment variable to any value, and the test writes the file instead of checking it.
const UPDATE: &str = "GASLIGHTMQ_UPDATE_NAMES";

const COMMAND: &str = "GASLIGHTMQ_UPDATE_NAMES=1 cargo test -p gaslightmq_wire --test kernel_names";

/// Every API of `kafka-protocol`: its key, and its name as `kafka-protocol` spells it.
fn apis() -> Vec<(i16, String)> {
    KafkaApiKey::iter()
        .map(|key| (key as i16, format!("{key:?}")))
        .collect()
}

/// Every error of `kafka-protocol`: its code, and its name as `kafka-protocol` spells it.
///
/// `kafka-protocol` has no error for the code `0`, which means "no error".
fn errors() -> Vec<(i16, String)> {
    (i16::MIN..=i16::MAX)
        .filter_map(|code| match ResponseError::try_from_code(code)? {
            ResponseError::Unknown(_) => None,
            error => Some((code, error.to_string())),
        })
        .collect()
}

/// `NotLeaderOrFollower` as `NOT_LEADER_OR_FOLLOWER`.
///
/// Kafka spells its errors that way, and `kafka-protocol` derives its own spelling from Kafka's.
/// The kernel's constants for the API keys follow the same spelling.
fn shouting(camel: &str) -> String {
    assert!(
        camel.starts_with(|first: char| first.is_ascii_uppercase()) && camel.chars().all(|c| c.is_ascii_alphabetic()),
        "{camel:?} is not words of letters that each start with a capital, so its spelling may not convert back"
    );
    let mut shouting = String::new();
    for character in camel.chars() {
        if character.is_ascii_uppercase() && !shouting.is_empty() {
            shouting.push('_');
        }
        shouting.push(character.to_ascii_uppercase());
    }
    shouting
}

/// `NOT_LEADER_OR_FOLLOWER` as `NotLeaderOrFollower`: the way back from [`shouting`].
fn camel(shouting: &str) -> String {
    shouting
        .split('_')
        .flat_map(|word| {
            let mut letters = word.chars();
            letters
                .next()
                .into_iter()
                .chain(letters.map(|letter| letter.to_ascii_lowercase()))
        })
        .collect()
}

/// The contents of `src/names.rs` of `gaslightmq_kernel`.
fn render() -> String {
    let mut file = format!(
        "//! The names Kafka gives its API keys and its error codes.\n\
         //!\n\
         //! This file is generated, so do not edit it.\n\
         //! The test `kernel_names` of `gaslightmq_wire` renders it from the `kafka-protocol` crate,\n\
         //! and fails when this file differs from what it renders.\n\
         //! Write the file again with:\n\
         //!\n\
         //! ```text\n\
         //! {COMMAND}\n\
         //! ```\n\
         \n\
         // One line per API: the key, the name of the constant, and the name of the API.\n\
         crate::api::api_keys! {{\n"
    );
    for (key, name) in apis() {
        writeln!(file, "    {key} {} {name:?}", shouting(&name)).unwrap();
    }
    file.push_str(
        "}\n\
         \n\
         // One line per error: the code, and the name of the error.\n\
         crate::error_code::error_codes! {\n",
    );
    for (code, name) in errors() {
        writeln!(file, "    {code} {}", shouting(&name)).unwrap();
    }
    file.push_str("}\n");
    file
}

#[test]
fn the_kernels_name_file_is_what_kafka_protocol_renders() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../gaslightmq_kernel/src/names.rs");
    let rendered = render();

    if std::env::var_os(UPDATE).is_some() {
        std::fs::write(&path, rendered).unwrap_or_else(|error| panic!("cannot write {}: {error}", path.display()));
        return;
    }

    let checked_in =
        std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    assert!(
        checked_in == rendered,
        "{} differs from the names of kafka-protocol. Write it again with:\n\n    {COMMAND}\n",
        path.display()
    );
}

#[test]
fn the_kernel_names_every_api_of_kafka_protocol() {
    let apis = apis();

    for (key, name) in &apis {
        assert_eq!(ApiKey::from(*key).name(), Some(name.as_str()));
        assert_eq!(name.parse::<ApiKey>(), Ok(ApiKey::from(*key)));
    }
    let known: Vec<i16> = ApiKey::known().map(ApiKey::value).collect();
    let keys: Vec<i16> = apis.iter().map(|(key, _)| *key).collect();
    assert_eq!(known, keys);
}

#[test]
fn the_kernel_names_every_error_of_kafka_protocol() {
    let errors = errors();

    for (code, name) in &errors {
        let error = ErrorCode::try_from(*code).unwrap();
        // The kernel spells an error as Kafka does, and that spelling converts back to the one of `kafka-protocol`.
        assert_eq!(error.name().map(camel).as_deref(), Some(name.as_str()));
        assert_eq!(shouting(name).parse::<ErrorCode>(), Ok(error));
    }
    let known: Vec<i16> = ErrorCode::known().map(ErrorCode::value).collect();
    let codes: Vec<i16> = errors.iter().map(|(code, _)| *code).collect();
    assert_eq!(known, codes);
}

#[test]
fn the_kernel_spells_errors_as_kafka_does() {
    // A few names from Kafka's own `Errors.java`, which `kafka-protocol` does not keep in this spelling.
    for (code, name) in [
        (-1, "UNKNOWN_SERVER_ERROR"),
        (1, "OFFSET_OUT_OF_RANGE"),
        (6, "NOT_LEADER_OR_FOLLOWER"),
        (7, "REQUEST_TIMED_OUT"),
        (27, "REBALANCE_IN_PROGRESS"),
        (58, "SASL_AUTHENTICATION_FAILED"),
        (110, "FENCED_MEMBER_EPOCH"),
    ] {
        assert_eq!(ErrorCode::try_from(code).unwrap().name(), Some(name));
    }
}

#[test]
fn kafka_protocol_has_no_error_for_no_error() {
    assert!(ResponseError::try_from_code(0).is_none());
    assert_eq!(ErrorCode::from_wire(0), None);
}
