//! A description, a summary and an end are messages that a layout carries, and that this crate reads.

use gaslightmq_clock::{Duration, Instant};
use gaslightmq_environment::RecordingMode;
use gaslightmq_journal::JournalEntryId;
use gaslightmq_recording::prelude::*;

#[test]
fn a_description_comes_back_from_wherever_a_layout_keeps_it() -> anyhow::Result<()> {
    let description = Description {
        environment: "orders-sim".parse()?,
        started: "2026-11-21T14:02:11.527Z".parse()?,
        clock: Instant::ZERO + Duration::from_secs(42),
        mode: RecordingMode::MetadataOnly,
        backend: RecordedBackend::Upstream,
        written_by: "0.1.0".to_owned(),
        armed: vec!["leader-flap".parse()?, "slow-fetch".parse()?],
        scenario: Some(ActiveScenario {
            scenario: "black-friday".parse()?,
            step: Some("warm-up".parse()?),
        }),
        journal_after: Some(JournalEntryId::new(118)?),
    };

    let message = description.encode();

    // A message is a head of 16 bytes and the message, up to a multiple of 16.
    assert_eq!(message.len() % 16, 0);
    assert_eq!(Description::decode(EncodingVersion::CURRENT, &message)?, description);

    // A layout hands a message over from any address, and behind it can stand whatever the layout keeps there.
    let mut in_an_object = vec![0xAB];
    in_an_object.extend_from_slice(&message);
    in_an_object.extend_from_slice(b"the records that follow");
    assert_eq!(
        Description::decode(EncodingVersion::CURRENT, &in_an_object[1..])?,
        description
    );
    Ok(())
}

#[test]
fn bytes_that_are_another_message_or_none_are_refused() {
    let end = End {
        ending: Ending::FellBehind,
        at: Instant::ZERO + Duration::from_secs(90),
        entries: 1204,
        calls: 1180,
    };
    let message = end.encode();

    assert_eq!(End::decode(EncodingVersion::CURRENT, &message), Ok(end));
    assert_eq!(
        Description::decode(EncodingVersion::CURRENT, &message),
        Err(MessageError::Damaged {
            reason: "it is an end, and not a description".to_owned()
        })
    );
    assert_eq!(
        Summary::decode(EncodingVersion::CURRENT, &message)
            .unwrap_err()
            .to_string(),
        "the message is damaged: it is an end, and not a summary"
    );

    // A message that was cut short says so, and so do bytes that are no head.
    let cut_short = &message[..message.len() - 16];
    let refused = End::decode(EncodingVersion::CURRENT, cut_short).unwrap_err();
    assert!(
        refused
            .to_string()
            .starts_with("the message is damaged: the head says a message of "),
        "{refused}"
    );
    assert_eq!(
        End::decode(EncodingVersion::CURRENT, b"GMQ").unwrap_err().to_string(),
        "the message is damaged: 3 bytes are left, and a head is 16 bytes"
    );
}

#[test]
fn a_reader_refuses_a_version_of_the_encoding_that_it_does_not_know() {
    let current = EncodingVersion::CURRENT.number();

    assert_eq!(
        EncodingVersion::read_by_this_build(current),
        Ok(EncodingVersion::CURRENT)
    );
    let newer = EncodingVersion::read_by_this_build(current + 1).unwrap_err();
    assert_eq!(
        newer,
        EncodingVersionError::Newer {
            written: current + 1,
            known: current
        }
    );
    assert_eq!(
        OpenError::EncodingNotRead { error: newer }.to_string(),
        format!(
            "the recording does not open: the recording has version {} of the encoding, \
             and this build reads up to version {current}",
            current + 1
        )
    );
    assert_eq!(
        EncodingVersion::read_by_this_build(0).unwrap_err().to_string(),
        "the versions of the encoding of a recording count from 1"
    );
}
