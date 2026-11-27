//! The prelude holds every type, and its names clash with none of `std`, of the clock,
//! or of the preludes that code imports next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the preludes of the vocabulary that an entry is made of.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_and_every_neighbour {
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

    use gaslightmq_call::prelude::*;
    use gaslightmq_clock::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_journal::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_recording::prelude::*;

    /// The traits of the contract, each named without a path.
    #[expect(dead_code, reason = "the bounds only have to compile: no storage is at hand here")]
    pub fn every_trait<S>()
    where
        S: RecordingStorage,
        S::Writer: RecordingWriter,
        S::Reader: RecordingReader,
        <S::Reader as RecordingReader>::Entries: RecordedEntries,
    {
    }

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let started: UtcInstant = "2026-11-21T14:02:11.527Z".parse().expect("an instant");
        let description = Description {
            environment: "orders-sim".parse().expect("a name"),
            started,
            clock: gaslightmq_clock::Instant::ZERO,
            mode: RecordingMode::Full,
            backend: RecordedBackend::Simulated,
            written_by: "0.1.0".to_owned(),
            armed: Vec::new(),
            scenario: Some(ActiveScenario {
                scenario: "black-friday".parse().expect("a name"),
                step: None,
            }),
            journal_after: None,
        };
        let id: RecordingId = description.id();
        let end = End {
            ending: Ending::Stopped,
            at: gaslightmq_clock::Instant::ZERO,
            entries: 1,
            calls: 1,
        };

        let call = Call::new(
            CallId::FIRST,
            Fields::new().with(ConnectionNumber::new(7)).without_truth(),
            Delivered::Nothing,
            Explanation::none(),
            CallTiming::arrived(gaslightmq_clock::Instant::ZERO),
        )
        .expect("a call on one connection");
        let frames = CallFrames::new(gaslightmq_wire::Frame::new(vec![0u8; 4]));
        let entry: Entry = Entry::call(call, frames, RecordingMode::Full).expect("a call without a lie");
        let mut summary = Summary::empty();
        summary.add(&entry, 256);
        let values: Option<&SummaryValues> = summary.values(Field::Connection);
        let wanted = Wanted::may_be_in(&Everything, &summary);

        let mut batch_encoder = BatchEncoder::new();
        let encoded: EncodedEntry = batch_encoder.encode(entry);
        let mut buffer = BatchBuffer::with_room(encoded.entry().len());
        buffer.write(encoded.entry());
        let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, buffer, None);
        let mut read = Vec::new();
        for (position, view) in batch.entries() {
            let view: EntryView<'_> = view.expect("an entry that passed the check");
            let EntryView::Call(view) = view else {
                continue;
            };
            let view: CallView<'_> = view;
            let fields: FieldsView<'_> = view.fields();
            let value: Option<FieldValueView<'_>> = fields.values().next();
            let part: Option<PartView<'_>> = fields.parts().next();
            let explanation: ExplanationView<'_> = view.explanation();
            let line: Option<RuleExplanationView<'_>> = explanation.rules().next();
            let frame: RecordedFrame = view.frame(FrameSlot::Request).expect("a frame inside its batch");
            read.push(format!(
                "{position} {:?} {} {} {} {:?}",
                value.map(|value| value.field()),
                part.is_some(),
                line.is_some(),
                frame.state() == FrameState::Withheld,
                frame.bytes()
            ));
        }
        let journal: Option<JournalEntryView<'_>> = None;
        let session: Option<SessionEventView<'_>> = None;

        vec![
            id.to_string(),
            started.to_string(),
            end.ending.to_string(),
            format!("{values:?} {wanted}"),
            read.join(";"),
            format!("{} {}", journal.is_some(), session.is_some()),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        vec![
            UtcInstantError::AfterYear9999 { unix_millis: u64::MAX }.to_string(),
            RecordingIdError::NoStart {
                input: "orders-sim".to_owned(),
            }
            .to_string(),
            EncodingVersionError::Zero.to_string(),
            StartError::AlreadyThere.to_string(),
            WriteError::NotWritten {
                reason: "the disk is full".to_owned(),
            }
            .to_string(),
            ListError::NotListed {
                reason: "the directory is gone".to_owned(),
            }
            .to_string(),
            OpenError::NotFound.to_string(),
            ExtentError::NotRead {
                reason: "the disk did not answer".to_owned(),
            }
            .to_string(),
            ReadError::AllReadersBudgetSpent {
                held: 268_435_456,
                budget: 268_435_456,
            }
            .to_string(),
            NextBatchError::ReaderBudgetSpent {
                held: 16_777_216,
                budget: 16_777_216,
            }
            .to_string(),
            NextBatchError::Damaged {
                from: Position::new(900),
                to: Position::new(1100),
            }
            .to_string(),
            EntryError::Damaged {
                reason: "a call ID is at least one".to_owned(),
            }
            .to_string(),
            MessageError::Damaged {
                reason: "it is an end, and not a description".to_owned(),
            }
            .to_string(),
            CallEntryError::LieNotStored {
                lie: "topology".to_owned(),
            }
            .to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_and_its_neighbours() {
    let items = next_to_std_and_every_neighbour::every_item();

    assert_eq!(items[0], "orders-sim.2026-11-21T14-02-11.527Z");
    assert_eq!(items[1], "2026-11-21T14:02:11.527Z");
    assert_eq!(items[2], "someone stopped the recording");
    assert_eq!(items[4], "0 Some(Connection) false false true None");
    assert_eq!(items[5], "false false");
}

#[test]
fn the_prelude_holds_every_error() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_error(),
        [
            "18446744073709551615 milliseconds after 1970 is after the year 9999",
            "\"orders-sim\" is no recording ID: the name of an environment, a `.` and the instant the recording started",
            "the versions of the encoding of a recording count from 1",
            "a recording with this ID is there already",
            "the recording was not written: the disk is full",
            "the recordings were not listed: the directory is gone",
            "no recording has this ID",
            "the extent of the recording was not read: the disk did not answer",
            "the readers of this process hold 268435456 bytes of recordings, and `recordings.all_readers_memory` \
             allows 268435456",
            "the consumer of this stream holds 16777216 bytes of batches, and `recordings.reader_memory` allows \
             16777216",
            "the entries 900 to 1100 of the recording are lost",
            "the entry is damaged: a call ID is at least one",
            "the message is damaged: it is an end, and not a description",
            "the encoding of a recording has no stored type for the lie topology",
        ]
    );
}

#[test]
fn the_prelude_and_the_crate_root_export_the_same_types() {
    let from_root: gaslightmq_recording::UtcInstantError = gaslightmq_recording::UtcInstant::new(u64::MAX).unwrap_err();
    let from_prelude: gaslightmq_recording::prelude::UtcInstantError = from_root.clone();

    assert_eq!(from_root, from_prelude);
}
