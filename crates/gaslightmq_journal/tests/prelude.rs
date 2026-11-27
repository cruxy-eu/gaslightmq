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

    use gaslightmq_clock::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_journal::prelude::*;
    use gaslightmq_kernel::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let rule = RecordedName::new("slow-order-producers").expect("a name");
        let definition = RecordedDefinition::Rule(rule.clone());
        let change = Change::ParameterChanged {
            rule,
            parameter: Parameter::Chance,
            old: "100%".to_owned(),
            new: "40%".to_owned(),
        };
        let from = ChangeSource::new(ChangeSurface::Pad).with_cause(ChangeCause::Undo);
        let mut journal = RecentJournal::new(RecentJournalSize::default());
        let entry: JournalEntry = journal.record(gaslightmq_clock::Instant::ZERO, change, from, Some(Revision::new(7)));
        let id: JournalEntryId = entry.id;

        vec![
            id.to_string(),
            definition.to_string(),
            RecentJournalSize::default().to_string(),
            format!("{entry:?}"),
            RecordingStopCause::FellBehind.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        vec![
            JournalEntryIdError::Zero.to_string(),
            RecordedNameError::Empty.to_string(),
            RecentJournalSizeError::Zero.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_and_its_neighbours() {
    let items = next_to_std_and_every_neighbour::every_item();

    assert_eq!(items[0], "1");
    assert_eq!(items[1], "rule \"slow-order-producers\"");
    assert_eq!(items[2], "1000 entries");
    assert_eq!(items[4], "the recorder could not keep up");
}

#[test]
fn the_prelude_holds_every_error() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_error(),
        [
            "a journal entry ID is at least one",
            "recorded name \"\" is empty",
            "a recent journal keeps at least one entry",
        ]
    );
}

#[test]
fn the_prelude_and_the_crate_root_export_the_same_types() {
    let from_root: gaslightmq_journal::JournalEntryIdError = gaslightmq_journal::JournalEntryId::new(0).unwrap_err();
    let from_prelude: gaslightmq_journal::prelude::JournalEntryIdError = from_root.clone();

    assert_eq!(from_root, from_prelude);
}
