//! The names of the files of a recording: which object a file is, and whether it is sealed.
//!
//! A name is the position of the first entry, with 20 digits so that the names sort as the positions do, then the
//! kind, and `.open` while the object grows.
//!
//! ```text
//! 00000000000000000000.index.open
//! 00000000000000000000.segment
//! 00000000000017840022.segment.open
//! ```

use gaslightmq_recording::Position;
use gaslightmq_recording_segments::{Object, ObjectKind};

/// The name of the manifest of a recording.
pub(crate) const MANIFEST: &str = "manifest";

/// The file of one object that grows: the object, and whether it is sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObjectFile {
    pub(crate) object: Object,
    pub(crate) sealed: bool,
}

impl ObjectFile {
    /// The digits of a position: enough for every `u64`.
    const DIGITS: usize = 20;
    const OPEN: &str = ".open";

    /// The file of `object` while it grows.
    pub(crate) const fn open(object: Object) -> ObjectFile {
        ObjectFile { object, sealed: false }
    }

    /// The file of `object` once it is sealed.
    pub(crate) const fn sealed(object: Object) -> ObjectFile {
        ObjectFile { object, sealed: true }
    }

    /// The name of the file.
    pub(crate) fn name(self) -> String {
        let open = if self.sealed { "" } else { Self::OPEN };
        format!(
            "{:0digits$}.{}{open}",
            self.object.first.number(),
            Self::kind_name(self.object.kind),
            digits = Self::DIGITS
        )
    }

    /// The object that a file of this name is, or `None` for a file that is no object of a recording.
    pub(crate) fn parse(name: &str) -> Option<ObjectFile> {
        let (name, sealed) = match name.strip_suffix(Self::OPEN) {
            Some(name) => (name, false),
            None => (name, true),
        };
        let (digits, named) = name.split_once('.')?;
        let kind = [ObjectKind::Segment, ObjectKind::IndexChunk]
            .into_iter()
            .find(|kind| Self::kind_name(*kind) == named)?;
        if digits.len() != Self::DIGITS || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
            return None;
        }
        let first = Position::new(digits.parse().ok()?);
        Some(ObjectFile {
            object: Object { kind, first },
            sealed,
        })
    }

    const fn kind_name(kind: ObjectKind) -> &'static str {
        match kind {
            ObjectKind::Segment => "segment",
            ObjectKind::IndexChunk => "index",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_the_first_position_in_twenty_digits_the_kind_and_whether_it_grows() {
        let segment = Object::segment(Position::new(17_840_022));
        let chunk = Object::index_chunk(Position::FIRST);

        assert_eq!(ObjectFile::open(segment).name(), "00000000000017840022.segment.open");
        assert_eq!(ObjectFile::sealed(segment).name(), "00000000000017840022.segment");
        assert_eq!(ObjectFile::open(chunk).name(), "00000000000000000000.index.open");
        assert_eq!(ObjectFile::sealed(chunk).name(), "00000000000000000000.index");
    }

    #[test]
    fn a_name_reads_back_as_its_object() {
        let last = Object::segment(Position::new(u64::MAX));
        for file in [ObjectFile::open(last), ObjectFile::sealed(last)] {
            assert_eq!(ObjectFile::parse(&file.name()), Some(file));
        }
    }

    #[test]
    fn a_file_with_another_name_is_no_object() {
        for name in [
            MANIFEST,
            ".manifest.4242.0.tmp",
            "17840022.segment",
            "0000000000001784002x.segment",
            "00000000000017840022.segment.tmp",
            "00000000000017840022.footer",
            "99999999999999999999.segment",
        ] {
            assert_eq!(ObjectFile::parse(name), None, "{name} is no object of a recording");
        }
    }
}
