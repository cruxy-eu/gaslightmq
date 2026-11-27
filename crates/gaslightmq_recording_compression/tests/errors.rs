//! What a reader is told when a block cannot be read.

use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{
    Codec, CodecError, Compression, Compressor, Decompressor, ZstandardLevel, ZstandardLevelError,
};

const BLOCK: &[u8] = b"a call to Produce, a call to Fetch, a call to Produce, a call to Fetch";

#[test]
fn a_header_names_every_codec_of_this_build_by_its_number() {
    assert_eq!(Codec::from_id(Codec::None.id()), Ok(Codec::None));
    assert_eq!(Codec::from_id(Codec::Zstandard.id()), Ok(Codec::Zstandard));
}

#[test]
fn a_number_that_names_no_codec_is_refused() {
    let unknown = 7;

    assert_eq!(Codec::from_id(unknown), Err(CodecError { id: unknown }));
}

#[test]
fn level_zero_is_no_level() {
    let refused = ZstandardLevel::new(0);

    assert!(matches!(refused, Err(ZstandardLevelError { level: 0, .. })));
}

#[test]
fn bytes_that_zstandard_did_not_write_are_not_unpacked() {
    let stored: &[u8] = b"these bytes were never compressed";
    let mut decompressor = Decompressor::new(Codec::Zstandard);

    let read = decompressor.read_block([stored], &mut BatchBuffer::with_room(stored.len()));

    assert!(read.is_err());
}

#[test]
fn a_block_larger_than_its_room_is_not_unpacked() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut segment = Vec::new();
    compressor.write(BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;
    let mut decompressor = Decompressor::new(Codec::Zstandard);

    let read = decompressor.read_block([segment.as_slice()], &mut BatchBuffer::with_room(BLOCK.len() - 1));

    assert!(read.is_err());
    Ok(())
}
