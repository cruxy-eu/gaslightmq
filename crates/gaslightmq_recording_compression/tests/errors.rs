//! What a reader is told when a block cannot be read.

use futures::AsyncWriteExt;
use futures::executor::block_on;
use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{
    Codec, CodecError, CompressionConfig, CompressionPool, Compressor, DecompressError, Decompressor, ZstandardLevel,
    ZstandardLevelError,
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
    let mut decompressor = Decompressor::new(Codec::Zstandard, stored);

    let read = block_on(decompressor.read_block(&mut BatchBuffer::with_room(stored.len())));

    assert!(matches!(read, Err(DecompressError::NotUnpacked { .. })));
}

#[test]
fn a_block_larger_than_its_room_is_not_unpacked() -> anyhow::Result<()> {
    let config = CompressionConfig::default();
    let pool = CompressionPool::new(&config)?;
    let mut compressor = Compressor::new(&config, &pool, Vec::new());
    block_on(compressor.write_all(BLOCK))?;
    block_on(compressor.close())?;
    let segment = compressor.into_inner();

    let mut decompressor = Decompressor::new(Codec::Zstandard, segment.as_slice());
    let read = block_on(decompressor.read_block(&mut BatchBuffer::with_room(BLOCK.len() - 1)));

    assert!(matches!(read, Err(DecompressError::NotUnpacked { .. })));
    Ok(())
}
