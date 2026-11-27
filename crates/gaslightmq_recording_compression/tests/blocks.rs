//! A block is a frame of its own: a reader starts at any block, and a torn tail loses the open block only.

use futures::AsyncWriteExt;
use futures::executor::block_on;
use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{
    BlockEnd, Codec, Compression, CompressionConfig, CompressionPool, Compressor, Decompressor, ZstandardLevel,
};

const FIRST_BLOCK: &[u8] = b"a call to Produce, a call to Fetch, a call to Produce, a call to Fetch";
const SECOND_BLOCK: &[u8] = b"a call to Metadata, a call to Metadata, a call to Metadata";

#[test]
fn every_block_unpacks_by_itself() -> anyhow::Result<()> {
    let config = CompressionConfig::default();
    let pool = CompressionPool::new(&config)?;
    let mut compressor = Compressor::new(&config, &pool, Vec::new());

    let first_block_starts = 0;
    block_on(compressor.write_all(FIRST_BLOCK))?;
    block_on(compressor.finish_block())?;
    let second_block_starts = compressor.get_ref().len();
    block_on(compressor.write_all(SECOND_BLOCK))?;
    block_on(compressor.close())?;
    let segment = compressor.into_inner();

    let mut from_the_first = Decompressor::new(Codec::Zstandard, &segment[first_block_starts..]);
    let mut first = BatchBuffer::with_room(FIRST_BLOCK.len());
    let mut second = BatchBuffer::with_room(SECOND_BLOCK.len());
    assert_eq!(
        block_on(from_the_first.read_block(&mut first))?,
        Some(BlockEnd::Finished)
    );
    assert_eq!(
        block_on(from_the_first.read_block(&mut second))?,
        Some(BlockEnd::Finished)
    );
    assert_eq!(
        block_on(from_the_first.read_block(&mut BatchBuffer::with_room(0)))?,
        None
    );
    assert_eq!(first.written(), FIRST_BLOCK);
    assert_eq!(second.written(), SECOND_BLOCK);

    let mut from_the_second = Decompressor::new(Codec::Zstandard, &segment[second_block_starts..]);
    let mut second_alone = BatchBuffer::with_room(SECOND_BLOCK.len());
    assert_eq!(
        block_on(from_the_second.read_block(&mut second_alone))?,
        Some(BlockEnd::Finished)
    );
    assert_eq!(second_alone.written(), SECOND_BLOCK);
    Ok(())
}

#[test]
fn a_torn_tail_keeps_the_open_block_up_to_its_last_flush() -> anyhow::Result<()> {
    let config = CompressionConfig::default();
    let pool = CompressionPool::new(&config)?;
    let mut compressor = Compressor::new(&config, &pool, Vec::new());

    block_on(compressor.write_all(FIRST_BLOCK))?;
    block_on(compressor.finish_block())?;
    block_on(compressor.write_all(SECOND_BLOCK))?;
    block_on(compressor.flush())?;
    let segment = compressor.into_inner();

    let mut decompressor = Decompressor::new(Codec::Zstandard, segment.as_slice());
    let mut first = BatchBuffer::with_room(FIRST_BLOCK.len());
    let mut open = BatchBuffer::with_room(SECOND_BLOCK.len());
    assert_eq!(block_on(decompressor.read_block(&mut first))?, Some(BlockEnd::Finished));
    assert_eq!(
        block_on(decompressor.read_block(&mut open))?,
        Some(BlockEnd::Unfinished)
    );
    assert_eq!(open.written(), SECOND_BLOCK);
    Ok(())
}

#[test]
fn a_level_compresses_with_it() -> anyhow::Result<()> {
    let config = CompressionConfig {
        compression: Compression::Zstandard {
            level: ZstandardLevel::new(19)?,
        },
        ..CompressionConfig::default()
    };
    let pool = CompressionPool::new(&config)?;
    let compressor = Compressor::new(&config, &pool, Vec::new());

    assert_eq!(compressor.compression(), config.compression);
    assert_eq!(compressor.compression().codec(), Codec::Zstandard);
    Ok(())
}

#[test]
fn none_writes_the_bytes_as_they_are() -> anyhow::Result<()> {
    let config = CompressionConfig {
        compression: Compression::None,
        ..CompressionConfig::default()
    };
    let pool = CompressionPool::new(&config)?;
    let mut compressor = Compressor::new(&config, &pool, Vec::new());

    block_on(compressor.write_all(FIRST_BLOCK))?;
    block_on(compressor.finish_block())?;
    block_on(compressor.close())?;

    assert_eq!(compressor.into_inner(), FIRST_BLOCK);
    Ok(())
}
