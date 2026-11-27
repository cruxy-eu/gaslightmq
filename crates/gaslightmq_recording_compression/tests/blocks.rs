//! A block is a frame of its own: a reader starts at any block, and a torn tail loses the open block only.

use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{
    BlockEnd, Codec, Compression, Compressor, Decompressor, UnpackedBlock, ZstandardLevel,
};

const FIRST_BLOCK: &[u8] = b"a call to Produce, a call to Fetch, a call to Produce, a call to Fetch";
const SECOND_BLOCK: &[u8] = b"a call to Metadata, a call to Metadata, a call to Metadata";

#[test]
fn every_block_unpacks_by_itself() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut segment = Vec::new();
    compressor.write(FIRST_BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;
    let second_block_starts = segment.len();
    compressor.write(SECOND_BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;

    let mut decompressor = Decompressor::new(Codec::Zstandard);
    let mut second = BatchBuffer::with_room(SECOND_BLOCK.len());
    let unpacked = decompressor.read_block([&segment[second_block_starts..]], &mut second)?;

    assert_eq!(
        unpacked,
        UnpackedBlock {
            end: BlockEnd::Finished,
            taken: segment.len() - second_block_starts,
        }
    );
    assert_eq!(second.written(), SECOND_BLOCK);
    Ok(())
}

#[test]
fn a_block_takes_no_byte_of_the_next() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut segment = Vec::new();
    compressor.write(FIRST_BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;
    let second_block_starts = segment.len();
    compressor.write(SECOND_BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;

    let mut decompressor = Decompressor::new(Codec::Zstandard);
    let mut first = BatchBuffer::with_room(FIRST_BLOCK.len());
    let unpacked = decompressor.read_block([segment.as_slice()], &mut first)?;

    assert_eq!(unpacked.taken, second_block_starts);
    assert_eq!(first.written(), FIRST_BLOCK);
    Ok(())
}

#[test]
fn a_torn_tail_keeps_the_open_block_up_to_its_last_flush() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut segment = Vec::new();
    compressor.write(SECOND_BLOCK, &mut segment)?;
    compressor.flush(&mut segment)?;

    let mut decompressor = Decompressor::new(Codec::Zstandard);
    let mut open = BatchBuffer::with_room(SECOND_BLOCK.len());
    let unpacked = decompressor.read_block([segment.as_slice()], &mut open)?;

    assert_eq!(unpacked.end, BlockEnd::Unfinished);
    assert_eq!(open.written(), SECOND_BLOCK);
    Ok(())
}

#[test]
fn a_block_that_nothing_was_written_to_writes_nothing() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut segment = Vec::new();

    compressor.flush(&mut segment)?;
    compressor.finish_block(&mut segment)?;

    assert_eq!(segment, Vec::<u8>::new());
    Ok(())
}

#[test]
fn a_compressor_says_its_compression() -> anyhow::Result<()> {
    let compression = Compression::Zstandard {
        level: ZstandardLevel::new(19)?,
    };

    let compressor = Compressor::new(compression)?;

    assert_eq!(compressor.compression(), compression);
    assert_eq!(compressor.compression().codec(), Codec::Zstandard);
    Ok(())
}

#[test]
fn none_writes_the_bytes_as_they_are() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::None)?;
    let mut segment = Vec::new();

    compressor.write(FIRST_BLOCK, &mut segment)?;
    compressor.finish_block(&mut segment)?;

    assert_eq!(segment, FIRST_BLOCK);
    Ok(())
}

#[test]
fn none_ends_a_block_where_its_buffer_is_full() -> anyhow::Result<()> {
    let segment = [FIRST_BLOCK, SECOND_BLOCK].concat();
    let mut decompressor = Decompressor::new(Codec::None);
    let mut first = BatchBuffer::with_room(FIRST_BLOCK.len());

    let unpacked = decompressor.read_block([segment.as_slice()], &mut first)?;

    assert_eq!(
        unpacked,
        UnpackedBlock {
            end: BlockEnd::Finished,
            taken: FIRST_BLOCK.len(),
        }
    );
    assert_eq!(first.written(), FIRST_BLOCK);
    Ok(())
}

#[test]
fn a_block_may_end_at_a_flush_and_unpacks_in_full() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut first_chunk = Vec::new();
    let mut last_chunk = Vec::new();
    compressor.write(FIRST_BLOCK, &mut first_chunk)?;
    compressor.flush(&mut first_chunk)?;
    compressor.write(SECOND_BLOCK, &mut last_chunk)?;
    compressor.flush(&mut last_chunk)?;

    let mut decompressor = Decompressor::new(Codec::Zstandard);
    let mut block = BatchBuffer::with_room(FIRST_BLOCK.len() + SECOND_BLOCK.len());
    let unpacked = decompressor.read_block([first_chunk.as_slice(), last_chunk.as_slice()], &mut block)?;

    assert_eq!(
        unpacked,
        UnpackedBlock {
            end: BlockEnd::Unfinished,
            taken: first_chunk.len() + last_chunk.len(),
        }
    );
    assert_eq!(block.written(), [FIRST_BLOCK, SECOND_BLOCK].concat());
    Ok(())
}

#[test]
fn frames_unpack_one_behind_the_other_into_one_buffer() -> anyhow::Result<()> {
    let mut compressor = Compressor::new(Compression::default())?;
    let mut first_frame = Vec::new();
    let mut second_frame = Vec::new();
    compressor.write(FIRST_BLOCK, &mut first_frame)?;
    compressor.finish_block(&mut first_frame)?;
    compressor.write(SECOND_BLOCK, &mut second_frame)?;
    compressor.finish_block(&mut second_frame)?;

    let mut decompressor = Decompressor::new(Codec::Zstandard);
    let mut frames = BatchBuffer::with_room(FIRST_BLOCK.len() + SECOND_BLOCK.len());
    let first = decompressor.read_block([first_frame.as_slice()], &mut frames)?;
    let second = decompressor.read_block([second_frame.as_slice()], &mut frames)?;

    assert_eq!(first.end, BlockEnd::Finished);
    assert_eq!(second.end, BlockEnd::Finished);
    assert_eq!(frames.written(), [FIRST_BLOCK, SECOND_BLOCK].concat());
    Ok(())
}

#[test]
fn none_unpacks_one_behind_the_other_into_one_buffer() -> anyhow::Result<()> {
    let mut decompressor = Decompressor::new(Codec::None);
    let mut frames = BatchBuffer::with_room(FIRST_BLOCK.len() + SECOND_BLOCK.len());

    decompressor.read_block([FIRST_BLOCK], &mut frames)?;
    let second = decompressor.read_block([SECOND_BLOCK], &mut frames)?;

    assert_eq!(second.end, BlockEnd::Finished);
    assert_eq!(frames.written(), [FIRST_BLOCK, SECOND_BLOCK].concat());
    Ok(())
}
