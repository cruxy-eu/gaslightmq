//! However the bytes are cut, in what the compressor is handed and in the chunks that a reader hands back,
//! every block unpacks to what was written, and a torn tail keeps what its last flush let go of.

use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{BlockEnd, Codec, Compression, Compressor, Decompressor, ZstandardLevel};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;

/// The most bytes that a block of a case holds.
const LARGEST_BLOCK: usize = 64 * 1024;
/// The most blocks that a case writes.
const MOST_BLOCKS: usize = 8;
/// The most cuts in what one block is handed as, or read back as.
const MOST_CUTS: usize = 8;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn every_block_unpacks_to_what_was_written_however_it_is_cut(
        blocks in vec(vec(any::<u8>(), 1..LARGEST_BLOCK), 1..MOST_BLOCKS),
        write_cuts in vec(vec(any::<Index>(), 0..MOST_CUTS), MOST_BLOCKS),
        flushes in vec(any::<Index>(), MOST_BLOCKS),
        read_cuts in vec(vec(any::<Index>(), 0..MOST_CUTS), MOST_BLOCKS),
        level in prop_oneof![Just(-5), Just(1), Just(3), Just(9)],
    ) {
        let compression = Compression::Zstandard { level: ZstandardLevel::new(level)? };
        let mut compressor = Compressor::new(compression)?;
        let mut segment = Vec::new();
        let mut block_ends = Vec::new();
        for ((block, cuts), flush) in blocks.iter().zip(&write_cuts).zip(&flushes) {
            let flush_at = flush.index(block.len() + 1);
            let mut at: Vec<usize> = cuts.iter().map(|cut| cut.index(block.len() + 1)).collect();
            at.extend([0, flush_at, block.len()]);
            at.sort_unstable();
            for piece in at.windows(2) {
                compressor.write(&block[piece[0]..piece[1]], &mut segment)?;
                if piece[1] == flush_at {
                    compressor.flush(&mut segment)?;
                }
            }
            compressor.finish_block(&mut segment)?;
            block_ends.push(segment.len());
        }

        let mut decompressor = Decompressor::new(compression.codec());
        let mut block_starts = 0;
        for ((block, cuts), block_ends) in blocks.iter().zip(&read_cuts).zip(block_ends) {
            let stored = &segment[block_starts..block_ends];
            let mut at: Vec<usize> = cuts.iter().map(|cut| cut.index(stored.len() + 1)).collect();
            at.extend([0, stored.len()]);
            at.sort_unstable();
            let chunks = at.windows(2).map(|piece| &stored[piece[0]..piece[1]]);
            let mut buffer = BatchBuffer::with_room(block.len());

            let unpacked = decompressor.read_block(chunks, &mut buffer)?;

            prop_assert_eq!(unpacked.end, BlockEnd::Finished);
            prop_assert_eq!(unpacked.taken, stored.len());
            prop_assert_eq!(buffer.written(), block.as_slice());
            block_starts = block_ends;
        }
    }

    #[test]
    fn a_torn_block_keeps_every_byte_up_to_its_last_whole_flush(
        block in vec(any::<u8>(), 1..LARGEST_BLOCK),
        flushes in vec(any::<Index>(), 1..MOST_CUTS),
        tear in any::<Index>(),
    ) {
        let mut compressor = Compressor::new(Compression::default())?;
        let mut segment = Vec::new();
        let mut at: Vec<usize> = flushes.iter().map(|flush| flush.index(block.len() + 1)).collect();
        at.insert(0, 0);
        at.sort_unstable();
        let mut flushed = Vec::new();
        for piece in at.windows(2) {
            compressor.write(&block[piece[0]..piece[1]], &mut segment)?;
            compressor.flush(&mut segment)?;
            flushed.push((segment.len(), piece[1]));
        }
        let torn = &segment[..tear.index(segment.len() + 1)];
        let kept = flushed
            .iter()
            .filter(|(stored, _)| *stored <= torn.len())
            .map(|(_, written)| *written)
            .max()
            .unwrap_or(0);
        let mut decompressor = Decompressor::new(Codec::Zstandard);
        let mut buffer = BatchBuffer::with_room(block.len());

        let unpacked = decompressor.read_block([torn], &mut buffer)?;

        prop_assert_eq!(unpacked.end, BlockEnd::Unfinished);
        prop_assert_eq!(unpacked.taken, torn.len());
        prop_assert!(block.starts_with(buffer.written()));
        prop_assert!(buffer.written().len() >= kept);
    }
}
