use bytes::{Buf, Bytes, BytesMut};

use crate::FrameError;

/// One Kafka message as it travels on a connection.
///
/// On the wire a frame is a 4-byte big-endian length followed by that many
/// bytes. A `Frame` holds those bytes, the payload, without the length.
/// The payload starts with a request header or a response header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame(Bytes);

impl Frame {
    /// The largest payload a [`FrameDecoder`] accepts unless told otherwise.
    pub const MAX_LEN: usize = 100 * 1024 * 1024;

    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self(payload.into())
    }

    /// The bytes after the length prefix.
    pub fn payload(&self) -> &Bytes {
        &self.0
    }

    pub fn into_payload(self) -> Bytes {
        self.0
    }

    /// The bytes to put on the connection: the length prefix and the payload.
    ///
    /// # Panics
    ///
    /// When the payload is longer than the prefix can say, as
    /// [`write_to`](Self::write_to) does.
    pub fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(4 + self.0.len());
        self.write_to(&mut out);
        out.freeze()
    }

    /// Append the length prefix and the payload to `out`.
    ///
    /// # Panics
    ///
    /// When the payload is longer than `u32::MAX` bytes, which the prefix
    /// cannot say.
    pub fn write_to(&self, out: &mut BytesMut) {
        let len = u32::try_from(self.0.len()).expect("a frame payload of at most u32::MAX bytes");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.0);
    }
}

impl From<Bytes> for Frame {
    fn from(payload: Bytes) -> Self {
        Self(payload)
    }
}

/// Cuts the bytes of one direction of a connection into [`Frame`]s.
///
/// Bytes arrive in pieces of any size. Add them as they come with
/// [`extend`](Self::extend), or read from a socket straight into
/// [`buffer_mut`](Self::buffer_mut). Then take frames with
/// [`next_frame`](Self::next_frame) until it returns `None`.
#[derive(Debug)]
pub struct FrameDecoder {
    buffer: BytesMut,
    max_len: usize,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::with_max_len(Frame::MAX_LEN)
    }

    /// A decoder that rejects frames with a payload over `max_len` bytes.
    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_len,
        }
    }

    /// Add bytes that arrived.
    pub fn extend(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// The buffer itself, for a caller that reads from a socket straight into it.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Number of bytes that arrived and no frame has taken yet.
    /// When the connection closes, anything above zero is a frame cut short.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Take the next whole frame.
    ///
    /// Returns `None` while the next frame is incomplete. A length the
    /// decoder does not accept is an error as soon as the prefix is there,
    /// before the payload arrives. After an error the connection is of no
    /// use: there is no way to find the start of the next frame.
    ///
    /// An incomplete frame makes room in the buffer for as many bytes again
    /// as arrived, and for no more than the frame still misses. The buffer
    /// therefore never holds far more than a peer really sent.
    ///
    /// # Errors
    ///
    /// [`FrameError::Empty`] when the prefix announces zero bytes, and
    /// [`FrameError::TooLarge`] when it announces more than the decoder
    /// accepts.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        let Some(prefix) = self.buffer.first_chunk::<4>() else {
            return Ok(None);
        };
        let len = u32::from_be_bytes(*prefix) as usize;
        if len == 0 {
            return Err(FrameError::Empty);
        }
        if len > self.max_len {
            return Err(FrameError::TooLarge { len, max: self.max_len });
        }
        if self.buffer.len() < 4 + len {
            // The room for the rest grows with the bytes that arrived, and not with the length the prefix announces:
            // a client that sends four bytes does not make the decoder hold room for the largest frame.
            let missing = 4 + len - self.buffer.len();
            self.buffer.reserve(missing.min(self.buffer.len()));
            return Ok(None);
        }
        self.buffer.advance(4);
        Ok(Some(Frame(self.buffer.split_to(len).freeze())))
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &'static [u8]) -> Frame {
        Frame::new(Bytes::from_static(payload))
    }

    #[test]
    fn to_bytes_puts_the_length_in_front() {
        assert_eq!(frame(b"abc").to_bytes().as_ref(), &[0, 0, 0, 3, b'a', b'b', b'c']);
    }

    #[test]
    fn waits_for_the_whole_frame() {
        let bytes = frame(b"abcdef").to_bytes();
        let mut decoder = FrameDecoder::new();
        for byte in &bytes[..bytes.len() - 1] {
            decoder.extend(&[*byte]);
            assert_eq!(decoder.next_frame(), Ok(None));
        }
        decoder.extend(&bytes[bytes.len() - 1..]);
        assert_eq!(decoder.next_frame(), Ok(Some(frame(b"abcdef"))));
        assert_eq!(decoder.buffered(), 0);
    }

    #[test]
    fn takes_frames_one_at_a_time() {
        let mut decoder = FrameDecoder::new();
        frame(b"one").write_to(decoder.buffer_mut());
        frame(b"two").write_to(decoder.buffer_mut());
        decoder.extend(&[0, 0]);
        assert_eq!(decoder.next_frame(), Ok(Some(frame(b"one"))));
        assert_eq!(decoder.next_frame(), Ok(Some(frame(b"two"))));
        assert_eq!(decoder.next_frame(), Ok(None));
        assert_eq!(decoder.buffered(), 2);
    }

    #[test]
    fn rejects_an_oversized_frame_before_its_payload_arrives() {
        let mut decoder = FrameDecoder::with_max_len(8);
        decoder.extend(&9u32.to_be_bytes());
        assert_eq!(decoder.next_frame(), Err(FrameError::TooLarge { len: 9, max: 8 }));
    }

    #[test]
    fn a_length_alone_takes_no_room_for_its_payload() {
        let mut decoder = FrameDecoder::new();
        decoder.extend(&u32::try_from(Frame::MAX_LEN).unwrap().to_be_bytes());

        assert_eq!(decoder.next_frame(), Ok(None));
        assert!(
            decoder.buffer_mut().capacity() < 1024,
            "{} bytes for a frame of which four bytes arrived",
            decoder.buffer_mut().capacity()
        );
    }

    #[test]
    fn the_room_for_a_frame_grows_with_the_bytes_that_arrived() {
        let payload = vec![7u8; 64 * 1024];
        let bytes = Frame::new(payload.clone()).to_bytes();
        let mut decoder = FrameDecoder::new();

        for piece in bytes.chunks(1000) {
            assert_eq!(decoder.next_frame(), Ok(None));
            // What the decoder holds ready is never far more than what arrived.
            assert!(decoder.buffer_mut().capacity() <= 4 * decoder.buffered().max(64));
            decoder.extend(piece);
        }

        assert_eq!(decoder.next_frame(), Ok(Some(Frame::new(payload))));
    }

    #[test]
    fn rejects_a_zero_length_frame() {
        let mut decoder = FrameDecoder::new();
        decoder.extend(&0u32.to_be_bytes());
        assert_eq!(decoder.next_frame(), Err(FrameError::Empty));
    }
}
