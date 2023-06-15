use snap::raw::{decompress_len, max_compress_len, Decoder, Encoder};
use snap::Result as SnapResult;

#[derive(Default)]
pub struct SnappyCodec {}

impl SnappyCodec {
    /// Decompresses data stored in slice `input_buf` and appends output to `output_buf`.
    ///
    /// If the uncompress_size is provided it will allocate the exact amount of memory.
    /// Otherwise, it will estimate the uncompressed size, allocating an amount of memory
    /// greater or equal to the real uncompress_size.
    ///
    /// Returns the total number of bytes written.
    pub fn decompress(
        &self,
        input_buf: &[u8],
        output_buf: &mut Vec<u8>,
        uncompress_size: Option<usize>,
    ) -> SnapResult<usize> {
        let len = match uncompress_size {
            Some(size) => size,
            None => decompress_len(input_buf)?,
        };
        let offset = output_buf.len();
        output_buf.resize(offset + len, 0);
        let mut decoder = Decoder::new();
        decoder.decompress(input_buf, &mut output_buf[offset..])
    }

    /// Compresses data stored in slice `input_buf` and appends the compressed result
    /// to `output_buf`.
    ///
    /// Note that you'll need to call `clear()` before reusing the same `output_buf`
    /// across different `compress` calls.
    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> SnapResult<()> {
        let output_buf_len = output_buf.len();
        let required_len = max_compress_len(input_buf.len());
        output_buf.resize(output_buf_len + required_len, 0);
        let mut encoder = Encoder::new();
        let n = encoder.compress(input_buf, &mut output_buf[output_buf_len..])?;
        output_buf.truncate(output_buf_len + n);
        Ok(())
    }
}
