use std::io::{Error as IoError, Result as IoResult, Write};

use bytes::Bytes;
use object_store::{ObjectStoreExt, WriteMultipart};

use crate::{
    arrow_parquet::uri_utils::ParsedUriInfo,
    object_store::{object_store_cache::get_or_create_object_store, object_store_error_message},
    PG_BACKEND_TOKIO_RUNTIME,
};

// the object stores require every part of a multipart upload except the last one to be at
// least 5MiB, and a larger part trades a bigger buffer for fewer requests
const MIN_PART_SIZE: usize = 8 * 1024 * 1024;

// the object stores cap a multipart upload at 10000 parts, so an upload that would need
// more than this many parts gets larger parts instead. The slack below the cap leaves room
// for the final part and for an output that turns out larger than the parts it is spliced
// from.
const MAX_PARTS: usize = 9000;

// the number of parts that may be uploaded at once. The backend's tokio runtime runs on the
// backend's own thread, so an in flight part makes progress while the writer waits for
// capacity, which is what overlaps the upload with the splice that feeds it.
const MAX_CONCURRENCY: usize = 4;

// MultipartUploadWriter writes a byte stream into an object store as a multipart upload. It
// is a std Write, so a parquet writer can stream a file into an object store without the
// file ever being written to local disk, and without the whole file being held in memory.
pub(crate) struct MultipartUploadWriter {
    // None once the upload is finished or aborted
    upload: Option<WriteMultipart>,
    part: Vec<u8>,
    part_size: usize,
    uri: String,
}

impl MultipartUploadWriter {
    // new starts a multipart upload to the given uri. expected_bytes only sizes the parts,
    // so an upload that ends up larger than expected is still written correctly.
    pub(crate) fn new(uri_info: &ParsedUriInfo, expected_bytes: usize) -> Result<Self, String> {
        let copy_from = false;
        let (object_store, location) = get_or_create_object_store(uri_info, copy_from);

        let part_size = Self::part_size(expected_bytes);

        let upload = PG_BACKEND_TOKIO_RUNTIME
            .block_on(object_store.put_multipart(&location))
            .map_err(object_store_error_message)?;

        Ok(Self {
            upload: Some(WriteMultipart::new_with_chunk_size(upload, part_size)),
            part: Vec::with_capacity(part_size),
            part_size,
            uri: uri_info.uri.to_string(),
        })
    }

    fn part_size(expected_bytes: usize) -> usize {
        let part_size = expected_bytes
            .div_ceil(MAX_PARTS)
            .next_multiple_of(1024 * 1024);

        part_size.max(MIN_PART_SIZE)
    }

    // finish uploads what is left in the current part and completes the upload. The object
    // only becomes visible to a reader once the upload is completed.
    pub(crate) fn finish(mut self) -> Result<(), String> {
        let mut upload = self
            .upload
            .take()
            .expect("multipart upload was already finished");

        if !self.part.is_empty() {
            if let Err(e) = Self::put_part(&mut upload, std::mem::take(&mut self.part)) {
                // hand the upload back so that the drop below aborts it
                self.upload = Some(upload);

                return Err(e);
            }
        }

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(upload.finish())
            .map_err(object_store_error_message)
            .map_err(|e| format!("failed to upload \"{}\": {e}", self.uri))?;

        Ok(())
    }

    // put_part waits until an upload slot is free and then hands the part to the runtime.
    // The wait is what keeps the memory the in flight parts take bounded, and it is also
    // the only point at which the already running uploads are driven forward.
    fn put_part(upload: &mut WriteMultipart, part: Vec<u8>) -> Result<(), String> {
        PG_BACKEND_TOKIO_RUNTIME.block_on(async {
            upload
                .wait_for_capacity(MAX_CONCURRENCY)
                .await
                .map_err(object_store_error_message)?;

            upload.put(Bytes::from(part));

            Ok(())
        })
    }
}

impl Write for MultipartUploadWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| IoError::other("multipart upload was already finished"))?;

        let taken = buf.len().min(self.part_size - self.part.len());
        self.part.extend_from_slice(&buf[..taken]);

        // a part is only handed over once it is full, because the object stores reject a
        // part below the minimum size unless it is the last one
        if self.part.len() == self.part_size {
            let part = std::mem::replace(&mut self.part, Vec::with_capacity(self.part_size));

            Self::put_part(upload, part).map_err(IoError::other)?;
        }

        Ok(taken)
    }

    fn flush(&mut self) -> IoResult<()> {
        // the current part cannot be handed over before it is full, see write
        Ok(())
    }
}

impl Drop for MultipartUploadWriter {
    fn drop(&mut self) {
        // an upload that was neither finished nor aborted leaves its parts behind in the
        // object store, which the object stores charge for until a lifecycle rule removes
        // them, so an upload the writer did not get to finish is aborted here
        if let Some(upload) = self.upload.take() {
            let _ = PG_BACKEND_TOKIO_RUNTIME.block_on(upload.abort());
        }
    }
}
