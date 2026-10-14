use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{ColumnChunkMetaData, PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::ChunkReader;
use parquet::file::writer::SerializedFileWriter;
use pgrx::check_for_interrupts;

use crate::arrow_parquet::parquet_multipart::MultipartUploadWriter;
use crate::arrow_parquet::uri_utils::ParsedUriInfo;

// concat_parquet_files splices the row groups of the given parquet files into a single
// parquet file, in the given order. The column chunks are copied as encoded bytes, so no
// decoding or re-encoding happens and the per column chunk settings (compression, encoding,
// statistics) of the inputs survive. All inputs must have the same schema. It returns the
// total number of rows written.
//
// Based on parquet's own parquet-concat binary.
pub(crate) fn concat_parquet_files(inputs: &[String], output: &str) -> Result<i64, String> {
    let output_file =
        File::create(output).map_err(|e| format!("failed to create {output}: {e}"))?;

    let (total_rows, _) = concat_parquet_files_into(inputs, output_file, output)?;

    Ok(total_rows)
}

// concat_parquet_files_to_uri splices the row groups of the given parquet files into a
// single parquet file in an object store, in the given order, as a multipart upload. The
// spliced file is streamed into the upload rather than written to local disk first, and the
// parts are uploaded while the splice that feeds them is still running.
pub(crate) fn concat_parquet_files_to_uri(
    inputs: &[String],
    uri_info: &ParsedUriInfo,
) -> Result<i64, String> {
    let uri = uri_info.uri.to_string();

    // the spliced file is the parts minus their footers plus one footer, so the parts are a
    // good enough estimate to size the upload's parts with
    let expected_bytes = inputs
        .iter()
        .map(|path| std::fs::metadata(path).map(|metadata| metadata.len() as usize))
        .sum::<Result<usize, _>>()
        .map_err(|e| format!("failed to stat the parquet parts of {uri}: {e}"))?;

    let writer = MultipartUploadWriter::new(uri_info, expected_bytes)?;

    let (total_rows, writer) = concat_parquet_files_into(inputs, writer, &uri)?;

    writer.finish()?;

    Ok(total_rows)
}

// concat_parquet_files_into splices the row groups into the given sink and hands the sink
// back, so that a sink which has to be finished explicitly still can be. output only names
// the destination in the error messages.
fn concat_parquet_files_into<W: Write + Send>(
    inputs: &[String],
    sink: W,
    output: &str,
) -> Result<(i64, W), String> {
    if inputs.is_empty() {
        return Err("no input files to concatenate".into());
    }

    let inputs = inputs
        .iter()
        .map(|path| {
            let reader = File::open(path).map_err(|e| format!("failed to open {path}: {e}"))?;

            let metadata = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::Optional)
                .parse_and_finish(&reader)
                .map_err(|e| format!("failed to read parquet metadata of {path}: {e}"))?;

            Ok((reader, metadata))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let expected_schema = inputs[0].1.file_metadata().schema();
    for (_, metadata) in inputs.iter().skip(1) {
        if metadata.file_metadata().schema() != expected_schema {
            return Err("parquet parts do not have the same schema".into());
        }
    }

    // the key value metadata (the encoded arrow schema among others) and the format version
    // only live in the parts, so they have to be carried over to the concatenated file
    let key_value_metadata = inputs[0].1.file_metadata().key_value_metadata().cloned();

    let writer_version = match inputs[0].1.file_metadata().version() {
        2 => WriterVersion::PARQUET_2_0,
        _ => WriterVersion::PARQUET_1_0,
    };

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_created_by("pg_parquet".to_string())
            .set_key_value_metadata(key_value_metadata)
            .set_writer_version(writer_version)
            .build(),
    );

    let schema = inputs[0].1.file_metadata().schema_descr().root_schema_ptr();

    let mut writer = SerializedFileWriter::new(sink, schema, writer_props)
        .map_err(|e| format!("failed to create parquet writer for {output}: {e}"))?;

    let mut total_rows = 0;

    for (input, metadata) in inputs {
        let page_index = metadata.page_index();

        for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
            // the splice writes the whole output and is the only phase of a parallel COPY TO
            // that runs no executor, so a cancellation is only noticed here
            check_for_interrupts!();

            let mut row_group_writer = writer
                .next_row_group()
                .map_err(|e| format!("failed to start a row group in {output}: {e}"))?;

            for (column_idx, column) in row_group.columns().iter().enumerate() {
                let close_result = ColumnCloseResult {
                    bytes_written: column.compressed_size() as _,
                    rows_written: row_group.num_rows() as _,
                    metadata: column.clone(),
                    bloom_filter: read_bloom_filter(column, &input),
                    column_index: page_index
                        .and_then(|page_index| page_index.column_index(row_group_idx, column_idx))
                        .cloned(),
                    offset_index: page_index
                        .and_then(|page_index| page_index.offset_index(row_group_idx, column_idx))
                        .cloned(),
                };

                row_group_writer
                    .append_column(&input, close_result)
                    .map_err(|e| format!("failed to append a column chunk to {output}: {e}"))?;
            }

            row_group_writer
                .close()
                .map_err(|e| format!("failed to close a row group in {output}: {e}"))?;

            total_rows += row_group.num_rows();
        }
    }

    // into_inner writes the footer and hands the sink back, where close would drop it
    let sink = writer
        .into_inner()
        .map_err(|e| format!("failed to close {output}: {e}"))?;

    Ok((total_rows, sink))
}

fn read_bloom_filter<R: ChunkReader>(column: &ColumnChunkMetaData, input: &R) -> Option<Sbbf> {
    Sbbf::read_from_column_chunk(column, input).ok().flatten()
}
