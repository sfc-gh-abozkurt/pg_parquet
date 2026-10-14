#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{
        extension_exists, extension_version, geospatial_crs_by_column_of, FileCleanup,
        LOCAL_TEST_FILE_PATH,
    };

    const SEQUENTIAL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_sequential.parquet";
    const GEO_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_parallel_geo.parquet";
    const GEO_SEQUENTIAL_TEST_FILE_PATH: &str =
        "/tmp/pg_parquet_test_parallel_geo_sequential.parquet";

    // pg_parquet only splits a COPY TO into ctid ranges above a minimum number of pages, so
    // the test table has to be wide enough to get there
    const WIDE_TABLE_ROWS: i64 = 100_000;
    const MIN_PARALLEL_BLOCKS: i64 = 1024;

    fn create_wide_table() {
        Spi::run(
            "CREATE TABLE wide_table (
                 id bigint,
                 name text,
                 amount numeric(12,4),
                 created_at timestamp,
                 flag bool,
                 payload bytea
             );",
        )
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO wide_table
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL ELSE 'name-' || g || repeat('x', g % 50) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 100000)::numeric / 10000 END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE timestamp '2024-01-01' + (g || ' seconds')::interval END,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE g % 2 = 0 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE decode(lpad(to_hex(g), 8, '0'), 'hex') END
             FROM generate_series(1, {WIDE_TABLE_ROWS}) g;"
        ))
        .unwrap();

        let nblocks = Spi::get_one::<i64>("SELECT pg_relation_size('wide_table') / 8192")
            .unwrap()
            .unwrap();

        assert!(
            nblocks >= MIN_PARALLEL_BLOCKS,
            "the test table has {nblocks} pages, which is too few to be copied in parallel"
        );
    }

    // pg_parquet only splits a COPY TO into ctid ranges above a minimum number of pages, so
    // the geometry table has to be wide enough to get there too
    const GEO_TABLE_ROWS: i64 = 200_000;

    fn create_geo_table() {
        Spi::run(
            "CREATE TABLE geo_table (
                 id bigint,
                 no_srid geometry,
                 lon_lat geometry(point, 4326),
                 web_mercator geometry(point, 3857),
                 geog geography
             );",
        )
        .unwrap();

        // the geometries of a column spread over the whole range, and are of more than one
        // geometry type, so that a range of the heap cannot hold all of the bounding box or
        // all of the geometry types of the column by itself
        Spi::run(&format!(
            "INSERT INTO geo_table
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL
                         WHEN g % 3 = 0 THEN ST_MakeLine(ST_MakePoint(g % 180, g % 90),
                                                         ST_MakePoint(-(g % 180), -(g % 90)))
                         ELSE ST_MakePoint(g % 180, g % 90) END,
                    CASE WHEN g % 11 = 0 THEN NULL
                         ELSE ST_SetSRID(ST_MakePoint(g % 180, g % 90), 4326) END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE ST_SetSRID(ST_MakePoint(g % 1000, g % 1000), 3857) END,
                    CASE WHEN g % 17 = 0 THEN NULL
                         ELSE ST_MakePoint(g % 180, g % 90)::geography END
             FROM generate_series(1, {GEO_TABLE_ROWS}) g;"
        ))
        .unwrap();

        let nblocks = Spi::get_one::<i64>("SELECT pg_relation_size('geo_table') / 8192")
            .unwrap()
            .unwrap();

        assert!(
            nblocks >= MIN_PARALLEL_BLOCKS,
            "the geometry table has {nblocks} pages, which is too few to be copied in parallel"
        );
    }

    fn row_groups(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(DISTINCT row_group_id) FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    fn max_row_group_rows(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT max(row_group_num_rows) FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // bounding_box is the box that covers the bounding boxes of every row group of the column
    fn bounding_box(uri: &str, column: &str) -> Option<String> {
        Spi::get_one::<String>(&format!(
            "SELECT min((stats_geospatial->'bbox'->>'xmin')::float8)::text || ' ' ||
                    max((stats_geospatial->'bbox'->>'xmax')::float8)::text || ' ' ||
                    min((stats_geospatial->'bbox'->>'ymin')::float8)::text || ' ' ||
                    max((stats_geospatial->'bbox'->>'ymax')::float8)::text
             FROM parquet.metadata('{uri}')
             WHERE path_in_schema = '{column}'"
        ))
        .unwrap()
    }

    // geospatial_types is the geometry types that the row groups of the column report
    fn geospatial_types(uri: &str, column: &str) -> Option<String> {
        Spi::get_one::<String>(&format!(
            "SELECT array_agg(DISTINCT t::int ORDER BY t::int)::text
             FROM parquet.metadata('{uri}') m,
                  jsonb_array_elements_text(m.stats_geospatial->'geospatial_types') t
             WHERE m.path_in_schema = '{column}'"
        ))
        .unwrap()
    }

    // rows_not_in_other_file reads both files into a table of the geometry table's own shape
    // and counts the rows of each that the other does not have
    fn rows_not_in_other_file(left_uri: &str, right_uri: &str) -> (i64, i64) {
        for (table, uri) in [("geo_left", left_uri), ("geo_right", right_uri)] {
            Spi::run(&format!("CREATE TABLE {table} (LIKE geo_table);")).unwrap();
            Spi::run(&format!("COPY {table} FROM '{uri}' WITH (format parquet);")).unwrap();
        }

        let not_in_right = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM geo_left
                                   EXCEPT ALL
                                   SELECT * FROM geo_right) q",
        )
        .unwrap()
        .unwrap();

        let not_in_left = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM geo_right
                                   EXCEPT ALL
                                   SELECT * FROM geo_left) q",
        )
        .unwrap()
        .unwrap();

        Spi::run("DROP TABLE geo_left, geo_right;").unwrap();

        (not_in_right, not_in_left)
    }

    fn file_metadata(uri: &str) -> (i64, String) {
        Spi::get_two::<i64, String>(&format!(
            "SELECT num_rows, format_version FROM parquet.file_metadata('{uri}')"
        ))
        .map(|(num_rows, format_version)| (num_rows.unwrap(), format_version.unwrap()))
        .unwrap()
    }

    fn kv_metadata_fingerprint(uri: &str) -> String {
        Spi::get_one::<String>(&format!(
            "SELECT md5(string_agg(key || coalesce(value, ''), '|' ORDER BY key))
             FROM parquet.kv_metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    fn schema_fingerprint(uri: &str) -> String {
        Spi::get_one::<String>(&format!(
            "SELECT md5(string_agg(name || coalesce(type_name, '') ||
                                   coalesce(repetition_type, '') ||
                                   coalesce(logical_type, '') ||
                                   coalesce(scale::text, '') ||
                                   coalesce(precision::text, ''), '|'))
             FROM parquet.schema('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // rows_not_in_file is the number of rows of the table that the parquet file does not
    // have, and the other way round. Both have to be 0 for the file to hold exactly the
    // rows of the table.
    fn rows_not_in_file(uri: &str) -> (i64, i64) {
        Spi::run("CREATE TABLE wide_table_from_file (LIKE wide_table);").unwrap();

        Spi::run(&format!(
            "COPY wide_table_from_file FROM '{uri}' WITH (format parquet);"
        ))
        .unwrap();

        let missing_from_file = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM wide_table
                                   EXCEPT ALL
                                   SELECT * FROM wide_table_from_file) q",
        )
        .unwrap()
        .unwrap();

        let extra_in_file = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM wide_table_from_file
                                   EXCEPT ALL
                                   SELECT * FROM wide_table) q",
        )
        .unwrap()
        .unwrap();

        Spi::run("DROP TABLE wide_table_from_file;").unwrap();

        (missing_from_file, extra_in_file)
    }

    fn compressed_size(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT sum(total_compressed_size)::bigint FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // part_files is the part files a parallel COPY TO a local uri left behind next to it. The
    // part file name holds the pid of the leader between the uri and the part infix.
    fn part_files(uri: &str) -> Vec<String> {
        let uri = std::path::Path::new(uri);
        let parent = uri.parent().unwrap();
        let prefix = format!("{}.", uri.file_name().unwrap().to_string_lossy());

        std::fs::read_dir(parent)
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.unwrap();
                let name = entry.file_name().to_string_lossy().to_string();

                (name.starts_with(&prefix) && name.contains(".pgpq-part-"))
                    .then(|| entry.path().display().to_string())
            })
            .collect()
    }

    // object_store_part_files is the part files a parallel COPY TO an object store uri left
    // behind in postgres' temporary file directory
    fn object_store_part_files() -> Vec<String> {
        let data_dir = Spi::get_one::<String>("SHOW data_directory")
            .unwrap()
            .unwrap();

        let temp_dir = std::path::Path::new(&data_dir)
            .join("base")
            .join("pgsql_tmp");

        let Ok(entries) = std::fs::read_dir(temp_dir) else {
            return Vec::new();
        };

        entries
            .filter_map(|entry| {
                let path = entry.unwrap().path().display().to_string();

                path.contains(".pgpq-part-").then_some(path)
            })
            .collect()
    }

    #[pg_test]
    fn test_parallel_copy_to_matches_sequential() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // the parallel file holds the same rows, with the same schema and the same key value
        // metadata, as the one the single process path writes
        assert_eq!(
            file_metadata(LOCAL_TEST_FILE_PATH),
            file_metadata(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            schema_fingerprint(LOCAL_TEST_FILE_PATH),
            schema_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            kv_metadata_fingerprint(LOCAL_TEST_FILE_PATH),
            kv_metadata_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // the part files are temporary and must not outlive the command
        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_with_options() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, compression zstd, compression_level 19,
                   parquet_version v2, row_group_size 10000,
                   row_group_size_bytes 1000000, dictionary 'all');"
        ))
        .unwrap();

        // the options of the command have to survive the splice of the part files
        assert_eq!(
            file_metadata(LOCAL_TEST_FILE_PATH),
            (WIDE_TABLE_ROWS, "2".to_string())
        );

        let codecs = Spi::get_one::<String>(&format!(
            "SELECT string_agg(DISTINCT split_part(compression, '(', 1), ',')
             FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(codecs, "ZSTD");

        // a row group never holds more rows than row_group_size asks for
        let max_row_group_rows = Spi::get_one::<i64>(&format!(
            "SELECT max(row_group_num_rows) FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        assert!(
            max_row_group_rows <= 10000,
            "a row group holds {max_row_group_rows} rows, more than row_group_size"
        );

        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // parquet only records the codec, not the level it was given, so the level is only
        // visible in how well the file compressed
        let parallel_compressed_size = compressed_size(LOCAL_TEST_FILE_PATH);

        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet, compression zstd, compression_level 1,
                   parquet_version v2, row_group_size 10000,
                   row_group_size_bytes 1000000, dictionary 'all');"
        ))
        .unwrap();

        assert!(
            parallel_compressed_size < compressed_size(SEQUENTIAL_TEST_FILE_PATH),
            "compression_level did not reach the parallel workers"
        );
    }

    #[pg_test]
    fn test_parallel_copy_to_with_attribute_list() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table (id, amount, payload) TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table (id, amount, payload) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            schema_fingerprint(LOCAL_TEST_FILE_PATH),
            schema_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);

        Spi::run(
            "CREATE TABLE wide_table_subset (id bigint, amount numeric(12,4), payload bytea);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY wide_table_subset FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let diff = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT id, amount, payload FROM wide_table
                 EXCEPT ALL
                 SELECT id, amount, payload FROM wide_table_subset
             ) q",
        )
        .unwrap()
        .unwrap();
        assert_eq!(diff, 0);
    }

    // a COPY TO that the parallel path cannot handle still has to write every row, through
    // the single process path
    #[pg_test]
    fn test_parallel_copy_to_fallbacks() {
        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // a table below the minimum number of pages
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run("CREATE TABLE narrow_table AS SELECT g id FROM generate_series(1, 100) g;")
                .unwrap();
            Spi::run(&format!(
                "COPY narrow_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, 100);
        }

        // a query rather than a relation
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY (SELECT id FROM wide_table WHERE id % 1000 = 0) TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(
                file_metadata(LOCAL_TEST_FILE_PATH).0,
                WIDE_TABLE_ROWS / 1000
            );
        }

        // a temporary relation, which a parallel worker cannot see
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run("CREATE TEMP TABLE temp_wide_table AS SELECT * FROM wide_table;").unwrap();
            Spi::run(&format!(
                "COPY temp_wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // file_size_bytes, which writes a folder of files rather than a single file
        {
            let split_folder = "/tmp/pg_parquet_test_split";
            let _folder = FileCleanup::new(split_folder);

            Spi::run(&format!(
                "COPY wide_table TO '{split_folder}'
                 WITH (format parquet, file_size_bytes '1MB');"
            ))
            .unwrap();

            let mut num_rows = 0;

            for file in std::fs::read_dir(split_folder).unwrap() {
                let file = file.unwrap().path().display().to_string();

                num_rows += file_metadata(&file).0;
            }

            assert_eq!(num_rows, WIDE_TABLE_ROWS);
        }
    }

    // the leader copies every ctid range itself when no parallel worker can be launched
    #[pg_test]
    fn test_parallel_copy_to_without_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET max_parallel_workers TO 0;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));
    }

    // a COPY TO an object store uri is spliced straight into a multipart upload, so this
    // also covers that the splice reaches the object store rather than local disk
    #[pg_test]
    fn test_s3_parallel_copy_to_matches_sequential() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let sequential_uri = format!("s3://{test_bucket_name}/parallel_copy_to/sequential.parquet");
        let parallel_uri = format!("s3://{test_bucket_name}/parallel_copy_to/parallel.parquet");

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(&parallel_uri), file_metadata(&sequential_uri));
        assert_eq!(
            schema_fingerprint(&parallel_uri),
            schema_fingerprint(&sequential_uri)
        );
        assert_eq!(
            kv_metadata_fingerprint(&parallel_uri),
            kv_metadata_fingerprint(&sequential_uri)
        );

        assert_eq!(file_metadata(&parallel_uri).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(&parallel_uri), (0, 0));

        // the part files are local files even for an object store destination, and they
        // must not outlive the command
        assert_eq!(object_store_part_files(), Vec::<String>::new());
    }
    // the part files of an object store destination are pg_parquet's own temporary files
    // rather than files the command names, so a user who may write to the object store gets
    // the fan-out even without the privilege to write the server's local files
    #[pg_test]
    fn test_s3_parallel_copy_to_without_local_file_privileges() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let parallel_uri =
            format!("s3://{test_bucket_name}/parallel_copy_to/object_store_only.parquet");

        create_wide_table();

        Spi::run(
            "CREATE USER object_store_only;
             GRANT parquet_object_store_write TO object_store_only;
             GRANT SELECT ON wide_table TO object_store_only;",
        )
        .unwrap();

        // the role the copy runs as may write to the object store and nothing else
        assert!(!Spi::get_one::<bool>(
            "SELECT pg_has_role('object_store_only', 'pg_write_server_files', 'USAGE')"
        )
        .unwrap()
        .unwrap());

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run("SET SESSION AUTHORIZATION object_store_only;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("RESET SESSION AUTHORIZATION;").unwrap();

        // a row group per ctid range is what the fan-out leaves behind, where the single
        // process path writes this table into a single row group
        assert!(
            row_groups(&parallel_uri) > 1,
            "the copy was not run in parallel"
        );

        assert_eq!(file_metadata(&parallel_uri).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(&parallel_uri), (0, 0));

        assert_eq!(object_store_part_files(), Vec::<String>::new());
    }

    // the part files of an object store destination take temporary space, so they are held
    // to the limit postgres holds the temporary files it writes itself to
    #[pg_test]
    #[should_panic(expected = "temporary file size exceeds temp_file_limit (64kB)")]
    fn test_s3_parallel_copy_to_respects_temp_file_limit() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let parallel_uri =
            format!("s3://{test_bucket_name}/parallel_copy_to/temp_file_limit.parquet");

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // a part file of this table is larger than this on its own
        Spi::run("SET temp_file_limit TO 64;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // a geospatial column is written through a crs that the column's postgis type carries and
    // through per row group bounding box statistics, both of which the leader has to splice out
    // of the part files, so the parallel path is checked against the single process path for a
    // table of geometries too
    #[pg_test]
    fn test_parallel_copy_to_geospatial_matches_sequential() {
        if !extension_exists("postgis") || *extension_version("postgis") < *"3.4" {
            return;
        }
        Spi::run("DROP EXTENSION IF EXISTS postgis CASCADE; CREATE EXTENSION postgis;").unwrap();

        let _parallel_file = FileCleanup::new(GEO_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(GEO_SEQUENTIAL_TEST_FILE_PATH);

        create_geo_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY geo_table TO '{GEO_SEQUENTIAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY geo_table TO '{GEO_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // the ranges are written by their own writers, so the parallel file is the one with
        // more row groups. without this the rest would also hold for a silent fallback to the
        // single process path
        assert!(
            row_groups(GEO_TEST_FILE_PATH) > row_groups(GEO_SEQUENTIAL_TEST_FILE_PATH),
            "the copy did not run in parallel"
        );

        assert_eq!(
            file_metadata(GEO_TEST_FILE_PATH),
            file_metadata(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            schema_fingerprint(GEO_TEST_FILE_PATH),
            schema_fingerprint(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            kv_metadata_fingerprint(GEO_TEST_FILE_PATH),
            kv_metadata_fingerprint(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(file_metadata(GEO_TEST_FILE_PATH).0, GEO_TABLE_ROWS);

        // the crs of each column survives the splice, and is the one the column's type says
        let crs_by_column = geospatial_crs_by_column_of(GEO_TEST_FILE_PATH);
        assert_eq!(
            crs_by_column,
            geospatial_crs_by_column_of(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            crs_by_column.get("no_srid"),
            Some(&Some("srid:0".to_string()))
        );
        assert_eq!(crs_by_column.get("lon_lat"), Some(&None));
        assert_eq!(
            crs_by_column.get("web_mercator"),
            Some(&Some("EPSG:3857".to_string()))
        );
        assert_eq!(crs_by_column.get("geog"), Some(&None));

        // the bounding box of a column covers the same area, and the same geometry types are
        // seen, however the rows are split into row groups
        for column in ["no_srid", "lon_lat", "web_mercator", "geog"] {
            assert_eq!(
                bounding_box(GEO_TEST_FILE_PATH, column),
                bounding_box(GEO_SEQUENTIAL_TEST_FILE_PATH, column),
                "the bounding box of {column} does not match"
            );
            assert_eq!(
                geospatial_types(GEO_TEST_FILE_PATH, column),
                geospatial_types(GEO_SEQUENTIAL_TEST_FILE_PATH, column),
                "the geometry types of {column} do not match"
            );
            assert!(
                bounding_box(GEO_TEST_FILE_PATH, column).is_some(),
                "{column} has no bounding box statistics"
            );
        }

        // a column that is not geospatial has no such statistics, which is what tells the
        // asserts above apart from a file that lost them
        assert_eq!(bounding_box(GEO_TEST_FILE_PATH, "id"), None);
        assert_eq!(geospatial_types(GEO_TEST_FILE_PATH, "id"), None);

        // both files read back as the same rows
        assert_eq!(
            rows_not_in_other_file(GEO_TEST_FILE_PATH, GEO_SEQUENTIAL_TEST_FILE_PATH),
            (0, 0)
        );

        assert_eq!(part_files(GEO_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_reaches_requested_row_group_size() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        // the ranges are sized from the relation's statistics, which an analyze puts there
        Spi::run("ANALYZE wide_table;").unwrap();

        // a tenth of the rows, so the relation holds ten row groups of the asked for size
        let row_group_size = WIDE_TABLE_ROWS / 10;

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {row_group_size});"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {row_group_size});"
        ))
        .unwrap();

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // a range is written into a part file of its own, so a range holding fewer rows than
        // the asked for row group size would leave every row group of the copy short of it
        assert_eq!(max_row_group_rows(LOCAL_TEST_FILE_PATH), row_group_size);
        assert_eq!(
            max_row_group_rows(LOCAL_TEST_FILE_PATH),
            max_row_group_rows(SEQUENTIAL_TEST_FILE_PATH)
        );

        // the rows are still spread over ranges rather than written by the leader alone
        assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_row_group_size_the_relation_cannot_fill() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("ANALYZE wide_table;").unwrap();

        // the relation cannot give a participant a row group of this size, so the copy writes
        // row groups short of it rather than giving up the fan-out for it
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {WIDE_TABLE_ROWS});"
        ))
        .unwrap();

        // a row group never runs over the asked for size, which is what the option bounds
        assert!(max_row_group_rows(LOCAL_TEST_FILE_PATH) <= WIDE_TABLE_ROWS);

        // the rows are still spread over ranges rather than written by the leader alone
        assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));
        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_directory_destination_errors_like_sequential() {
        create_wide_table();

        let directory = "/tmp/pg_parquet_test_parallel_dir";
        std::fs::create_dir_all(directory).unwrap();

        Spi::run(&format!(
            "CREATE FUNCTION copy_to_directory_error(workers int) RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 EXECUTE 'SET pg_parquet.max_parallel_workers TO ' || workers;

                 BEGIN
                     EXECUTE 'COPY wide_table TO ''{directory}'' WITH (format parquet)';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
             END;
             $$;"
        ))
        .unwrap();

        let sequential_error = Spi::get_one::<String>("SELECT copy_to_directory_error(0)")
            .unwrap()
            .unwrap();
        let parallel_error = Spi::get_one::<String>("SELECT copy_to_directory_error(4)")
            .unwrap()
            .unwrap();

        // the splice reports a destination it cannot create in its own words, so a
        // destination it cannot create at all is left to the single process path
        assert_eq!(parallel_error, sequential_error);
        assert!(
            sequential_error.contains("Is a directory"),
            "the single process path reported \"{sequential_error}\" instead"
        );

        std::fs::remove_dir_all(directory).unwrap();
    }
}
