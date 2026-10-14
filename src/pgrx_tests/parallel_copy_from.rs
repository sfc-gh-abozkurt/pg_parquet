#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::FileCleanup;

    // the tests of this module keep to their own paths rather than the file path the rest of
    // the suite shares, because a parallel worker reads the file while the command runs and
    // another test removing it in between turns into a confusing failure
    const LOCAL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_parallel_from.parquet";

    const WIDE_TABLE_ROWS: i64 = 100_000;

    // pg_parquet only splits a COPY FROM between the row groups of the file, so the test file
    // has to hold more than one of them
    const ROW_GROUP_SIZE: i64 = 10_000;

    fn create_wide_table(name: &str) {
        Spi::run(&format!(
            "CREATE TABLE {name} (
                 id bigint,
                 name text,
                 amount numeric(12,4),
                 created_at timestamp,
                 flag bool,
                 payload bytea,
                 tags int[],
                 doc jsonb
             );"
        ))
        .unwrap();
    }

    // write_wide_file writes the file that the COPY FROM tests read, and returns the number of
    // row groups it holds
    fn write_wide_file() -> i64 {
        write_wide_file_at(LOCAL_TEST_FILE_PATH)
    }

    fn write_wide_file_at(uri: &str) -> i64 {
        create_wide_table("wide_table");

        Spi::run(&format!(
            "INSERT INTO wide_table
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL ELSE 'name-' || g || repeat('x', g % 50) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 100000)::numeric / 10000 END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE timestamp '2024-01-01' + (g || ' seconds')::interval END,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE g % 2 = 0 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE decode(lpad(to_hex(g), 8, '0'), 'hex') END,
                    CASE WHEN g % 19 = 0 THEN NULL ELSE ARRAY[g, g + 1, g + 2] END,
                    CASE WHEN g % 23 = 0 THEN NULL ELSE jsonb_build_object('k', g) END
             FROM generate_series(1, {WIDE_TABLE_ROWS}) g;"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{uri}'
             WITH (format parquet, row_group_size {ROW_GROUP_SIZE});"
        ))
        .unwrap();

        let num_row_groups = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.metadata('{uri}')
             WHERE column_id = 0"
        ))
        .unwrap()
        .unwrap();

        assert!(
            num_row_groups > 1,
            "the test file holds {num_row_groups} row groups, which is too few to be copied \
             in parallel"
        );

        num_row_groups
    }

    fn count(relation: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT count(*) FROM {relation}"))
            .unwrap()
            .unwrap()
    }

    // rows_not_in is the number of rows of the first relation that the second one does not
    // have, and the other way round. Both have to be 0 for the two to hold exactly the same
    // rows.
    fn rows_not_in(left: &str, right: &str) -> (i64, i64) {
        let missing = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}) q"
        ))
        .unwrap()
        .unwrap();

        let extra = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM (SELECT * FROM {right} EXCEPT ALL SELECT * FROM {left}) q"
        ))
        .unwrap()
        .unwrap();

        (missing, extra)
    }

    #[pg_test]
    fn test_parallel_copy_from_matches_sequential() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        // the rows of the file are inserted in the order the file holds them, the same order
        // the single process path inserts them in, even though several processes decode them
        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM sequential_table) s
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM parallel_table) p
                 USING (rn)
             WHERE s.id IS DISTINCT FROM p.id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(out_of_order, 0);
    }

    #[pg_test]
    fn test_parallel_copy_from_with_attribute_list() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run("CREATE TABLE subset_table (id bigint, amount numeric(12,4), doc jsonb);")
            .unwrap();
        Spi::run(&format!(
            "COPY subset_table (id, amount, doc) FROM '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, match_by 'name');"
        ))
        .unwrap();

        assert_eq!(count("subset_table"), WIDE_TABLE_ROWS);

        let diff = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT id, amount, doc FROM wide_table
                 EXCEPT ALL
                 SELECT id, amount, doc FROM subset_table
             ) q",
        )
        .unwrap()
        .unwrap();
        assert_eq!(diff, 0);
    }

    // the WHERE clause of the command filters the rows the parallel workers decode, the same
    // way it filters the ones the single process path decodes
    #[pg_test]
    fn test_parallel_copy_from_with_where_clause() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("filtered_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY filtered_table FROM '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet) WHERE id % 100 = 0;"
        ))
        .unwrap();

        assert_eq!(count("filtered_table"), WIDE_TABLE_ROWS / 100);
        assert_eq!(
            rows_not_in(
                "filtered_table",
                "(SELECT * FROM wide_table WHERE id % 100 = 0) w"
            ),
            (0, 0)
        );
    }

    // a COPY FROM that the parallel path cannot handle still has to insert every row, through
    // the single process path
    #[pg_test]
    fn test_parallel_copy_from_fallbacks() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // a file with a single row group, which there is nothing to split
        {
            let single_row_group_file =
                "/tmp/pg_parquet_test_parallel_from_single_row_group.parquet";
            let _file = FileCleanup::new(single_row_group_file);

            Spi::run("CREATE TABLE narrow_table AS SELECT g id FROM generate_series(1, 100) g;")
                .unwrap();
            Spi::run(&format!(
                "COPY narrow_table TO '{single_row_group_file}' WITH (format parquet);"
            ))
            .unwrap();

            Spi::run("TRUNCATE narrow_table;").unwrap();
            Spi::run(&format!(
                "COPY narrow_table FROM '{single_row_group_file}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("narrow_table"), 100);
        }

        // a temporary relation, which a parallel worker cannot see
        {
            create_wide_table("temp_holder");
            Spi::run("CREATE TEMP TABLE temp_table (LIKE temp_holder);").unwrap();
            Spi::run(&format!(
                "COPY temp_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("temp_table"), WIDE_TABLE_ROWS);
        }

        // a relation with a row trigger, which has to see every row it is inserted
        {
            create_wide_table("trigger_table");
            Spi::run(
                "CREATE FUNCTION count_rows() RETURNS trigger AS $$
                 BEGIN
                     INSERT INTO trigger_counter VALUES (1);
                     RETURN NEW;
                 END
                 $$ LANGUAGE plpgsql;",
            )
            .unwrap();
            Spi::run("CREATE TABLE trigger_counter (n int);").unwrap();
            Spi::run(
                "CREATE TRIGGER count_rows BEFORE INSERT ON trigger_table
                 FOR EACH ROW EXECUTE FUNCTION count_rows();",
            )
            .unwrap();

            Spi::run(&format!(
                "COPY trigger_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("trigger_table"), WIDE_TABLE_ROWS);
            assert_eq!(count("trigger_counter"), WIDE_TABLE_ROWS);
        }

        // a relation with a default expression for an attribute the command does not name,
        // which only the process that holds the command can evaluate
        {
            create_wide_table("default_table");
            Spi::run("ALTER TABLE default_table ADD COLUMN extra int DEFAULT 7;").unwrap();

            Spi::run(&format!(
                "COPY default_table (id, name, amount, created_at, flag, payload, tags, doc)
                 FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("default_table"), WIDE_TABLE_ROWS);
            assert_eq!(count("default_table WHERE extra = 7"), WIDE_TABLE_ROWS);
        }

        // a uri with a wildcard, which names more than one file
        {
            let pattern_folder = "/tmp/pg_parquet_test_parallel_from_dir";
            let pattern_file = format!("{pattern_folder}/wide.parquet");
            let _folder = FileCleanup::new(pattern_folder);

            Spi::run(&format!(
                "COPY wide_table TO '{pattern_file}'
                 WITH (format parquet, row_group_size {ROW_GROUP_SIZE});"
            ))
            .unwrap();

            create_wide_table("pattern_table");
            Spi::run(&format!(
                "COPY pattern_table FROM '{pattern_folder}/*.parquet' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("pattern_table"), WIDE_TABLE_ROWS);
        }
    }

    // the leader decodes every row group itself when no parallel worker can be launched
    #[pg_test]
    fn test_parallel_copy_from_without_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("no_worker_table");

        Spi::run("SET max_parallel_workers TO 0;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY no_worker_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("no_worker_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "no_worker_table"), (0, 0));
    }

    // a request above the server's own max_parallel_workers is capped to it, because a COPY
    // FROM that cannot launch every worker it asked for reads the whole file in the leader
    #[pg_test]
    fn test_parallel_copy_from_above_server_max_parallel_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("capped_worker_table");

        Spi::run("SET max_parallel_workers TO 1;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY capped_worker_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("capped_worker_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "capped_worker_table"), (0, 0));
    }

    // an error a parallel worker raises has to reach the client rather than leave the leader
    // waiting for rows that are never coming
    #[pg_test]
    #[should_panic(expected = "column count mismatch between table and parquet file")]
    fn test_parallel_copy_from_worker_error() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run("CREATE TABLE mismatched_table (a text, b text);").unwrap();

        Spi::run(&format!(
            "COPY mismatched_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // a worker of a parallel COPY FROM an object store uri reads its own row groups out of
    // the uri itself, so this covers that a worker reaches the object store at all, and that
    // the rows several workers read out of one object still land in the file's own order
    #[pg_test]
    fn test_s3_parallel_copy_from_matches_sequential() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/parallel_copy_from/wide.parquet");

        write_wide_file_at(&s3_uri);

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM sequential_table) s
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM parallel_table) p
                 USING (rn)
             WHERE s.id IS DISTINCT FROM p.id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(out_of_order, 0);
    }
}
