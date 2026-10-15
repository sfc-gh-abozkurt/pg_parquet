#[pgrx::pg_schema]
mod tests {
    use std::sync::mpsc::channel;
    use std::thread::Builder;

    use pgrx::{pg_test, Spi};

    use crate::parquet_copy_hook::copy_process_guard::{children, foreign_threads};
    use crate::pgrx_tests::common::{FileCleanup, LOCAL_TEST_FILE_PATH};

    // a thread that the extension did not create has to be found, otherwise the checks that
    // the copies of this module pass are no checks at all
    #[pg_test]
    fn test_foreign_thread_is_found() {
        let (stop, wait_for_stop) = channel::<()>();

        // the thread exists as soon as it was spawned, and it stays around until it is told
        // to stop, so what the check reports about it does not race with it
        let thread = Builder::new()
            .name("not_pg_parquet".into())
            .spawn(move || {
                let _ = wait_for_stop.recv();
            })
            .expect("failed to spawn a thread");

        let found = foreign_threads();

        stop.send(()).expect("the thread is gone already");
        thread.join().expect("the thread panicked");

        // a release build does not look at the threads at all, and neither does a platform
        // other than linux
        if cfg!(all(debug_assertions, target_os = "linux")) {
            assert!(
                found.iter().any(|thread| thread.contains("not_pg_parquet")),
                "the foreign thread was not found among {found:?}"
            );
        }

        assert_eq!(foreign_threads(), Vec::<String>::new());
    }

    // a thread that another extension started is already there when pg_parquet is entered, and
    // the extension does not answer for it: a copy has to go through rather than fail over a
    // thread it did not create and cannot stop
    #[pg_test]
    fn test_copy_runs_with_a_pre_existing_foreign_thread() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        Spi::run(
            "CREATE TABLE pre_existing_thread_table AS SELECT i FROM generate_series(1, 100) i;",
        )
        .unwrap();

        let (stop, wait_for_stop) = channel::<()>();

        // the thread is up before the first copy starts and stays up past the last one, which
        // is what another extension's thread pool looks like to the check
        let thread = Builder::new()
            .name("not_pg_parquet".into())
            .spawn(move || {
                let _ = wait_for_stop.recv();
            })
            .expect("failed to spawn a thread");

        let copies = format!(
            "
            COPY pre_existing_thread_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);
            COPY pre_existing_thread_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);
            CREATE TABLE pre_existing_thread_copy () WITH (load_from = '{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.schema('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.file_metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.kv_metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.column_stats('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.list('{LOCAL_TEST_FILE_PATH}');
        "
        );

        let result = Spi::run(&copies);

        stop.send(()).expect("the thread is gone already");
        thread.join().expect("the thread panicked");

        result.expect("a foreign thread that was already there failed a copy");
    }

    // the udfs and the schema inference of a CREATE TABLE read a parquet file through the same
    // object store calls a copy does, so they are checked the same way
    #[pg_test]
    fn test_udfs_and_create_table_from_parquet_stay_in_the_backend() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        Spi::run("CREATE TABLE udf_backend_only_table AS SELECT i, i::text AS t FROM generate_series(1, 1000) i;").unwrap();

        let children_before = children();

        let reads = format!(
            "
            COPY udf_backend_only_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);
            SELECT count(*) FROM parquet.schema('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.file_metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.kv_metadata('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.column_stats('{LOCAL_TEST_FILE_PATH}');
            SELECT count(*) FROM parquet.list('{LOCAL_TEST_FILE_PATH}');
            CREATE TABLE udf_backend_only_inferred () WITH (definition_from = '{LOCAL_TEST_FILE_PATH}');
            CREATE TABLE udf_backend_only_loaded () WITH (load_from = '{LOCAL_TEST_FILE_PATH}');
        "
        );

        Spi::run(&reads).unwrap();

        assert_eq!(foreign_threads(), Vec::<String>::new());
        assert_eq!(children(), children_before);
    }

    // every copy of the suite is checked by the assertion of copy_process_guard, and this test
    // is the one that says what the assertion is about
    #[pg_test]
    fn test_copy_to_and_from_parquet_stays_in_the_backend() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        Spi::run("CREATE TABLE backend_only_table AS SELECT i, i::text AS t FROM generate_series(1, 100000) i;").unwrap();

        let children_before = children();

        for workers in [0, 2] {
            Spi::run(&format!(
                "SET pg_parquet.max_parallel_workers TO {workers};"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY backend_only_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet, row_group_size 10000);"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY backend_only_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();
        }

        // the object store calls are the ones that run on a thread of the tokio runtime, and
        // the runtime names its threads after the extension, so they are not foreign ones
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");
        let s3_uri = format!("s3://{test_bucket_name}/copy_process_guard/backend_only.parquet");

        Spi::run(&format!(
            "COPY backend_only_table TO '{s3_uri}' WITH (format parquet, row_group_size 10000);"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY backend_only_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(foreign_threads(), Vec::<String>::new());
        assert_eq!(children(), children_before);
    }
}
