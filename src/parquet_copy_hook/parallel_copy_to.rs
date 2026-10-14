use std::{
    cell::{Cell, RefCell},
    ffi::{c_char, c_int, CStr, CString},
    path::Path,
};

use object_store::ObjectStoreScheme;
use pgrx::{
    ereport, pg_guard,
    pg_sys::{
        self, defGetString, dsm_segment, pg_atomic_fetch_add_u32, pg_atomic_init_u32,
        pg_atomic_uint32, quote_identifier, quote_literal_cstr, quote_qualified_identifier,
        shm_toc, AsPgCStr, CmdType, CopyStmt, DataDir, DefElem, Node, NodeTag, PlannedStmt,
        RawStmt, HEAP_TABLE_AM_OID, RELKIND_RELATION, RELPERSISTENCE_TEMP,
    },
    GucSetting, PgBox, PgList, PgLogLevel, PgMemoryContexts, PgRelation, PgSqlErrorCode,
    PgTryBuilder,
};

use crate::{
    arrow_parquet::{
        parquet_concat::{concat_parquet_files, concat_parquet_files_to_uri},
        uri_utils::{uri_as_string, ParsedUriInfo},
    },
    parquet_copy_hook::{
        copy_to_split_dest_receiver::INVALID_FILE_SIZE_BYTES,
        copy_utils::{
            copy_stmt_attribute_list, copy_stmt_has_relation, copy_stmt_lock_mode,
            copy_stmt_relation_oid, copy_to_stmt_compression, copy_to_stmt_compression_level,
            copy_to_stmt_explicit_row_group_size, copy_to_stmt_file_size_bytes,
        },
        hook::process_copy_to_parquet,
        pg_compat::strVal,
    },
};

// MAX_PARALLEL_WORKERS is the number of parallel workers a single COPY TO parquet command
// may request. 0, the default, keeps COPY TO single process.
pub(crate) static MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) const MAX_PARALLEL_WORKERS_LIMIT: i32 = 64;

// requested_parallel_workers is how many workers a command may ask for. The server hands
// parallel workers out of the max_parallel_workers pool, so a request above it is never met
// in full, and a COPY FROM that is short of a single worker gives up on parallelism
// altogether, which makes a request above the pool slower than no request at all.
pub(crate) fn requested_parallel_workers() -> i32 {
    // read as copies, a reference to a mutable static is not allowed
    let (pool, slots) = unsafe { (pg_sys::max_parallel_workers, pg_sys::max_worker_processes) };

    MAX_PARALLEL_WORKERS.get().min(pool).min(slots)
}

// the number of ctid ranges a participant gets on average. More ranges than participants
// keeps everyone busy when the pages are not evenly filled, but every range is a part file
// that has to be spliced back in at the end.
const RANGES_PER_PARTICIPANT: u32 = 4;

// below this many pages the fan-out does not pay for the part files and the final splice
const MIN_PARALLEL_BLOCKS: u32 = 1024;

// what a part file is named after its prefix with, see part_file
const PART_FILE_INFIX: &str = ".pgpq-part-";

const PARALLEL_COPY_TO_SHARED_KEY: u64 = 0xBEEF_0001;

const PARALLEL_COPY_TO_WORKER_FUNCTION: &CStr = c"pg_parquet_parallel_copy_to_worker";

// placeholders that the participant which claims a ctid range fills in
const FIRST_BLOCK_PLACEHOLDER: &str = "@FIRST_BLOCK@";
const LAST_BLOCK_PLACEHOLDER: &str = "@LAST_BLOCK@";
const PART_FILE_PLACEHOLDER: &str = "@PART_FILE@";

// options the per range COPY statement renders as a bare number, since defGetInt64,
// defGetInt32 and defGetNumeric all reject a string argument
const NUMBER_VALUED_COPY_TO_OPTIONS: [&str; 3] =
    ["row_group_size", "row_group_size_bytes", "bloom_filter_fpp"];

// options the per range COPY statement renders as a quoted literal
const STRING_VALUED_COPY_TO_OPTIONS: [&str; 4] =
    ["parquet_version", "bloom_filter", "dictionary", "field_ids"];

// ParallelCopyToShared is the DSM payload. The per range COPY statement and the part file
// prefix are stored right after it in the same chunk, in that order.
#[repr(C)]
struct ParallelCopyToShared {
    // the next ctid range a participant may claim
    next_range: pg_atomic_uint32,
    nranges: u32,
    pages_per_range: u32,
    // whether the part files are postgres' temporary files, which only the parts of an
    // object store destination are, see part_file_prefix
    parts_are_temp_files: bool,
    statement_len: u32,
    part_prefix_len: u32,
}

impl ParallelCopyToShared {
    fn size_with_strings(statement: &str, part_prefix: &str) -> usize {
        std::mem::size_of::<Self>() + statement.len() + part_prefix.len()
    }

    // write_strings stores the per range COPY statement and the part file prefix right
    // after the struct, so that a participant needs no further DSM keys
    unsafe fn write_strings(shared: *mut Self, statement: &str, part_prefix: &str) {
        (*shared).statement_len = statement.len() as u32;
        (*shared).part_prefix_len = part_prefix.len() as u32;

        let strings = shared.add(1) as *mut u8;
        std::ptr::copy_nonoverlapping(statement.as_ptr(), strings, statement.len());
        std::ptr::copy_nonoverlapping(
            part_prefix.as_ptr(),
            strings.add(statement.len()),
            part_prefix.len(),
        );
    }

    unsafe fn statement(shared: *const Self) -> String {
        let strings = shared.add(1) as *const u8;

        Self::string_at(strings, (*shared).statement_len)
    }

    unsafe fn part_prefix(shared: *const Self) -> String {
        let strings = (shared.add(1) as *const u8).add((*shared).statement_len as usize);

        Self::string_at(strings, (*shared).part_prefix_len)
    }

    unsafe fn string_at(bytes: *const u8, len: u32) -> String {
        let bytes = std::slice::from_raw_parts(bytes, len as usize);

        std::str::from_utf8(bytes)
            .expect("parallel COPY TO shared state is not valid utf8")
            .to_string()
    }
}

// try_parallel_copy_to runs a COPY TO parquet command with parallel workers. Each
// participant claims ctid ranges of the table and writes the rows of each range into its own
// part file, and the leader finally splices the part files, in range order, into the file
// the user asked for. An object store destination is spliced straight into a multipart
// upload, so the file is never written to local disk as a whole. It returns None when the
// command is not eligible, in which case the caller falls back to the single process path.
pub(crate) fn try_parallel_copy_to(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
) -> Option<u64> {
    let nworkers = requested_parallel_workers();

    if nworkers <= 0 {
        return None;
    }

    let destination = parallel_copy_to_destination(uri_info)?;

    let relation = parallel_copy_to_relation(p_stmt)?;

    // all tuples the snapshot can see live below this block, which is the same assumption
    // the heap's own parallel scan makes
    let nblocks = unsafe {
        pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), pg_sys::ForkNumber::MAIN_FORKNUM)
    };

    if nblocks < MIN_PARALLEL_BLOCKS {
        return None;
    }

    let statement = render_range_copy_to_statement(p_stmt, &relation, uri_info)?;

    let nparticipants = nworkers as u32 + 1;
    let pages_per_range = range_size_in_pages(p_stmt, &relation, nblocks, nparticipants);
    let nranges = nblocks.div_ceil(pages_per_range);

    // creates the directory the part files go into, so it comes after the bails above
    let part_prefix = part_file_prefix(&destination);

    // the cleanup below does not run when the backend exits in the middle of the copy, e.g.
    // on pg_terminate_backend, so the prefix is also swept at process exit
    register_part_prefix(&part_prefix);

    let nprocessed = PgTryBuilder::new(|| {
        run_parallel_copy_to(
            nworkers,
            nranges,
            pages_per_range,
            &statement,
            &part_prefix,
            &destination,
        )
    })
    .finally(|| {
        // an error in the middle of a range unwinds past the clear in the claim loop
        clear_internal_part_file();

        unregister_part_prefix(&part_prefix);

        remove_part_files(&part_prefix);
    })
    .execute();

    Some(nprocessed)
}

// parallel_copy_to_relation returns the relation the COPY TO command reads, but only when
// the command is one the parallel path can handle. The rows are claimed as ctid ranges of a
// single heap, each range is written as a local part file, and the part files are spliced
// into one file at the end, so anything else falls back to the single process path. The
// destination the parts are spliced into is checked separately, see
// parallel_copy_to_destination.
fn parallel_copy_to_relation(p_stmt: &PgBox<PlannedStmt>) -> Option<PgRelation> {
    // COPY (SELECT ..) TO has no relation to split into ctid ranges. This is also what stops
    // the per range COPY statements from recursing back into the parallel path.
    if !copy_stmt_has_relation(p_stmt) {
        return None;
    }

    // the splice would have to honor the size limit across the part files
    if copy_to_stmt_file_size_bytes(p_stmt) != INVALID_FILE_SIZE_BYTES {
        return None;
    }

    // a parallel worker cannot start a parallel operation of its own
    if unsafe { pg_sys::IsInParallelMode() } {
        return None;
    }

    let relation = unsafe {
        PgRelation::with_lock(copy_stmt_relation_oid(p_stmt), copy_stmt_lock_mode(p_stmt))
    };

    let relation_form = unsafe { *relation.rd_rel };

    // ctid ranges are a heap concept, and a temporary relation is not visible to a worker
    if relation_form.relkind != RELKIND_RELATION as c_char
        || relation_form.relam != HEAP_TABLE_AM_OID
        || relation_form.relpersistence == RELPERSISTENCE_TEMP as c_char
    {
        return None;
    }

    // row level security rewrites the scan into a subquery holding the policy quals, which
    // a ctid range qual can no longer be pushed into as a tid range scan
    if relation_form.relrowsecurity || relation_form.relforcerowsecurity {
        return None;
    }

    Some(relation)
}

// ParallelCopyToDestination is where the part files the participants wrote are spliced into
enum ParallelCopyToDestination<'a> {
    // spliced into a local file, which is also where the part files sit
    LocalFile(String),
    // spliced into a multipart upload, so that the spliced file is uploaded while the
    // splice that feeds it is still running and is never held on local disk as a whole
    ObjectStore(&'a ParsedUriInfo),
}

// parallel_copy_to_destination returns how the part files of the given uri are spliced
// together, or None when the uri is not one the parallel path can splice into.
fn parallel_copy_to_destination(uri_info: &ParsedUriInfo) -> Option<ParallelCopyToDestination<'_>> {
    // a program takes its rows on a pipe, which cannot be spliced into
    if uri_info.is_program {
        return None;
    }

    match uri_info.scheme {
        ObjectStoreScheme::Local => {
            let output_file = uri_as_string(&uri_info.uri);

            // the splice writes the destination itself instead of going through the object
            // store, and it reports a directory destination in its own words, so the error
            // is left to the single process path
            if Path::new(&output_file).is_dir() {
                return None;
            }

            Some(ParallelCopyToDestination::LocalFile(output_file))
        }
        ObjectStoreScheme::AmazonS3
        | ObjectStoreScheme::MicrosoftAzure
        | ObjectStoreScheme::GoogleCloudStorage => {
            Some(ParallelCopyToDestination::ObjectStore(uri_info))
        }
        // the remaining schemes, e.g. an http uri, take no multipart upload, which is what
        // the splice is streamed into
        _ => None,
    }
}

// range_size_in_pages is how many pages a single range covers. The ranges are spread over the
// participants first, and then grown to hold the row group size the command asked for, because
// a range is written into a part file of its own and its rows cannot share a row group with
// another range's. The growth stops at a range per participant: a row group of the asked for
// size is worth coarser ranges, but not the fan-out itself.
fn range_size_in_pages(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &PgRelation,
    nblocks: u32,
    nparticipants: u32,
) -> u32 {
    let pages_per_range = (nblocks / (nparticipants * RANGES_PER_PARTICIPANT)).max(1);

    // the default row group size is pg_parquet's own choice rather than something the
    // command asked for, so the fan-out is free to write smaller row groups than it
    let Some(row_group_size) = copy_to_stmt_explicit_row_group_size(p_stmt) else {
        return pages_per_range;
    };

    let (reltuples, relpages) = unsafe {
        let relation_form = *relation.rd_rel;

        (
            relation_form.reltuples as f64,
            relation_form.relpages as f64,
        )
    };

    // never analyzed, so there is no estimate to grow the ranges by
    if reltuples <= 0.0 || relpages <= 0.0 {
        return pages_per_range;
    }

    let pages_per_row_group = (row_group_size as f64 * relpages / reltuples).ceil();

    // a relation that cannot give every participant a row group of the asked for size writes
    // row groups short of it, the way the tail of a single process copy does. The row groups
    // never run over it, because the writer flushes one as soon as it fills.
    let pages_per_participant = (nblocks / nparticipants).max(1);

    pages_per_range.max((pages_per_row_group as u32).min(pages_per_participant))
}

// the part file prefixes of the parallel copies running in this backend. A copy the backend
// exits in the middle of dies with a FATAL, which unwinds none of the copy's own cleanup, so
// the prefixes are swept at process exit as well.
thread_local! {
    static RUNNING_PART_PREFIXES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static PART_FILE_SWEEP_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn register_part_prefix(part_prefix: &str) {
    // the callback is registered once and for all, because postgres holds a fixed number of
    // process exit callbacks and only cancels the one registered last
    if !PART_FILE_SWEEP_REGISTERED.replace(true) {
        unsafe { pg_sys::on_proc_exit(Some(remove_running_part_files), pg_sys::Datum::from(0)) };
    }

    RUNNING_PART_PREFIXES.with_borrow_mut(|prefixes| prefixes.push(part_prefix.to_string()));
}

fn unregister_part_prefix(part_prefix: &str) {
    RUNNING_PART_PREFIXES
        .with_borrow_mut(|prefixes| prefixes.retain(|prefix| prefix != part_prefix));
}

// runs after the abort that terminates the workers, which is what makes a part file written
// after the sweep impossible
unsafe extern "C-unwind" fn remove_running_part_files(_code: c_int, _arg: pg_sys::Datum) {
    let part_prefixes = RUNNING_PART_PREFIXES.with_borrow_mut(std::mem::take);

    for part_prefix in part_prefixes {
        remove_part_files(&part_prefix);
    }
}

// the part file the participant is writing right now, and how much temporary space the
// participant's part files take, see is_internal_part_file and account_temp_part_file
thread_local! {
    static INTERNAL_PART_FILE: RefCell<Option<String>> = const { RefCell::new(None) };
    static TEMP_PART_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn set_internal_part_file(part_file: &str) {
    INTERNAL_PART_FILE.with_borrow_mut(|current| *current = Some(part_file.to_string()));
}

fn clear_internal_part_file() {
    INTERNAL_PART_FILE.with_borrow_mut(|current| *current = None);
}

// is_internal_part_file tells whether the uri is the part file the participant is writing
// right now. A part file is a file pg_parquet picked the path of itself rather than one the
// command names, so the copy that writes it is not a COPY TO a file the user chose, and the
// privilege of the command it is a part of is the one that counts.
pub(crate) fn is_internal_part_file(uri_info: &ParsedUriInfo) -> bool {
    if uri_info.is_program || uri_info.scheme != ObjectStoreScheme::Local {
        return false;
    }

    let path = uri_as_string(&uri_info.uri);

    INTERNAL_PART_FILE.with_borrow(|part_file| part_file.as_deref() == Some(path.as_str()))
}

// account_temp_part_file counts the part file against temp_file_limit, which is the limit
// postgres holds its own temporary files to. The parts of an object store destination are
// temporary files of pg_parquet's own, so a copy may not take more temporary space through
// them than a dba allowed. The count is per finished part, so a single part may run over
// the limit before it is noticed, and every participant is held to the limit on its own,
// which is also how postgres counts the temporary files of a parallel query.
fn account_temp_part_file(part_file: &str) {
    // read as a copy, a reference to a mutable static is not allowed
    let limit_kb = unsafe { pg_sys::temp_file_limit };

    if limit_kb < 0 {
        return;
    }

    let Ok(part_metadata) = std::fs::metadata(part_file) else {
        return;
    };

    let temp_bytes = TEMP_PART_BYTES.get() + part_metadata.len();

    TEMP_PART_BYTES.set(temp_bytes);

    // compared the way postgres compares it, in bytes against the limit in kilobytes
    if temp_bytes > limit_kb as u64 * 1024 {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            format!("temporary file size exceeds temp_file_limit ({limit_kb}kB)")
        );
    }
}

// part_file_prefix is the path the part files are named after. The parts of a local file sit
// next to the file, and the parts of an object store upload go into postgres' temporary file
// directory, under a name a crash restart cleans up, since an object store uri is no place
// to write a local part file to. Either way the leader pid is part of the name, so that two
// copies that run at the same time into the same destination cannot write each other's parts
// and splice a mix of the two into the destination.
fn part_file_prefix(destination: &ParallelCopyToDestination) -> String {
    match destination {
        ParallelCopyToDestination::LocalFile(output_file) => {
            let leader_pid = unsafe { pg_sys::MyProcPid };
            format!("{output_file}.{leader_pid}")
        }
        ParallelCopyToDestination::ObjectStore(_) => {
            let data_dir = unsafe {
                CStr::from_ptr(DataDir)
                    .to_str()
                    .expect("invalid data directory")
            };

            // PG_TEMP_FILES_DIR of the default tablespace. A temporary tablespace is not
            // used because a part file is not a file postgres itself knows about.
            let temp_dir = Path::new(data_dir).join("base").join("pgsql_tmp");

            std::fs::create_dir_all(&temp_dir)
                .unwrap_or_else(|e| panic!("failed to create \"{}\": {e}", temp_dir.display()));

            // PG_TEMP_FILE_PREFIX is what a crash restart removes the leftovers of
            let leader_pid = unsafe { pg_sys::MyProcPid };

            temp_dir
                .join(format!("pgsql_tmp_pg_parquet_copy_to_{leader_pid}"))
                .to_str()
                .expect("temporary file path is not valid utf8")
                .to_string()
        }
    }
}

// run_parallel_copy_to sets up the DSM, launches the workers, claims ctid ranges in the
// leader as well, and splices the part files into the destination. It returns the number of
// rows written.
fn run_parallel_copy_to(
    nworkers: i32,
    nranges: u32,
    pages_per_range: u32,
    statement: &str,
    part_prefix: &str,
    destination: &ParallelCopyToDestination,
) -> u64 {
    unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            c"pg_parquet".as_ptr(),
            PARALLEL_COPY_TO_WORKER_FUNCTION.as_ptr(),
            nworkers,
        );

        let chunk_size = ParallelCopyToShared::size_with_strings(statement, part_prefix);

        // shm_toc_estimate_chunk and shm_toc_estimate_keys are macros, so they are spelled
        // out here. The chunk is rounded up because shm_toc_allocate aligns what it hands
        // out, and estimating too much shared memory is harmless.
        (*pcxt).estimator.space_for_chunks += chunk_size.next_multiple_of(64);
        (*pcxt).estimator.number_of_keys += 1;

        // serializes the leader's snapshot, GUCs, combo CIDs and transaction state into the
        // segment, which is what lets a worker see exactly the rows the leader sees
        pg_sys::InitializeParallelDSM(pcxt);

        let shared = pg_sys::shm_toc_allocate((*pcxt).toc, chunk_size) as *mut ParallelCopyToShared;

        pg_atomic_init_u32(&mut (*shared).next_range, 0);
        (*shared).nranges = nranges;
        (*shared).pages_per_range = pages_per_range;
        (*shared).parts_are_temp_files =
            matches!(destination, ParallelCopyToDestination::ObjectStore(_));
        ParallelCopyToShared::write_strings(shared, statement, part_prefix);

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_TO_SHARED_KEY, shared as _);

        pg_sys::LaunchParallelWorkers(pcxt);

        PgTryBuilder::new(|| {
            // the leader claims ranges too, so a command that could not launch a single
            // worker still completes, just without the fan-out
            claim_and_copy_ranges(shared);

            pg_sys::WaitForParallelWorkersToFinish(pcxt);
        })
        // terminates the workers and waits for them to exit, which an interrupted copy has
        // to do before its caller removes the part files, or a worker still running writes
        // a part file after the removal
        .finally(|| pg_sys::DestroyParallelContext(pcxt))
        .execute();

        pg_sys::ExitParallelMode();
    }

    // the parts are spliced in range order, so the spliced file holds the rows of range 0
    // first. A range that found no tuple at all wrote no part file.
    let part_files = (0..nranges)
        .map(|range| part_file(part_prefix, range))
        .filter(|part_file| Path::new(part_file).exists())
        .collect::<Vec<_>>();

    let nprocessed = match destination {
        ParallelCopyToDestination::LocalFile(output_file) => {
            concat_parquet_files(&part_files, output_file)
                .unwrap_or_else(|e| panic!("failed to write \"{output_file}\": {e}"))
        }
        ParallelCopyToDestination::ObjectStore(uri_info) => {
            concat_parquet_files_to_uri(&part_files, uri_info)
                .unwrap_or_else(|e| panic!("failed to write \"{}\": {e}", uri_info.uri))
        }
    };

    nprocessed as u64
}

// pg_parquet_parallel_copy_to_worker is the entry point of a parallel COPY TO worker. It is
// resolved by name out of pg_parquet's shared library, so its symbol has to stay.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_parquet_parallel_copy_to_worker(
    _seg: *mut dsm_segment,
    toc: *mut shm_toc,
) {
    let shared = unsafe {
        pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_TO_SHARED_KEY, false) as *mut ParallelCopyToShared
    };

    claim_and_copy_ranges(shared);
}

// claim_and_copy_ranges claims ctid ranges one by one and writes the rows of each range into
// its own part file, until no range is left.
fn claim_and_copy_ranges(shared: *mut ParallelCopyToShared) {
    let (nranges, pages_per_range, parts_are_temp_files) = unsafe {
        (
            (*shared).nranges,
            (*shared).pages_per_range,
            (*shared).parts_are_temp_files,
        )
    };

    TEMP_PART_BYTES.set(0);

    let statement = unsafe { ParallelCopyToShared::statement(shared) };
    let part_prefix = unsafe { ParallelCopyToShared::part_prefix(shared) };

    // every range's COPY leaves its own allocations behind, so they are freed per range
    // rather than held until the command ends
    let mut range_memory_context = PgMemoryContexts::new("pg_parquet parallel copy to range");

    loop {
        let range = unsafe { pg_atomic_fetch_add_u32(&mut (*shared).next_range, 1) };

        if range >= nranges {
            break;
        }

        // the ranges cover every block of the relation, and the last one reaches past its
        // end, which costs nothing because a tuple written into a new block after the
        // snapshot was taken is not visible to it anyway
        let first_block = range * pages_per_range;
        let last_block = first_block + pages_per_range;

        let part_file = part_file(&part_prefix, range);

        let range_statement = statement
            .replace(FIRST_BLOCK_PLACEHOLDER, &first_block.to_string())
            .replace(LAST_BLOCK_PLACEHOLDER, &last_block.to_string())
            .replace(PART_FILE_PLACEHOLDER, &quote_literal(&part_file));

        // the copy below writes a path pg_parquet chose rather than one a user named, which
        // is what the privilege check of the part file goes by
        set_internal_part_file(&part_file);

        unsafe {
            range_memory_context.switch_to(|_| execute_copy_to_statement(&range_statement));

            range_memory_context.reset();
        }

        clear_internal_part_file();

        if parts_are_temp_files {
            account_temp_part_file(&part_file);
        }
    }
}

// execute_copy_to_statement runs a rendered COPY TO parquet statement in this backend. It
// calls pg_parquet's COPY TO handler directly rather than going through ProcessUtility,
// since a parallel operation cannot open the subtransaction that SPI would want.
fn execute_copy_to_statement(statement: &str) {
    let statement = CString::new(statement).expect("COPY TO statement has an interior nul byte");

    unsafe {
        let raw_stmts = PgList::<RawStmt>::from_pg(pg_sys::pg_parse_query(statement.as_ptr()));

        let raw_stmt = raw_stmts
            .get_ptr(0)
            .expect("COPY TO statement did not parse into a statement");

        let mut p_stmt = PgBox::<PlannedStmt>::alloc_node(NodeTag::T_PlannedStmt);
        p_stmt.commandType = CmdType::CMD_UTILITY;
        p_stmt.canSetTag = true;
        p_stmt.utilityStmt = (*raw_stmt).stmt;
        p_stmt.stmt_location = (*raw_stmt).stmt_location;
        p_stmt.stmt_len = (*raw_stmt).stmt_len;

        let p_stmt = PgBox::<PlannedStmt>::from_pg(p_stmt.into_pg());

        let params = PgBox::<pg_sys::ParamListInfoData>::from_pg(std::ptr::null_mut());
        let query_env = PgBox::<pg_sys::QueryEnvironment>::from_pg(std::ptr::null_mut());

        process_copy_to_parquet(&p_stmt, &statement, &params, &query_env);
    }
}

// render_range_copy_to_statement renders the COPY statement a single ctid range is copied
// with, leaving the range bounds and the part file as placeholders for the participant that
// claims the range. It returns None when an option cannot be rendered back into a statement,
// so that a command is never written with options other than the ones it was given.
fn render_range_copy_to_statement(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &PgRelation,
    uri_info: &ParsedUriInfo,
) -> Option<String> {
    let options = render_copy_to_options(p_stmt, uri_info)?;

    let attributes = render_attribute_list(p_stmt);

    let relation_name = unsafe {
        let relation_name = quote_qualified_identifier(
            relation.namespace().as_pg_cstr(),
            relation.name().as_pg_cstr(),
        );

        cstr_to_string(relation_name)
    };

    // ONLY matches what the single process path does, which builds the SELECT without the
    // inheritance flag for a plain relation, so an inheritance child is not copied along
    Some(format!(
        "COPY (SELECT {attributes} FROM ONLY {relation_name} WHERE \
         ctid >= '({FIRST_BLOCK_PLACEHOLDER},0)'::tid AND \
         ctid < '({LAST_BLOCK_PLACEHOLDER},0)'::tid) \
         TO {PART_FILE_PLACEHOLDER} ({options})"
    ))
}

// render_attribute_list renders the column list of the COPY TO command, or * when the
// command names no columns.
fn render_attribute_list(p_stmt: &PgBox<PlannedStmt>) -> String {
    let attribute_list = unsafe { PgList::<Node>::from_pg(copy_stmt_attribute_list(p_stmt)) };

    if attribute_list.is_empty() {
        return "*".into();
    }

    attribute_list
        .iter_ptr()
        .map(|attribute| {
            let attribute_name = strVal(attribute);

            unsafe { cstr_to_string(quote_identifier(attribute_name.as_pg_cstr())) }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// render_copy_to_options renders the options of the COPY TO command back into an option list.
// The format, the compression and the compression level are always rendered, from their
// resolved values, because they would otherwise be inferred from the extension of the part
// file rather than from the file the user named.
fn render_copy_to_options(p_stmt: &PgBox<PlannedStmt>, uri_info: &ParsedUriInfo) -> Option<String> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let mut rendered_options = vec!["format 'parquet'".to_string()];

    for option in unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) }.iter_ptr() {
        let option_name = unsafe { cstr_to_string((*option).defname) };

        // rendered from the resolved values below
        if ["format", "compression", "compression_level"].contains(&option_name.as_str()) {
            continue;
        }

        // an option without an argument is not something we can render back
        if unsafe { (*option).arg.is_null() } {
            return None;
        }

        let option_value = unsafe { cstr_to_string(defGetString(option)) };

        if NUMBER_VALUED_COPY_TO_OPTIONS.contains(&option_name.as_str()) {
            rendered_options.push(format!("{option_name} {option_value}"));
        } else if STRING_VALUED_COPY_TO_OPTIONS.contains(&option_name.as_str()) {
            rendered_options.push(format!("{option_name} {}", quote_literal(&option_value)));
        } else {
            return None;
        }
    }

    let compression = copy_to_stmt_compression(p_stmt, uri_info).to_string();
    rendered_options.push(format!("compression {}", quote_literal(&compression)));

    if let Some(compression_level) = copy_to_stmt_compression_level(p_stmt, uri_info) {
        rendered_options.push(format!("compression_level {compression_level}"));
    }

    Some(rendered_options.join(", "))
}

// part_file is the file the rows of the given ctid range are written into
fn part_file(part_prefix: &str, range: u32) -> String {
    format!("{part_prefix}{PART_FILE_INFIX}{range}.parquet")
}

// remove_part_files removes the part files of the copy. The whole prefix is swept, rather
// than the part file of each range, because the object store writer stages a part file in a
// sibling file it names after it, which is left behind when the write does not finish. The
// prefix holds the leader pid, so the sweep cannot remove the parts of another copy.
fn remove_part_files(part_prefix: &str) {
    let part_prefix = Path::new(part_prefix);

    let (Some(part_dir), Some(part_name)) = (part_prefix.parent(), part_prefix.file_name()) else {
        return;
    };

    let Ok(part_dir) = std::fs::read_dir(part_dir) else {
        return;
    };

    let part_name = format!("{}{PART_FILE_INFIX}", part_name.to_string_lossy());

    for part in part_dir.flatten() {
        if part.file_name().to_string_lossy().starts_with(&part_name) {
            let _ = std::fs::remove_file(part.path());
        }
    }
}

fn quote_literal(value: &str) -> String {
    unsafe { cstr_to_string(quote_literal_cstr(value.as_pg_cstr())) }
}

unsafe fn cstr_to_string(cstr: *const c_char) -> String {
    CStr::from_ptr(cstr)
        .to_str()
        .expect("string is not valid utf8")
        .to_string()
}
