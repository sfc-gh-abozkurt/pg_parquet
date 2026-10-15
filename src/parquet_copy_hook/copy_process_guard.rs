// pg_parquet does its work in the Postgres backend that runs the command: Postgres owns every
// process involved, be it the backend itself, a parallel worker or the shell of a PROGRAM clause,
// and the only threads the extension adds are the ones its tokio runtime parks object store calls
// on. A debug build checks both of those around every copy, every parquet udf and the schema
// inference of a CREATE TABLE, so a dependency that starts a thread pool or forks a helper
// process of its own does not go unnoticed.

// the name the extension gives the threads of its tokio runtime, which is how a thread of ours
// is told apart from one a dependency started. Linux truncates a thread name to 15 characters.
pub(crate) const RUNTIME_THREAD_NAME: &str = "pg_parquet_rt";

// assert_backend_only_during runs part of a command and then checks that it left behind no
// thread and no child process that Postgres does not manage. allows_program_child is for a COPY
// that has a PROGRAM clause: Postgres forks a shell for it through OpenPipeStream, and that child
// is still around when the copy returns.
//
// Both checks are against what the backend held before, because the extension only answers for
// what it starts itself. Another extension's thread, or a child that a trigger forked earlier in
// the transaction, is already there when pg_parquet is entered, and failing over one of those
// would turn a debug build into a build that cannot copy at all.
//
// The check is only a debug build's, and only on Linux, where /proc tells us about our own
// threads and children. It is also skipped when the work throws, since Postgres aborts the
// command then and whatever is left behind is cleaned up by the abort rather than by us.
pub(crate) fn assert_backend_only_during<T>(
    allows_program_child: bool,
    work: impl FnOnce() -> T,
) -> T {
    let children_before = children();
    let foreign_threads_before = foreign_threads();

    let result = work();

    let new_children: Vec<i32> = children()
        .into_iter()
        .filter(|child| !children_before.contains(child))
        .collect();

    let allowed_children = if allows_program_child { 1 } else { 0 };

    assert!(
        new_children.len() <= allowed_children,
        "pg_parquet left {} child process(es) behind that postgres did not fork: {:?}",
        new_children.len() - allowed_children,
        new_children
    );

    let new_foreign_threads: Vec<String> = foreign_threads()
        .into_iter()
        .filter(|thread| !foreign_threads_before.contains(thread))
        .collect();

    assert!(
        new_foreign_threads.is_empty(),
        "pg_parquet ran with thread(s) that the extension did not create: {:?}",
        new_foreign_threads
    );

    result
}

// assert_backend_only_during_read is the check of work that reads a parquet file without a
// PROGRAM clause to fork a shell for, which is what every parquet udf and the CREATE TABLE
// inference do.
pub(crate) fn assert_backend_only_during_read<T>(work: impl FnOnce() -> T) -> T {
    let allows_program_child = false;

    assert_backend_only_during(allows_program_child, work)
}

// children returns the processes this process forked. A thread only lists the children it
// forked itself, so every thread of ours is asked.
#[cfg(all(debug_assertions, target_os = "linux"))]
pub(crate) fn children() -> Vec<i32> {
    let mut children = vec![];

    for thread in threads() {
        // a thread that exited between the two reads has no children left to report
        let Ok(line) = std::fs::read_to_string(format!("/proc/self/task/{thread}/children")) else {
            continue;
        };

        children.extend(
            line.split_ascii_whitespace()
                .filter_map(|pid| pid.parse::<i32>().ok()),
        );
    }

    children
}

// foreign_threads returns the threads of this process that neither are the one Postgres runs
// the backend on nor were created by the extension, as "<thread id> (<thread name>)".
#[cfg(all(debug_assertions, target_os = "linux"))]
pub(crate) fn foreign_threads() -> Vec<String> {
    // the thread a process starts on has the process id as its thread id
    let backend_thread = std::process::id().to_string();

    threads()
        .into_iter()
        .filter(|thread| *thread != backend_thread)
        .filter_map(|thread| {
            let name = std::fs::read_to_string(format!("/proc/self/task/{thread}/comm")).ok()?;
            let name = name.trim().to_string();

            (name != RUNTIME_THREAD_NAME).then_some(format!("{thread} ({name})"))
        })
        .collect()
}

#[cfg(all(debug_assertions, target_os = "linux"))]
fn threads() -> Vec<String> {
    let Ok(threads) = std::fs::read_dir("/proc/self/task") else {
        return vec![];
    };

    threads
        .filter_map(|thread| Some(thread.ok()?.file_name().to_str()?.to_string()))
        .collect()
}

#[cfg(not(all(debug_assertions, target_os = "linux")))]
pub(crate) fn children() -> Vec<i32> {
    vec![]
}

#[cfg(not(all(debug_assertions, target_os = "linux")))]
pub(crate) fn foreign_threads() -> Vec<String> {
    vec![]
}
