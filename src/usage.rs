//! Asynchronous usage accounting for completed transit connections.
//!
//! Like the reference implementation, writers run on a single background
//! thread/task fed by a bounded queue, so the connection teardown path never
//! blocks on SQLite or file I/O. Batches are written in one transaction
//! where the backend supports it. Events may be dropped (and counted) if the
//! queue is full; usage is telemetry, not correctness.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::io::FromRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::relay::Mood;
use crate::usage_db::{open_usage_db, UsageDbHandle};

/// Queue bounds for the asynchronous usage pipeline.
const USAGE_QUEUE_CAP: usize = 1024;
const USAGE_BATCH_SIZE: usize = 64;
const USAGE_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const USAGE_DROP_LOG_EVERY: u64 = 1024;

/// One completed-connection usage record. `waiting_time` is `None` when the
/// connection was never paired (lonely); the database records NULL, matching
/// the Python reference.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub started: SystemTime,
    pub total_time: f64,
    pub waiting_time: Option<f64>,
    pub total_bytes: u64,
    pub mood: Mood,
}

/// Error from creating or closing a usage backend.
#[derive(Debug)]
pub struct UsageError {
    msg: String,
}

impl UsageError {
    fn new(msg: String) -> Self {
        UsageError { msg }
    }
}

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for UsageError {}

enum UsageMsg {
    Record(UsageEvent),
    Flush(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}

/// One usage backend (JSON log file or SQLite database).
pub trait UsageWriter: Send {
    fn record_usage(&mut self, ev: &UsageEvent);
    /// Default: write events one at a time. Backends that can persist a
    /// batch in one transaction should override this.
    fn record_usage_batch(&mut self, events: &[UsageEvent]) {
        for ev in events {
            self.record_usage(ev);
        }
    }
    /// Persist global statistics. The stats row is tiny and infrequent
    /// (every 5 minutes), so this stays on the synchronous path.
    fn update_stats(
        &mut self,
        rebooted: SystemTime,
        updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    );
    fn close(&mut self) -> Result<(), UsageError> {
        Ok(())
    }
}

fn system_time_nanos(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as f64 / 1e9)
        .unwrap_or(0.0)
}

fn min_time(a: SystemTime, b: SystemTime) -> SystemTime {
    if a <= b {
        a
    } else {
        b
    }
}

fn max_time(a: SystemTime, b: SystemTime) -> SystemTime {
    if a >= b {
        a
    } else {
        b
    }
}

fn blur_start_time(t: SystemTime, coarseness: i64) -> SystemTime {
    let ts = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let blurred = (ts / coarseness) * coarseness;
    UNIX_EPOCH + Duration::from_secs(blurred.max(0) as u64)
}

fn round_to(size: i64, coarseness: i64) -> i64 {
    coarseness * (1 + (size - 1) / coarseness)
}

fn blur_size(size: i64) -> i64 {
    if size == 0 {
        return 0;
    }
    if size < 1_000_000 {
        return round_to(size, 10_000);
    }
    if size < 1_000_000_000 {
        return round_to(size, 1_000_000);
    }
    round_to(size, 100_000_000)
}

/// Tracks usage statistics of connections.
pub struct UsageTracker {
    backends: Arc<Mutex<Vec<Box<dyn UsageWriter>>>>,
    blur_usage: i64,
    queue: Option<mpsc::Sender<UsageMsg>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    dropped: AtomicU64,
}

impl UsageTracker {
    /// Create a usage tracker. `log_file` may be a path or a file descriptor
    /// number (matching the Python flag's intent; the path form is a
    /// superset). `usage_db` is a SQLite database path (`:memory:` allowed).
    pub fn create(
        blur_usage: i64,
        log_file: &str,
        usage_db: &str,
    ) -> Result<UsageTracker, UsageError> {
        let mut backends: Vec<Box<dyn UsageWriter>> = Vec::new();

        if blur_usage > 0 {
            println!("blurring access times to {} seconds", blur_usage);
        } else {
            println!("not blurring access times");
        }

        if !log_file.is_empty() {
            let file = match log_file.parse::<i64>() {
                Ok(fd) if fd >= 0 => {
                    // take ownership, like the reference does with os.NewFile
                    unsafe { File::from_raw_fd(fd as i32) }
                }
                _ => OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file)
                    .map_err(|e| {
                        UsageError::new(format!("failed to open log file {:?}: {}", log_file, e))
                    })?,
            };
            backends.push(Box::new(LogFileUsageWriter::new(file)));
        }
        if !usage_db.is_empty() {
            let db = open_usage_db(usage_db).map_err(|e| {
                UsageError::new(format!("failed to open usage db {:?}: {}", usage_db, e))
            })?;
            backends.push(Box::new(DatabaseUsageWriter::new(db)));
        }

        if backends.is_empty() {
            return Ok(UsageTracker {
                backends: Arc::new(Mutex::new(backends)),
                blur_usage,
                queue: None,
                worker: Mutex::new(None),
                dropped: AtomicU64::new(0),
            });
        }

        let (tx, mut rx) = mpsc::channel::<UsageMsg>(USAGE_QUEUE_CAP);
        let shared = Arc::new(Mutex::new(backends));
        let worker_backends = Arc::clone(&shared);
        let worker = tokio::spawn(async move {
            let mut pending: Vec<UsageEvent> = Vec::with_capacity(USAGE_BATCH_SIZE);
            let mut ticker = tokio::time::interval(USAGE_FLUSH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(UsageMsg::Record(ev)) => {
                                pending.push(ev);
                                if pending.len() >= USAGE_BATCH_SIZE {
                                    dispatch(&worker_backends, &mut pending);
                                }
                            }
                            Some(UsageMsg::Flush(ack)) => {
                                dispatch(&worker_backends, &mut pending);
                                let _ = ack.send(());
                            }
                            Some(UsageMsg::Stop(ack)) => {
                                dispatch(&worker_backends, &mut pending);
                                let _ = ack.send(());
                                break;
                            }
                            None => {
                                dispatch(&worker_backends, &mut pending);
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        dispatch(&worker_backends, &mut pending);
                    }
                }
            }
        });

        Ok(UsageTracker {
            backends: shared,
            blur_usage,
            queue: Some(tx),
            worker: Mutex::new(Some(worker)),
            dropped: AtomicU64::new(0),
        })
    }

    /// Block until every record enqueued so far has been written to the
    /// backends. Intended for tests and shutdown.
    pub async fn flush(&self) {
        if let Some(tx) = &self.queue {
            let (ack_tx, ack_rx) = oneshot::channel();
            if tx.send(UsageMsg::Flush(ack_tx)).await.is_ok() {
                let _ = ack_rx.await;
            }
        }
    }

    /// Flush pending records, stop the background writer and close the
    /// backends.
    pub async fn close(&self) -> Result<(), UsageError> {
        if let Some(tx) = &self.queue {
            let (done_tx, done_rx) = oneshot::channel();
            let _ = tx.send(UsageMsg::Stop(done_tx)).await;
            let _ = done_rx.await;
        }
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        let mut err = None;
        if let Ok(mut backends) = self.backends.lock() {
            for backend in backends.iter_mut() {
                if let Err(e) = backend.close() {
                    if err.is_none() {
                        err = Some(e);
                    }
                }
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Record a usage event (computes everything up front, then hands off to
    /// the background writer; never touches the backends inline).
    pub fn record(
        &self,
        started: SystemTime,
        buddy_started: Option<SystemTime>,
        mood: Mood,
        bytes_sent: u64,
        buddy_bytes: u64,
    ) {
        let finished = SystemTime::now();
        let total_time: f64;
        let mut waiting_time: Option<f64> = None;
        let mut total_bytes: u64;

        if let Some(bs) = buddy_started {
            let earliest = min_time(started, bs);
            let latest = max_time(started, bs);
            total_time = finished
                .duration_since(earliest)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            waiting_time = Some(
                latest
                    .duration_since(earliest)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0),
            );
            total_bytes = bytes_sent + buddy_bytes;
        } else {
            total_time = finished
                .duration_since(started)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            total_bytes = bytes_sent;
            // note that "bytes_sent" should always be 0 here, but we record
            // what the state machine remembered anyway
        }

        if self.blur_usage > 0 {
            total_bytes = blur_size(total_bytes as i64) as u64;
        }

        let started = if self.blur_usage > 0 {
            blur_start_time(started, self.blur_usage)
        } else {
            started
        };

        if self.queue.is_none() {
            return;
        }
        let ev = UsageEvent {
            started,
            total_time,
            waiting_time,
            total_bytes,
            mood,
        };
        if let Some(tx) = &self.queue {
            match tx.try_send(UsageMsg::Record(ev)) {
                Ok(()) => {}
                Err(_) => {
                    let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped % USAGE_DROP_LOG_EVERY == 1 {
                        eprintln!(
                            "warning: usage queue full; dropped {} usage events so far",
                            dropped
                        );
                    }
                }
            }
        }
    }

    /// Persist global statistics.
    pub fn update_stats(
        &self,
        rebooted: SystemTime,
        updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    ) {
        if let Ok(mut backends) = self.backends.lock() {
            for backend in backends.iter_mut() {
                backend.update_stats(rebooted, updated, connected, waiting, incomplete_bytes);
            }
        }
    }
}

fn dispatch(backends: &Arc<Mutex<Vec<Box<dyn UsageWriter>>>>, pending: &mut Vec<UsageEvent>) {
    if pending.is_empty() {
        return;
    }
    if let Ok(mut backends) = backends.lock() {
        for backend in backends.iter_mut() {
            backend.record_usage_batch(pending);
        }
    }
    pending.clear();
}

/// Writes usage records to a file, one JSON object per line.
pub struct LogFileUsageWriter {
    file: BufWriter<File>,
}

impl LogFileUsageWriter {
    pub fn new(file: File) -> LogFileUsageWriter {
        LogFileUsageWriter {
            file: BufWriter::new(file),
        }
    }
}

impl UsageWriter for LogFileUsageWriter {
    fn record_usage(&mut self, ev: &UsageEvent) {
        let data = serde_json::json!({
            "started": system_time_nanos(ev.started),
            "total_time": ev.total_time,
            "waiting_time": ev.waiting_time,
            "total_bytes": ev.total_bytes,
            "mood": ev.mood.as_str(),
        });
        if let Err(e) = writeln!(self.file, "{}", data) {
            eprintln!("warning: unable to write usage record: {}", e);
            return;
        }
        // flush per record so log consumers see events promptly
        if let Err(e) = self.file.flush() {
            eprintln!("warning: unable to flush usage log: {}", e);
        }
    }

    fn update_stats(
        &mut self,
        _rebooted: SystemTime,
        _updated: SystemTime,
        _connected: i64,
        _waiting: i64,
        _incomplete_bytes: i64,
    ) {
        // not implemented for log files (the Python reference only updates
        // the database backend as well)
    }
}

/// Writes usage records to an SQLite database. The equivalent of
/// `DatabaseUsageRecorder` in the Python reference; the schema is copied
/// verbatim from `db-schemas/usage-v1.sql` in magic-wormhole-transit-relay.
pub struct DatabaseUsageWriter {
    db: UsageDbHandle,
}

impl DatabaseUsageWriter {
    pub fn new(db: UsageDbHandle) -> DatabaseUsageWriter {
        DatabaseUsageWriter { db }
    }
}

impl UsageWriter for DatabaseUsageWriter {
    fn record_usage(&mut self, ev: &UsageEvent) {
        match self.db.lock() {
            Ok(conn) => {
                if let Err(e) = conn.execute(
                    "INSERT INTO `usage` (`started`, `total_time`, `waiting_time`, `total_bytes`, `result`) VALUES (?1,?2,?3,?4,?5)",
                    rusqlite::params![system_time_nanos(ev.started), ev.total_time, ev.waiting_time, ev.total_bytes as i64, ev.mood.as_str()],
                ) {
                    eprintln!("warning: unable to record usage: {}", e);
                }
            }
            Err(_) => eprintln!("warning: unable to record usage: database mutex poisoned"),
        }
    }

    fn record_usage_batch(&mut self, events: &[UsageEvent]) {
        let conn = match self.db.lock() {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("warning: unable to record usage batch: database mutex poisoned");
                return;
            }
        };
        // single transaction; this is the hot path under churn
        if let Err(e) = conn.execute_batch("BEGIN") {
            eprintln!("warning: unable to begin usage batch: {}", e);
            return;
        }
        for ev in events {
            if let Err(e) = conn.execute(
                "INSERT INTO `usage` (`started`, `total_time`, `waiting_time`, `total_bytes`, `result`) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![system_time_nanos(ev.started), ev.total_time, ev.waiting_time, ev.total_bytes as i64, ev.mood.as_str()],
            ) {
                eprintln!("warning: unable to record usage (batch): {}", e);
            }
        }
        if let Err(e) = conn.execute_batch("COMMIT") {
            eprintln!("warning: unable to commit usage batch: {}", e);
            let _ = conn.execute_batch("ROLLBACK");
        }
    }

    fn update_stats(
        &mut self,
        rebooted: SystemTime,
        updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    ) {
        let conn = match self.db.lock() {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("warning: unable to record current usage stats: database mutex poisoned");
                return;
            }
        };
        if let Err(e) = conn.execute("DELETE FROM `current`", []) {
            eprintln!("warning: unable to clear current usage stats: {}", e);
            return;
        }
        if let Err(e) = conn.execute(
            "INSERT INTO `current` (`rebooted`, `updated`, `connected`, `waiting`, `incomplete_bytes`) VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![
                rebooted.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0),
                updated.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0),
                connected,
                waiting,
                incomplete_bytes,
            ],
        ) {
            eprintln!("warning: unable to record current usage stats: {}", e);
        }
    }
}

impl crate::relay::UsageRecorder for UsageTracker {
    fn record(
        &self,
        started: SystemTime,
        buddy_started: Option<SystemTime>,
        mood: Mood,
        bytes_sent: u64,
        buddy_bytes: u64,
    ) {
        UsageTracker::record(self, started, buddy_started, mood, bytes_sent, buddy_bytes)
    }

    fn update_stats(
        &self,
        rebooted: SystemTime,
        updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    ) {
        UsageTracker::update_stats(
            self,
            rebooted,
            updated,
            connected,
            waiting,
            incomplete_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_to_sizes() {
        assert_eq!(round_to(1, 10), 10);
        assert_eq!(round_to(10, 10), 10);
        assert_eq!(round_to(11, 10), 20);
    }

    #[test]
    fn blur_size_small() {
        assert_eq!(blur_size(0), 0);
        assert_eq!(blur_size(1), 10_000);
        assert_eq!(blur_size(10_000), 10_000);
        assert_eq!(blur_size(10_001), 20_000);
        assert_eq!(blur_size(999_999), 1_000_000);
        assert_eq!(blur_size(1_000_000), 1_000_000);
        assert_eq!(blur_size(1_000_001), 2_000_000);
        assert_eq!(blur_size(1_000_000_000), 1_000_000_000);
        assert_eq!(blur_size(1_000_000_001), 1_100_000_000);
    }

    #[test]
    fn blur_start_time_rounds_down() {
        let t = UNIX_EPOCH + Duration::from_secs(12_345);
        let blurred = blur_start_time(t, 100);
        assert_eq!(
            blurred.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            12_300
        );
    }
}
