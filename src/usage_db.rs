//! Usage database handling. The usage schema under `src/db_schemas/` is
//! copied verbatim from the reference implementation
//! (`magic-wormhole-transit-relay`, `db-schemas/usage-v1.sql`) and must not
//! be edited.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OpenFlags};

const USAGE_DB_TARGET_VERSION: i64 = 1;

const USAGE_V1_SCHEMA: &str = include_str!("db_schemas/usage-v1.sql");

/// Unrecoverable usage database problem.
#[derive(Debug, Clone)]
pub struct UsageDbError {
    msg: String,
}

impl UsageDbError {
    fn new(msg: String) -> Self {
        UsageDbError { msg }
    }
}

impl fmt::Display for UsageDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for UsageDbError {}

impl From<rusqlite::Error> for UsageDbError {
    fn from(e: rusqlite::Error) -> Self {
        UsageDbError::new(e.to_string())
    }
}

/// A usage database connection, shared between the async usage writer
/// (batched inserts) and the periodic `current`-stats updater.
pub type UsageDbHandle = Arc<Mutex<Connection>>;

fn open_connection(dbfile: &str) -> Result<Connection, UsageDbError> {
    let conn = if dbfile == ":memory:" {
        Connection::open_in_memory()?
    } else {
        Connection::open_with_flags(
            dbfile,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?
    };
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // verify the database passes a foreign key check, like the reference
    let count: i64 = conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
        r.get(0)
    })?;
    if count != 0 {
        return Err(UsageDbError::new("failed foreign key check".to_string()));
    }
    Ok(conn)
}

fn initialize_usage_db(conn: &Connection) -> Result<(), UsageDbError> {
    conn.execute_batch(USAGE_V1_SCHEMA).map_err(|e| {
        UsageDbError::new(format!(
            "failed to initialize usage schema v{}: {}",
            USAGE_DB_TARGET_VERSION, e
        ))
    })?;
    conn.execute(
        "INSERT INTO version (version) VALUES (?1)",
        [USAGE_DB_TARGET_VERSION],
    )
    .map_err(|e| UsageDbError::new(format!("failed to set schema version: {}", e)))?;
    Ok(())
}

fn atomic_create_and_initialize_usage_db(dbfile: &str) -> Result<(), UsageDbError> {
    let path = Path::new(dbfile);
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let base = path
        .file_name()
        .ok_or_else(|| UsageDbError::new(format!("unrecognized database path {:?}", dbfile)))?;
    eprintln!(
        "populating new database with schema v{}",
        USAGE_DB_TARGET_VERSION
    );

    let tmp = tempfile::Builder::new()
        .prefix(&format!(".{}.", base.to_string_lossy()))
        .tempfile_in(dir)
        .map_err(|e| {
            UsageDbError::new(format!(
                "unable to create temporary database near {:?}: {}",
                dbfile, e
            ))
        })?;
    let tmp_path = tmp.path().to_path_buf();
    // build the database on disk in place
    drop(tmp); // close the file handle; we re-open below
    let result = (|| {
        let conn = open_connection(tmp_path.to_str().ok_or_else(|| {
            UsageDbError::new(format!(
                "temporary database path {:?} is not valid UTF-8",
                tmp_path
            ))
        })?)?;
        initialize_usage_db(&conn)?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    std::fs::rename(&tmp_path, dbfile).map_err(|e| {
        let _ = std::fs::remove_file(&tmp_path);
        UsageDbError::new(format!(
            "unable to rename {:?} to {:?}: {}",
            tmp_path, dbfile, e
        ))
    })?;
    Ok(())
}

/// Open (or create) the transit relay usage database.
///
/// `:memory:` is supported for tests. The parent directory of a file path
/// must exist. Mirrors `database.get_db()` in the Python reference
/// (`TARGET_VERSION = 1`, no upgrade chain).
pub fn open_usage_db(dbfile: &str) -> Result<UsageDbHandle, UsageDbError> {
    if dbfile == ":memory:" {
        let conn = open_connection(dbfile)?;
        initialize_usage_db(&conn)?;
        return Ok(Arc::new(Mutex::new(conn)));
    }

    if Path::new(dbfile).exists() {
        let conn = open_connection(dbfile)?;
        let version: i64 = conn
            .query_row("SELECT version FROM version", [], |r| r.get(0))
            .map_err(|e| UsageDbError::new(format!("unable to read db version: {}", e)))?;
        if version != USAGE_DB_TARGET_VERSION {
            return Err(UsageDbError::new(format!(
                "unable to handle db version {}",
                version
            )));
        }
        return Ok(Arc::new(Mutex::new(conn)));
    }

    atomic_create_and_initialize_usage_db(dbfile)?;
    Ok(Arc::new(Mutex::new(open_connection(dbfile)?)))
}
