//! Minimal SQL abstraction so coordinator/registry logic unit-tests against
//! real SQLite (rusqlite, host) while production runs on DO `SqlStorage`.
//! `exec` returns selected rows; writes return an empty vec.

#[derive(Debug, Clone, PartialEq)]
pub enum DbValue {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl DbValue {
    pub fn as_int(&self) -> i64 {
        match self {
            DbValue::Int(i) => *i,
            DbValue::Real(f) => *f as i64,
            _ => 0,
        }
    }

    pub fn as_text(&self) -> String {
        match self {
            DbValue::Text(s) => s.clone(),
            DbValue::Int(i) => i.to_string(),
            _ => String::new(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, DbValue::Null)
    }
}

impl From<i64> for DbValue {
    fn from(v: i64) -> Self {
        DbValue::Int(v)
    }
}

impl From<u64> for DbValue {
    fn from(v: u64) -> Self {
        DbValue::Int(v as i64)
    }
}

impl From<String> for DbValue {
    fn from(v: String) -> Self {
        DbValue::Text(v)
    }
}

impl From<&str> for DbValue {
    fn from(v: &str) -> Self {
        DbValue::Text(v.to_string())
    }
}

impl From<Vec<u8>> for DbValue {
    fn from(v: Vec<u8>) -> Self {
        DbValue::Blob(v)
    }
}

impl<T: Into<DbValue>> From<Option<T>> for DbValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => DbValue::Null,
        }
    }
}

pub trait Db {
    fn exec(&self, sql: &str, params: &[DbValue]) -> Result<Vec<Vec<DbValue>>, String>;
}

/// Row helpers: positional, matching `SELECT` order written at each call site.
pub fn col_text(row: &[DbValue], i: usize) -> String {
    row.get(i).map(DbValue::as_text).unwrap_or_default()
}

pub fn col_int(row: &[DbValue], i: usize) -> i64 {
    row.get(i).map(DbValue::as_int).unwrap_or(0)
}

pub fn col_opt_text(row: &[DbValue], i: usize) -> Option<String> {
    match row.get(i) {
        Some(DbValue::Null) | None => None,
        Some(v) => Some(v.as_text()),
    }
}

/// Host SQLite backend for unit tests. Production impl over `SqlStorage`
/// lives in `do_glue` (wasm target).
#[cfg(test)]
pub mod test_db {
    use super::*;
    use rusqlite::{Connection, types::ValueRef};

    pub struct TestDb {
        conn: Connection,
    }

    impl TestDb {
        pub fn new() -> Self {
            Self {
                conn: Connection::open_in_memory().expect("in-memory sqlite"),
            }
        }
    }

    impl Db for TestDb {
        fn exec(&self, sql: &str, params: &[DbValue]) -> Result<Vec<Vec<DbValue>>, String> {
            let rusqlite_params: Vec<rusqlite::types::Value> = params
                .iter()
                .map(|v| match v {
                    DbValue::Null => rusqlite::types::Value::Null,
                    DbValue::Int(i) => rusqlite::types::Value::Integer(*i),
                    DbValue::Real(f) => rusqlite::types::Value::Real(*f),
                    DbValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
                    DbValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                })
                .collect();
            let mut stmt = self.conn.prepare(sql).map_err(|e| e.to_string())?;
            if stmt.column_count() == 0 {
                stmt.execute(rusqlite::params_from_iter(rusqlite_params))
                    .map_err(|e| e.to_string())?;
                return Ok(Vec::new());
            }
            let ncol = stmt.column_count();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(rusqlite_params))
                .map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                let mut vals = Vec::new();
                for i in 0..ncol {
                    let v = match row.get_ref(i).map_err(|e| e.to_string())? {
                        ValueRef::Null => DbValue::Null,
                        ValueRef::Integer(i) => DbValue::Int(i),
                        ValueRef::Real(f) => DbValue::Real(f),
                        ValueRef::Text(t) => {
                            DbValue::Text(String::from_utf8_lossy(t).into_owned())
                        }
                        ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
                    };
                    vals.push(v);
                }
                out.push(vals);
            }
            Ok(out)
        }
    }
}

/// Production backend over DO SQLite storage.
pub struct WorkerDb {
    sql: worker::SqlStorage,
}

impl WorkerDb {
    pub fn new(sql: worker::SqlStorage) -> Self {
        Self { sql }
    }
}

fn to_worker_value(v: &DbValue) -> worker::SqlStorageValue {
    match v {
        DbValue::Null => worker::SqlStorageValue::Null,
        DbValue::Int(i) => worker::SqlStorageValue::Integer(*i),
        DbValue::Real(f) => worker::SqlStorageValue::Float(*f),
        DbValue::Text(s) => worker::SqlStorageValue::String(s.clone()),
        DbValue::Blob(b) => worker::SqlStorageValue::Blob(b.clone()),
    }
}

fn from_worker_value(v: worker::SqlStorageValue) -> DbValue {
    match v {
        worker::SqlStorageValue::Null => DbValue::Null,
        worker::SqlStorageValue::Boolean(b) => DbValue::Int(i64::from(b)),
        worker::SqlStorageValue::Integer(i) => DbValue::Int(i),
        worker::SqlStorageValue::Float(f) => DbValue::Real(f),
        worker::SqlStorageValue::String(s) => DbValue::Text(s),
        worker::SqlStorageValue::Blob(b) => DbValue::Blob(b),
    }
}

impl Db for WorkerDb {
    fn exec(&self, sql: &str, params: &[DbValue]) -> Result<Vec<Vec<DbValue>>, String> {
        let bindings: Vec<worker::SqlStorageValue> = params.iter().map(to_worker_value).collect();
        let cursor = self
            .sql
            .exec(sql, Some(bindings))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in cursor.raw() {
            let row = row.map_err(|e| e.to_string())?;
            out.push(row.into_iter().map(from_worker_value).collect());
        }
        Ok(out)
    }
}
