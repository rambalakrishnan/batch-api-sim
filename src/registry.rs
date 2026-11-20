//! `FileRegistry` singleton DO: input `.jsonl` uploads only. Outputs live
//! embedded in `BatchCoordinator` (plan Step 3).
//!
//! [`RegistryStore`] holds all logic over the [`Db`] trait (unit-tested);
//! the `#[durable_object]` wrapper at the bottom is thin worker glue.

use serde::{Deserialize, Serialize};

use crate::config::{REFERENCED_TTL_MS, UNREFERENCED_TTL_MS};
use crate::db::{Db, col_int, col_text};
use crate::domain::{ProviderKind, SimError};

// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

pub struct RegistryStore<'d, D: Db> {
    db: &'d D,
}

impl<'d, D: Db> RegistryStore<'d, D> {
    pub fn new(db: &'d D) -> Self {
        Self { db }
    }

    pub fn init(&self) -> Result<(), String> {
        self.db.exec(
            "CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                referenced INTEGER NOT NULL DEFAULT 0,
                expires_at_ms INTEGER NOT NULL
            )",
            &[],
        )?;
        self.db.exec(
            "CREATE TABLE IF NOT EXISTS lines (
                file_id TEXT NOT NULL,
                idx INTEGER NOT NULL,
                line TEXT NOT NULL,
                PRIMARY KEY (file_id, idx)
            )",
            &[],
        )?;
        Ok(())
    }

    pub fn put(
        &self,
        file_id: &str,
        provider: ProviderKind,
        lines: &[String],
        now_ms: i64,
    ) -> Result<(), String> {
        let provider_tag = serde_json::to_string(&provider).map_err(|e| e.to_string())?;
        self.db.exec(
            "INSERT OR REPLACE INTO files (file_id, provider, created_at_ms, referenced, expires_at_ms)
             VALUES (?, ?, ?, 0, ?)",
            &[
                file_id.into(),
                provider_tag.into(),
                now_ms.into(),
                (now_ms + UNREFERENCED_TTL_MS).into(),
            ],
        )?;
        self.db
            .exec("DELETE FROM lines WHERE file_id = ?", &[file_id.into()])?;
        for (idx, line) in lines.iter().enumerate() {
            self.db.exec(
                "INSERT INTO lines (file_id, idx, line) VALUES (?, ?, ?)",
                &[file_id.into(), (idx as i64).into(), line.clone().into()],
            )?;
        }
        Ok(())
    }

    /// Marks a file referenced by a created batch and extends its TTL.
    /// Returns `false` when the file does not exist.
    pub fn mark_referenced(&self, file_id: &str, now_ms: i64) -> Result<bool, String> {
        let rows = self.db.exec(
            "UPDATE files SET referenced = 1, expires_at_ms = ? WHERE file_id = ? RETURNING file_id",
            &[(now_ms + REFERENCED_TTL_MS).into(), file_id.into()],
        )?;
        Ok(!rows.is_empty())
    }

    pub fn get_page(
        &self,
        file_id: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Option<FilePage>, String> {
        let total = match self.line_count(file_id)? {
            Some(n) => n,
            None => return Ok(None),
        };
        let rows = self.db.exec(
            "SELECT line FROM lines WHERE file_id = ? ORDER BY idx LIMIT ? OFFSET ?",
            &[file_id.into(), limit.into(), offset.into()],
        )?;
        Ok(Some(FilePage {
            found: true,
            total,
            lines: rows.iter().map(|r| col_text(r, 0)).collect(),
        }))
    }

    pub fn line_count(&self, file_id: &str) -> Result<Option<i64>, String> {
        let meta = self.db.exec(
            "SELECT file_id FROM files WHERE file_id = ?",
            &[file_id.into()],
        )?;
        if meta.is_empty() {
            return Ok(None);
        }
        let rows = self.db.exec(
            "SELECT COUNT(*) FROM lines WHERE file_id = ?",
            &[file_id.into()],
        )?;
        Ok(Some(rows.first().map(|r| col_int(r, 0)).unwrap_or(0)))
    }

    /// Deletes expired files + their lines. Returns files removed.
    pub fn prune_expired(&self, now_ms: i64) -> Result<i64, String> {
        self.db.exec(
            "DELETE FROM lines WHERE file_id IN (SELECT file_id FROM files WHERE expires_at_ms < ?)",
            &[now_ms.into()],
        )?;
        let gone = self.db.exec(
            "DELETE FROM files WHERE expires_at_ms < ? RETURNING file_id",
            &[now_ms.into()],
        )?;
        Ok(gone.len() as i64)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePage {
    pub found: bool,
    pub total: i64,
    pub lines: Vec<String>,
}

// ---------------------------------------------------------------------------
// FilePort: what BatchCoordinator needs from the registry
// ---------------------------------------------------------------------------

pub trait FileSource {
    fn get_page(&self, file_id: &str, offset: i64, limit: i64)
    -> Result<FilePage, SimError>;
    fn mark_referenced(&self, file_id: &str, now_ms: i64) -> Result<bool, SimError>;
}

impl<D: Db> FileSource for RegistryStore<'_, D> {
    fn get_page(
        &self,
        file_id: &str,
        offset: i64,
        limit: i64,
    ) -> Result<FilePage, SimError> {
        RegistryStore::get_page(self, file_id, offset, limit)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("file '{file_id}'")))
    }

    fn mark_referenced(&self, file_id: &str, now_ms: i64) -> Result<bool, SimError> {
        RegistryStore::mark_referenced(self, file_id, now_ms).map_err(SimError::bad_request)
    }
}

// ---------------------------------------------------------------------------
// internal DO protocol (edge/coordinator stub fetch, JSON body)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RegistryRequest {
    Put {
        file_id: String,
        provider: ProviderKind,
        lines: Vec<String>,
        now_ms: i64,
    },
    MarkReferenced {
        file_id: String,
        now_ms: i64,
    },
    GetPage {
        file_id: String,
        offset: i64,
        limit: i64,
    },
}

/// Success envelope; errors surface as HTTP status + `SimError` JSON.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegistryResponse {
    pub page: Option<FilePage>,
    pub found: Option<bool>,
}

impl RegistryResponse {
    pub fn page(page: FilePage) -> Self {
        Self {
            page: Some(page),
            found: None,
        }
    }

    pub fn found(found: bool) -> Self {
        Self {
            page: None,
            found: Some(found),
        }
    }

    pub fn ack() -> Self {
        Self {
            page: None,
            found: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;

    fn setup() -> TestDb {
        let db = TestDb::new();
        RegistryStore::new(&db).init().unwrap();
        db
    }

    #[test]
    fn put_page_mark_and_prune() {
        let db = setup();
        let s = RegistryStore::new(&db);
        let lines: Vec<String> = (0..600).map(|i| format!("{{\"i\":{i}}}")).collect();
        s.put("file_a", ProviderKind::OpenAI, &lines, 1_000).unwrap();

        let p0 = s.get_page("file_a", 0, 256).unwrap().unwrap();
        assert!(p0.found);
        assert_eq!(p0.total, 600);
        assert_eq!(p0.lines.len(), 256);
        assert_eq!(p0.lines[0], "{\"i\":0}");
        let p2 = s.get_page("file_a", 512, 256).unwrap().unwrap();
        assert_eq!(p2.lines.len(), 88);

        assert!(s.get_page("file_nope", 0, 10).unwrap().is_none());

        // unreferenced: expires 24h after put
        assert_eq!(s.prune_expired(1_000 + UNREFERENCED_TTL_MS - 1).unwrap(), 0);
        assert!(s.mark_referenced("file_a", 2_000).unwrap());
        assert!(!s.mark_referenced("file_nope", 2_000).unwrap());
        // referenced: survives past old deadline, dies 7d after mark
        assert_eq!(s.prune_expired(1_000 + UNREFERENCED_TTL_MS + 1).unwrap(), 0);
        assert_eq!(s.prune_expired(2_000 + REFERENCED_TTL_MS + 1).unwrap(), 1);
        assert!(s.get_page("file_a", 0, 10).unwrap().is_none());
    }

    #[test]
    fn filesource_maps_missing_to_404() {
        let db = setup();
        let s = RegistryStore::new(&db);
        let err = <RegistryStore<TestDb> as FileSource>::get_page(&s, "nope", 0, 1)
            .unwrap_err();
        assert_eq!(err.http_status, 404);
    }
}
