//! `BatchCoordinator` DO: one per batch, self-contained after create.
//! Copy-at-create resolves `FileRef`s via [`FileSource`]; alarms are purely
//! local (state flips + seeded rolls + webhook flag). Determinism comes from
//! per-stream RNG derivation — no RNG state is ever persisted.
//!
//! [`CoordinatorStore`] holds all logic over [`Db`] (unit-tested); the
//! `#[durable_object]` wrapper lives in `do_glue`.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::adapters::adapter_for;
use crate::adapters::common::{IngestOutcome, IngestRequest, truncate_echo};
use crate::chaos::ChaosEngine;
use crate::config::{COPY_PAGE_ROWS, MAX_ITEMS_PER_BATCH};
use crate::db::{Db, col_int, col_opt_text, col_text};
use crate::domain::{
    BatchStatus, SimError, UnifiedBatch, UnifiedItem, UnifiedOutcome, WebhookStatus,
};
use crate::registry::FileSource;

// ---------------------------------------------------------------------------
// RNG stream tags: seed ^ tag (^ index) ⇒ independent deterministic streams.
// ---------------------------------------------------------------------------

const STREAM_CREATE: u64 = 0xc1ea7e01;
const STREAM_ALARM1: u64 = 0xa1a2b00c;
const STREAM_ITEM: u64 = 0x17e7517c;
const STREAM_EDGE: u64 = 0xed9e0dd0;

fn engine_for(seed: u64, tag: u64, index: u64, chaos: &crate::domain::ChaosConfig) -> ChaosEngine {
    ChaosEngine::new(seed ^ tag ^ index.wrapping_mul(0x9e3779b97f4a7c15), chaos.jitter_pct)
}

// ---------------------------------------------------------------------------
// outcomes
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SubmitOutcome {
    pub batch: UnifiedBatch,
    pub alarm_ms: u64,
}

#[derive(Debug)]
pub struct PollOutcome {
    pub batch: UnifiedBatch,
    pub items: Vec<UnifiedItem>,
    /// Edge-chaos injection for this poll attempt (`Retry-After` on 429).
    pub edge_status: Option<u16>,
}

#[derive(Debug)]
pub enum AlarmOutcome {
    /// Terminal/cancelled already, or unknown batch: nothing to do.
    Noop,
    Advanced { alarm_ms: u64 },
    Terminal { dispatch_webhook: bool },
}

// ---------------------------------------------------------------------------
// BatchPort: what the edge needs from a coordinator
// ---------------------------------------------------------------------------

pub trait BatchPort {
    fn submit(
        &self,
        batch: UnifiedBatch,
        files: &dyn FileSource,
        now_ms: u64,
    ) -> Result<SubmitOutcome, SimError>;
    fn poll(&self, batch_id: &str, now_ms: u64) -> Result<PollOutcome, SimError>;
    fn fetch_item(&self, batch_id: &str, custom_id: &str) -> Result<UnifiedItem, SimError>;
    fn cancel(&self, batch_id: &str, now_ms: u64) -> Result<UnifiedBatch, SimError>;
}

/// FileSource for submits that already carry inline items: any file access
/// is a caller bug (glue pre-converts `FileRef`s), surfaced as an honest 500.
pub struct NoFileSource;

impl FileSource for NoFileSource {
    fn get_page(
        &self,
        file_id: &str,
        _offset: i64,
        _limit: i64,
    ) -> Result<crate::registry::FilePage, SimError> {
        Err(SimError::internal_error(format!(
            "file '{file_id}' reached submit without pre-conversion"
        )))
    }

    fn mark_referenced(&self, file_id: &str, _now_ms: i64) -> Result<bool, SimError> {
        Err(SimError::internal_error(format!(
            "file '{file_id}' reached submit without pre-conversion"
        )))
    }
}
// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

pub struct CoordinatorStore<'d, D: Db> {
    db: &'d D,
}

impl<'d, D: Db> CoordinatorStore<'d, D> {
    pub fn new(db: &'d D) -> Self {
        Self { db }
    }

    pub fn init(&self) -> Result<(), String> {
        self.db.exec(
            "CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )",
            &[],
        )?;
        self.db.exec(
            "CREATE TABLE IF NOT EXISTS items (
                batch_id TEXT NOT NULL,
                idx INTEGER NOT NULL,
                custom_id TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                body_style TEXT NULL,
                input TEXT NOT NULL,
                outcome TEXT NULL,
                PRIMARY KEY (batch_id, custom_id)
            )",
            &[],
        )?;
        Ok(())
    }

    fn load_batch(&self, batch_id: &str) -> Result<Option<UnifiedBatch>, String> {
        let rows = self.db.exec(
            "SELECT data FROM batches WHERE id = ?",
            &[batch_id.into()],
        )?;
        rows.first()
            .map(|r| serde_json::from_str(&col_text(r, 0)).map_err(|e| e.to_string()))
            .transpose()
    }

    fn save_batch(&self, batch: &UnifiedBatch) -> Result<(), String> {
        let data = serde_json::to_string(batch).map_err(|e| e.to_string())?;
        self.db.exec(
            "INSERT OR REPLACE INTO batches (id, data) VALUES (?, ?)",
            &[batch.batch_id.as_str().into(), data.into()],
        )?;
        Ok(())
    }

    fn load_items(&self, batch_id: &str) -> Result<Vec<(UnifiedItem, i64)>, String> {
        let rows = self.db.exec(
            "SELECT custom_id, endpoint, body_style, input, outcome, idx
             FROM items WHERE batch_id = ? ORDER BY idx",
            &[batch_id.into()],
        )?;
        rows.iter()
            .map(|r| {
                let outcome = match col_opt_text(r, 4) {
                    Some(s) => Some(serde_json::from_str(&s).map_err(|e| e.to_string())?),
                    None => None,
                };
                let body_style = match col_opt_text(r, 2) {
                    Some(s) => Some(serde_json::from_str(&s).map_err(|e| e.to_string())?),
                    None => None,
                };
                Ok((
                    UnifiedItem {
                        custom_id: col_text(r, 0),
                        endpoint: col_text(r, 1),
                        body_style,
                        input_payload: serde_json::from_str(&col_text(r, 3))
                            .map_err(|e| e.to_string())?,
                        outcome,
                    },
                    col_int(r, 5),
                ))
            })
            .collect()
    }

    fn insert_item(&self, batch_id: &str, idx: i64, item: &UnifiedItem) -> Result<(), String> {
        let style = item
            .body_style
            .map(|s| serde_json::to_string(&s).map_err(|e| e.to_string()))
            .transpose()?;
        let input = serde_json::to_string(&item.input_payload).map_err(|e| e.to_string())?;
        self.db.exec(
            "INSERT INTO items (batch_id, idx, custom_id, endpoint, body_style, input, outcome)
             VALUES (?, ?, ?, ?, ?, ?, NULL)",
            &[
                batch_id.into(),
                idx.into(),
                item.custom_id.as_str().into(),
                item.endpoint.as_str().into(),
                style.into(),
                input.into(),
            ],
        )?;
        Ok(())
    }

    fn check_duplicate_ids(items: &[UnifiedItem]) -> Result<(), SimError> {
        let mut seen = HashSet::with_capacity(items.len());
        for item in items {
            if !seen.insert(item.custom_id.as_str()) {
                return Err(SimError::bad_request(format!(
                    "duplicate custom_id '{}'",
                    item.custom_id
                )));
            }
        }
        Ok(())
    }

    fn copy_from_file(
        batch: &UnifiedBatch,
        files: &dyn FileSource,
        file_id: &str,
    ) -> Result<Vec<UnifiedItem>, SimError> {
        let first = files.get_page(file_id, 0, COPY_PAGE_ROWS)?;
        if !first.found {
            return Err(SimError::not_found(&format!("file '{file_id}'")));
        }
        if first.total == 0 {
            return Err(SimError::bad_request("input file contains no requests"));
        }
        if first.total > MAX_ITEMS_PER_BATCH as i64 {
            return Err(SimError::bad_request(format!(
                "input file holds {} requests, exceeding the {MAX_ITEMS_PER_BATCH} limit",
                first.total
            )));
        }
        let adapter = adapter_for(batch.provider);
        let mut items = Vec::with_capacity(first.total as usize);
        let mut offset = 0i64;
        loop {
            let page = if offset == 0 {
                first.clone()
            } else {
                files.get_page(file_id, offset, COPY_PAGE_ROWS)?
            };
            for (i, line) in page.lines.iter().enumerate() {
                let number = (offset as usize) + i + 1;
                match adapter.ingest(IngestRequest::FileLine { line, number })? {
                    IngestOutcome::Item(item) => items.push(item),
                    IngestOutcome::CreateIntent(_) => {
                        return Err(SimError::bad_request(format!(
                            "line {number}: unexpected create intent"
                        )));
                    }
                }
            }
            offset += page.lines.len() as i64;
            if offset >= page.total || page.lines.is_empty() {
                break;
            }
        }
        Ok(items)
    }
}

fn json_err(e: serde_json::Error) -> SimError {
    SimError::bad_request(e.to_string())
}

impl<D: Db> BatchPort for CoordinatorStore<'_, D> {
    fn submit(
        &self,
        mut batch: UnifiedBatch,
        files: &dyn FileSource,
        now_ms: u64,
    ) -> Result<SubmitOutcome, SimError> {
        if self
            .load_batch(&batch.batch_id)
            .map_err(SimError::bad_request)?
            .is_some()
        {
            return Err(SimError::conflict(&format!(
                "batch '{}' already exists",
                batch.batch_id
            )));
        }
        // Glue pre-converts FileRefs to inline items (async transport); the
        // FileSource branch serves direct sync callers (unit tests).
        let items = if !batch.inline_items.is_empty() {
            std::mem::take(&mut batch.inline_items)
        } else {
            match batch.input_file_id.clone() {
                Some(file_id) => {
                    let items = Self::copy_from_file(&batch, files, &file_id)?;
                    files.mark_referenced(&file_id, now_ms as i64)?;
                    items
                }
                None => Vec::new(),
            }
        };
        if items.is_empty() {
            return Err(SimError::bad_request("batch contains no requests"));
        }
        if items.len() > MAX_ITEMS_PER_BATCH {
            return Err(SimError::bad_request(format!(
                "batch holds {} requests, exceeding the {MAX_ITEMS_PER_BATCH} limit",
                items.len()
            )));
        }
        Self::check_duplicate_ids(&items)?;

        batch.status = BatchStatus::Validating;
        batch.created_at_ms = now_ms;
        batch.updated_at_ms = now_ms;
        batch.completed_at_ms = None;
        batch.counts = crate::domain::RequestCounts {
            total: items.len() as u64,
            completed: 0,
            failed: 0,
        };
        for (idx, item) in items.iter().enumerate() {
            self.insert_item(&batch.batch_id, idx as i64, item)
                .map_err(SimError::bad_request)?;
        }
        self.save_batch(&batch).map_err(SimError::bad_request)?;

        let mut engine = engine_for(batch.chaos.seed, STREAM_CREATE, 0, &batch.chaos);
        let alarm_ms = now_ms + engine.jitter(batch.chaos.validation_delay_ms);
        Ok(SubmitOutcome { batch, alarm_ms })
    }

    fn poll(&self, batch_id: &str, _now_ms: u64) -> Result<PollOutcome, SimError> {
        let mut batch = self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        batch.poll_count += 1;
        self.save_batch(&batch).map_err(SimError::bad_request)?;
        let mut engine = engine_for(
            batch.chaos.seed,
            STREAM_EDGE,
            batch.poll_count,
            &batch.chaos,
        );
        let edge_status = engine.edge_chaos(batch.chaos.edge_chaos_rate);
        let items = self
            .load_items(batch_id)
            .map_err(SimError::bad_request)?
            .into_iter()
            .map(|(item, _)| item)
            .collect();
        Ok(PollOutcome { batch, items, edge_status })
    }

    fn fetch_item(&self, batch_id: &str, custom_id: &str) -> Result<UnifiedItem, SimError> {
        self.load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        let rows = self
            .db
            .exec(
                "SELECT custom_id, endpoint, body_style, input, outcome
                 FROM items WHERE batch_id = ? AND custom_id = ?",
                &[batch_id.into(), custom_id.into()],
            )
            .map_err(SimError::bad_request)?;
        let r = rows
            .first()
            .ok_or_else(|| SimError::not_found(&format!("item '{custom_id}'")))?;
        let outcome = match col_opt_text(r, 4) {
            Some(s) => Some(serde_json::from_str(&s).map_err(json_err)?),
            None => None,
        };
        let body_style = match col_opt_text(r, 2) {
            Some(s) => Some(serde_json::from_str(&s).map_err(json_err)?),
            None => None,
        };
        Ok(UnifiedItem {
            custom_id: col_text(r, 0),
            endpoint: col_text(r, 1),
            body_style,
            input_payload: serde_json::from_str(&col_text(r, 3)).map_err(json_err)?,
            outcome,
        })
    }

    fn cancel(&self, batch_id: &str, now_ms: u64) -> Result<UnifiedBatch, SimError> {
        let mut batch = self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        if batch.status.is_terminal() {
            return Err(SimError::conflict(&format!(
                "batch '{batch_id}' is already '{}'",
                status_name(batch.status)
            )));
        }
        batch.status = BatchStatus::Cancelled;
        batch.updated_at_ms = now_ms;
        batch.completed_at_ms = Some(now_ms);
        self.save_batch(&batch).map_err(SimError::bad_request)?;
        Ok(batch)
    }
}

fn status_name(s: BatchStatus) -> &'static str {
    match s {
        BatchStatus::Validating => "validating",
        BatchStatus::InProgress => "in_progress",
        BatchStatus::Completed => "completed",
        BatchStatus::Failed => "failed",
        BatchStatus::Cancelled => "cancelled",
    }
}

impl<D: Db> CoordinatorStore<'_, D> {
    /// Alarm handler core. Idempotent: safe under at-least-once re-delivery.
    pub fn alarm_tick(&self, batch_id: &str, now_ms: u64) -> Result<AlarmOutcome, SimError> {
        let mut batch = match self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
        {
            Some(b) => b,
            None => return Ok(AlarmOutcome::Noop),
        };
        match batch.status {
            BatchStatus::Validating => {
                let mut engine =
                    engine_for(batch.chaos.seed, STREAM_ALARM1, 0, &batch.chaos);
                if engine.roll_bool(batch.chaos.batch_fail_rate) {
                    batch.status = BatchStatus::Failed;
                    batch.updated_at_ms = now_ms;
                    batch.completed_at_ms = Some(now_ms);
                    batch.counts.failed = batch.counts.total;
                    batch.batch_error = Some(SimError::batch_failed());
                    self.save_batch(&batch).map_err(SimError::bad_request)?;
                    return Ok(AlarmOutcome::Terminal {
                        dispatch_webhook: batch.callback_url.is_some(),
                    });
                }
                batch.status = BatchStatus::InProgress;
                batch.updated_at_ms = now_ms;
                self.save_batch(&batch).map_err(SimError::bad_request)?;
                let alarm_ms = now_ms + engine.jitter(batch.chaos.processing_delay_ms);
                Ok(AlarmOutcome::Advanced { alarm_ms })
            }
            BatchStatus::InProgress => {
                let stored = self
                    .load_items(batch_id)
                    .map_err(SimError::bad_request)?;
                let mut completed = 0u64;
                let mut failed = 0u64;
                for (item, idx) in &stored {
                    let outcome = self.materialize_outcome(&batch, item, *idx as u64);
                    match &outcome {
                        UnifiedOutcome::Success(_) => completed += 1,
                        UnifiedOutcome::Error(_) => failed += 1,
                    }
                    let encoded =
                        serde_json::to_string(&outcome).map_err(json_err)?;
                    self.db
                        .exec(
                            "UPDATE items SET outcome = ? WHERE batch_id = ? AND custom_id = ?",
                            &[
                                encoded.into(),
                                batch_id.into(),
                                item.custom_id.as_str().into(),
                            ],
                        )
                        .map_err(SimError::bad_request)?;
                }
                batch.status = BatchStatus::Completed;
                batch.updated_at_ms = now_ms;
                batch.completed_at_ms = Some(now_ms);
                batch.counts.completed = completed;
                batch.counts.failed = failed;
                self.save_batch(&batch).map_err(SimError::bad_request)?;
                Ok(AlarmOutcome::Terminal {
                    dispatch_webhook: batch.callback_url.is_some(),
                })
            }
            _ => Ok(AlarmOutcome::Noop),
        }
    }

    fn materialize_outcome(
        &self,
        batch: &UnifiedBatch,
        item: &UnifiedItem,
        idx: u64,
    ) -> UnifiedOutcome {
        if item.body_style.is_none() {
            return UnifiedOutcome::Error(SimError::invalid_item(
                &item.custom_id,
                "unrecognizable request body",
            ));
        }
        let mut engine = engine_for(batch.chaos.seed, STREAM_ITEM, idx, &batch.chaos);
        if engine.roll_bool(batch.chaos.item_fail_rate) {
            return match engine.pick_item_error() {
                429 => UnifiedOutcome::Error(SimError::rate_limit(&item.custom_id)),
                _ => UnifiedOutcome::Error(SimError::internal(&item.custom_id)),
            };
        }
        UnifiedOutcome::Success(truncate_echo(&item.input_payload))
    }

    pub fn set_webhook_status(
        &self,
        batch_id: &str,
        status: WebhookStatus,
    ) -> Result<(), SimError> {
        let mut batch = self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        batch.webhook_status = status;
        self.save_batch(&batch).map_err(SimError::bad_request)?;
        Ok(())
    }

    /// Full batch + items without poll side effects (content/webhook paths).
    pub fn fetch_all(&self, batch_id: &str) -> Result<(UnifiedBatch, Vec<UnifiedItem>), SimError> {
        let batch = self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        let items = self
            .load_items(batch_id)
            .map_err(SimError::bad_request)?
            .into_iter()
            .map(|(item, _)| item)
            .collect();
        Ok((batch, items))
    }

    /// Item plus owning batch (per-item lookups need terminality).
    pub fn fetch_item_with_batch(
        &self,
        batch_id: &str,
        custom_id: &str,
    ) -> Result<(UnifiedBatch, UnifiedItem), SimError> {
        let batch = self
            .load_batch(batch_id)
            .map_err(SimError::bad_request)?
            .ok_or_else(|| SimError::not_found(&format!("batch '{batch_id}'")))?;
        let rows = self
            .db
            .exec(
                "SELECT custom_id, endpoint, body_style, input, outcome
                 FROM items WHERE batch_id = ? AND custom_id = ?",
                &[batch_id.into(), custom_id.into()],
            )
            .map_err(SimError::bad_request)?;
        let r = rows
            .first()
            .ok_or_else(|| SimError::not_found(&format!("item '{custom_id}'")))?;
        let outcome = match col_opt_text(r, 4) {
            Some(s) => Some(serde_json::from_str(&s).map_err(json_err)?),
            None => None,
        };
        let body_style = match col_opt_text(r, 2) {
            Some(s) => Some(serde_json::from_str(&s).map_err(json_err)?),
            None => None,
        };
        Ok((
            batch,
            UnifiedItem {
                custom_id: col_text(r, 0),
                endpoint: col_text(r, 1),
                body_style,
                input_payload: serde_json::from_str(&col_text(r, 3)).map_err(json_err)?,
                outcome,
            },
        ))
    }
}

// ---------------------------------------------------------------------------
// internal DO protocol (edge stub fetch, JSON body)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CoordRequest {
    Create { batch: UnifiedBatch },
    Poll { batch_id: String },
    FetchItem { batch_id: String, custom_id: String },
    FetchAll { batch_id: String },
    Cancel { batch_id: String },
    SetWebhookStatus { batch_id: String, status: WebhookStatus },
}

/// Sparse success envelope; errors surface as HTTP status + `SimError` JSON.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CoordResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<UnifiedBatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<UnifiedItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<UnifiedItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alarm_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_webhook: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noop: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use crate::domain::{ChaosConfig, ProviderKind, UnifiedItem};
    use crate::registry::FilePage;
    use std::collections::HashMap;

    struct MapSource {
        files: HashMap<String, Vec<String>>,
    }

    impl FileSource for MapSource {
        fn get_page(
            &self,
            file_id: &str,
            offset: i64,
            limit: i64,
        ) -> Result<FilePage, SimError> {
            match self.files.get(file_id) {
                Some(lines) => Ok(FilePage {
                    found: true,
                    total: lines.len() as i64,
                    lines: lines
                        .iter()
                        .skip(offset as usize)
                        .take(limit as usize)
                        .cloned()
                        .collect(),
                }),
                None => Err(SimError::not_found(&format!("file '{file_id}'"))),
            }
        }

        fn mark_referenced(&self, _file_id: &str, _now_ms: i64) -> Result<bool, SimError> {
            Ok(true)
        }
    }

    fn chaos() -> ChaosConfig {
        ChaosConfig {
            validation_delay_ms: 1000,
            processing_delay_ms: 5000,
            jitter_pct: 50,
            batch_fail_rate: 0.0,
            item_fail_rate: 0.0,
            edge_chaos_rate: 0.0,
            seed: 99,
        }
    }

    fn batch(provider: ProviderKind) -> UnifiedBatch {
        UnifiedBatch {
            batch_id: "batch_t1".to_string(),
            provider,
            status: BatchStatus::Validating,
            created_at_ms: 0,
            updated_at_ms: 0,
            completed_at_ms: None,
            chaos: chaos(),
            callback_url: None,
            webhook_secret: None,
            webhook_status: WebhookStatus::Skipped,
            input_file_id: None,
            counts: crate::domain::RequestCounts::default(),
            batch_error: None,
            model: None,
            endpoint: None,
            poll_count: 0,
            inline_items: vec![],
        }
    }

    fn inline_item(id: &str) -> UnifiedItem {
        UnifiedItem {
            custom_id: id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            body_style: Some(crate::domain::BodyStyle::ChatCompletions),
            input_payload: serde_json::json!({"messages": []}),
            outcome: None,
        }
    }

    fn setup() -> TestDb {
        let db = TestDb::new();
        CoordinatorStore::new(&db).init().unwrap();
        db
    }

    #[test]
    fn submit_inline_full_lifecycle() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::OpenRouter);
        b.inline_items = vec![inline_item("a"), inline_item("b")];
        let out = s.submit(b, &files, 10_000).unwrap();
        assert_eq!(out.batch.counts.total, 2);
        assert!((500..=1500).contains(&(out.alarm_ms - 10_000)));

        // alarm 1 → in_progress + next alarm scheduled
        let r = s.alarm_tick("batch_t1", 11_000).unwrap();
        let next = match r {
            AlarmOutcome::Advanced { alarm_ms } => alarm_ms,
            _ => panic!("expected advance, got {r:?}"),
        };
        assert!((2500..=7500).contains(&(next - 11_000)));

        // alarm 1 re-delivery while in_progress is NOT a noop (it is alarm 2's
        // job); but a duplicate validating→in_progress transition must not
        // double-schedule: state is already InProgress so tick materializes.
        // alarm 2 → completed with successes
        let r = s.alarm_tick("batch_t1", next).unwrap();
        assert!(matches!(r, AlarmOutcome::Terminal { dispatch_webhook: false }));
        let poll = s.poll("batch_t1", next + 1).unwrap();
        assert_eq!(poll.batch.status, BatchStatus::Completed);
        assert_eq!(poll.batch.counts.completed, 2);
        assert!(poll.items.iter().all(|i| matches!(
            i.outcome,
            Some(UnifiedOutcome::Success(_))
        )));
        // re-delivery after terminal → noop
        assert!(matches!(
            s.alarm_tick("batch_t1", next + 5).unwrap(),
            AlarmOutcome::Noop
        ));
    }

    #[test]
    fn batch_fail_rate_one_aborts_with_envelope() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::Sail);
        b.chaos.batch_fail_rate = 1.0;
        b.callback_url = Some("https://cb.test".to_string());
        b.inline_items = vec![inline_item("a")];
        s.submit(b, &files, 0).unwrap();
        let r = s.alarm_tick("batch_t1", 1_000).unwrap();
        assert!(matches!(r, AlarmOutcome::Terminal { dispatch_webhook: true }));
        let poll = s.poll("batch_t1", 1_001).unwrap();
        assert_eq!(poll.batch.status, BatchStatus::Failed);
        assert_eq!(poll.batch.counts.failed, 1);
        assert!(poll.batch.batch_error.is_some());
    }

    #[test]
    fn item_fail_rate_one_errors_every_item_with_mixed_codes() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::OpenRouter);
        b.chaos.item_fail_rate = 1.0;
        b.inline_items = (0..20).map(|i| inline_item(&format!("i{i}"))).collect();
        s.submit(b, &files, 0).unwrap();
        s.alarm_tick("batch_t1", 1_000).unwrap();
        s.alarm_tick("batch_t1", 6_000).unwrap();
        let poll = s.poll("batch_t1", 6_001).unwrap();
        assert_eq!(poll.batch.counts.failed, 20);
        let codes: HashSet<u16> = poll
            .items
            .iter()
            .map(|i| match &i.outcome {
                Some(UnifiedOutcome::Error(e)) => e.http_status,
                _ => panic!("expected error"),
            })
            .collect();
        assert!(codes.contains(&429) && codes.contains(&500), "{codes:?}");
    }

    #[test]
    fn seed_replay_is_identical() {
        let run = || {
            let db = setup();
            let s = CoordinatorStore::new(&db);
            let files = MapSource { files: HashMap::new() };
            let mut b = batch(ProviderKind::OpenRouter);
            b.chaos.seed = 1234;
            b.chaos.item_fail_rate = 0.5;
            b.inline_items = (0..10).map(|i| inline_item(&format!("i{i}"))).collect();
            let create = s.submit(b, &files, 0).unwrap();
            s.alarm_tick("batch_t1", 1_000).unwrap();
            s.alarm_tick("batch_t1", 6_000).unwrap();
            let poll = s.poll("batch_t1", 6_001).unwrap();
            let codes: Vec<u16> = poll
                .items
                .iter()
                .map(|i| match &i.outcome {
                    Some(UnifiedOutcome::Success(_)) => 200,
                    Some(UnifiedOutcome::Error(e)) => e.http_status,
                    None => 0,
                })
                .collect();
            (create.alarm_ms, codes)
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn unrecognizable_body_becomes_item_error() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::OpenRouter);
        let mut bad = inline_item("bad");
        bad.body_style = None;
        b.inline_items = vec![inline_item("good"), bad];
        s.submit(b, &files, 0).unwrap();
        s.alarm_tick("batch_t1", 1_000).unwrap();
        s.alarm_tick("batch_t1", 6_000).unwrap();
        let poll = s.poll("batch_t1", 6_001).unwrap();
        assert_eq!(poll.batch.status, BatchStatus::Completed);
        assert_eq!(poll.batch.counts.completed, 1);
        assert_eq!(poll.batch.counts.failed, 1);
    }

    #[test]
    fn file_copy_validates_and_pages() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let mut lines = Vec::new();
        for i in 0..600 {
            lines.push(format!(
                r#"{{"custom_id":"c{i}","method":"POST","url":"/v1/chat/completions","body":{{"model":"m"}}}}"#
            ));
        }
        let mut map = HashMap::new();
        map.insert("file_big".to_string(), lines);
        let files = MapSource { files: map };
        let mut b = batch(ProviderKind::OpenAI);
        b.input_file_id = Some("file_big".to_string());
        let out = s.submit(b, &files, 0).unwrap();
        assert_eq!(out.batch.counts.total, 600);

        // missing file → 404
        let mut b2 = batch(ProviderKind::OpenAI);
        b2.batch_id = "batch_t2".to_string();
        b2.input_file_id = Some("file_nope".to_string());
        assert_eq!(
            s.submit(b2, &files, 0).unwrap_err().http_status,
            404
        );

        // malformed line → 400 naming the line
        let mut map = HashMap::new();
        map.insert("file_bad".to_string(), vec!["{oops".to_string()]);
        let files = MapSource { files: map };
        let mut b3 = batch(ProviderKind::OpenAI);
        b3.batch_id = "batch_t3".to_string();
        b3.input_file_id = Some("file_bad".to_string());
        let err = s.submit(b3, &files, 0).unwrap_err();
        assert_eq!(err.http_status, 400);
        assert!(err.message.contains("line 1"));
    }

    #[test]
    fn duplicate_ids_and_quotas_rejected() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::Sail);
        b.inline_items = vec![inline_item("dup"), inline_item("dup")];
        let err = s.submit(b, &files, 0).unwrap_err();
        assert!(err.message.contains("duplicate"));

        let mut b = batch(ProviderKind::Sail);
        b.batch_id = "batch_big".to_string();
        b.inline_items = (0..10_001).map(|i| inline_item(&format!("i{i}"))).collect();
        assert_eq!(s.submit(b, &files, 0).unwrap_err().http_status, 400);
    }

    #[test]
    fn cancel_flips_and_races_terminal() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::OpenAI);
        b.inline_items = vec![inline_item("a")];
        s.submit(b, &files, 0).unwrap();
        let c = s.cancel("batch_t1", 500).unwrap();
        assert_eq!(c.status, BatchStatus::Cancelled);
        // racing alarm cannot overwrite
        assert!(matches!(
            s.alarm_tick("batch_t1", 1_000).unwrap(),
            AlarmOutcome::Noop
        ));
        // second cancel → 409
        assert_eq!(s.cancel("batch_t1", 600).unwrap_err().http_status, 409);
        // unknown batch → 404
        assert_eq!(s.cancel("batch_nope", 600).unwrap_err().http_status, 404);
    }

    #[test]
    fn edge_chaos_deterministic_per_poll_index() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::OpenAI);
        b.chaos.edge_chaos_rate = 1.0;
        b.inline_items = vec![inline_item("a")];
        s.submit(b, &files, 0).unwrap();
        let seq: Vec<Option<u16>> =
            (0..6).map(|t| s.poll("batch_t1", t).unwrap().edge_status).collect();
        assert!(seq.iter().all(|v| v.is_some()));
        // same seed ⇒ same sequence on a fresh store
        let db2 = setup();
        let s2 = CoordinatorStore::new(&db2);
        let mut b = batch(ProviderKind::OpenAI);
        b.chaos.edge_chaos_rate = 1.0;
        b.inline_items = vec![inline_item("a")];
        s2.submit(b, &files, 0).unwrap();
        let seq2: Vec<Option<u16>> =
            (0..6).map(|t| s2.poll("batch_t1", t).unwrap().edge_status).collect();
        assert_eq!(seq, seq2);
    }

    #[test]
    fn fetch_item_paths() {
        let db = setup();
        let s = CoordinatorStore::new(&db);
        let files = MapSource { files: HashMap::new() };
        let mut b = batch(ProviderKind::Sail);
        b.inline_items = vec![inline_item("a")];
        s.submit(b, &files, 0).unwrap();
        assert_eq!(s.fetch_item("batch_t1", "a").unwrap().custom_id, "a");
        assert_eq!(s.fetch_item("batch_t1", "zzz").unwrap_err().http_status, 404);
        assert_eq!(s.fetch_item("batch_nope", "a").unwrap_err().http_status, 404);
    }
}
