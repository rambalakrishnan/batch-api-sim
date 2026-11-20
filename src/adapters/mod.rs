//! Provider adapters (driving/driven edge translation). Core owns the
//! state machine; adapters own wire shapes. Adding a [`ProviderKind`]
//! variant breaks [`adapter_for`] until the new adapter implements all
//! trait methods — no wildcard arm exists by design.

pub mod common;
pub mod gemini;
pub mod openai;
pub mod openrouter;
pub mod sail;

use serde_json::Value;

use crate::domain::{ProviderKind, SimError, UnifiedBatch, UnifiedItem};
use common::{IngestOutcome, IngestRequest, RenderedFiles};

pub trait BatchProviderAdapter: Send + Sync {
    fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome, SimError>;
    fn render_status(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value;
    fn render_results(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> RenderedFiles;
    fn render_item(&self, batch: &UnifiedBatch, item: &UnifiedItem) -> Value;
    fn build_webhook(&self, batch: &UnifiedBatch, items: &[UnifiedItem]) -> Value;
    fn render_error(&self, err: &SimError) -> Value;
    /// 200 except OpenRouter's 202 on create.
    fn create_http_status(&self) -> u16;
    /// Wire HTTP status for a `SimError`. Default echoes core status;
    /// Gemini maps 409 conflicts → 400 FAILED_PRECONDITION (plan-locked).
    fn error_http_status(&self, err: &SimError) -> u16 {
        err.http_status
    }
}

pub fn adapter_for(provider: ProviderKind) -> &'static dyn BatchProviderAdapter {
    match provider {
        ProviderKind::OpenAI => &openai::OpenAiAdapter,
        ProviderKind::OpenRouter => &openrouter::OpenRouterAdapter,
        ProviderKind::Sail => &sail::SailAdapter,
        ProviderKind::Gemini => &gemini::GeminiAdapter,
    }
}
