//! Edge routing: pure path matcher + upload parsing (host-tested).
//! Worker I/O glue lives in `do_glue`.

use crate::domain::{ProviderKind, SimError};

#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    OpenAiUpload,
    OpenAiCreate,
    OpenAiPoll { batch_id: String },
    OpenAiCancel { batch_id: String },
    OpenAiFileContent { file_id: String },
    OpenRouterCreate,
    OpenRouterPoll { batch_id: String },
    OpenRouterCancel { batch_id: String },
    SailCreate,
    SailPoll { batch_id: String },
    SailItem { batch_id: String, custom_id: String },
    SailCancel { batch_id: String },
    GeminiUpload,
    GeminiCreate { model: String },
    GeminiPoll { batch_id: String },
    GeminiCancel { batch_id: String },
    GeminiDownload { file_id: String },
    NotFound,
}

impl Route {
    pub fn provider(&self) -> Option<ProviderKind> {
        match self {
            Route::OpenAiUpload
            | Route::OpenAiCreate
            | Route::OpenAiPoll { .. }
            | Route::OpenAiCancel { .. }
            | Route::OpenAiFileContent { .. } => Some(ProviderKind::OpenAI),
            Route::OpenRouterCreate | Route::OpenRouterPoll { .. } | Route::OpenRouterCancel { .. } => {
                Some(ProviderKind::OpenRouter)
            }
            Route::SailCreate
            | Route::SailPoll { .. }
            | Route::SailItem { .. }
            | Route::SailCancel { .. } => Some(ProviderKind::Sail),
            Route::GeminiUpload
            | Route::GeminiCreate { .. }
            | Route::GeminiPoll { .. }
            | Route::GeminiCancel { .. }
            | Route::GeminiDownload { .. } => Some(ProviderKind::Gemini),
            Route::NotFound => None,
        }
    }
}

fn strip_cancel(seg: &str) -> Option<&str> {
    seg.strip_suffix(":cancel").filter(|s| !s.is_empty())
}

pub fn match_route(method: &str, path: &str) -> Route {
    let path = path.split('?').next().unwrap_or(path);
    let path = if path.len() > 1 { path.trim_end_matches('/') } else { path };
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segs.as_slice() {
        // ---- OpenAI ----
        ["openai", "v1", "files"] if method == "POST" => Route::OpenAiUpload,
        ["openai", "v1", "batches"] if method == "POST" => Route::OpenAiCreate,
        ["openai", "v1", "batches", id] if method == "GET" => Route::OpenAiPoll {
            batch_id: id.to_string(),
        },
        ["openai", "v1", "batches", id, "cancel"] if method == "POST" => Route::OpenAiCancel {
            batch_id: id.to_string(),
        },
        ["openai", "v1", "files", id, "content"] if method == "GET" => Route::OpenAiFileContent {
            file_id: id.to_string(),
        },
        // ---- OpenRouter ----
        ["openrouter", "api", "v1", "batches"] if method == "POST" => Route::OpenRouterCreate,
        ["openrouter", "api", "v1", "batches", id] if method == "GET" => Route::OpenRouterPoll {
            batch_id: id.to_string(),
        },
        ["openrouter", "api", "v1", "batches", id, "cancel"] if method == "POST" => {
            Route::OpenRouterCancel { batch_id: id.to_string() }
        }
        // ---- Sail ----
        ["sail", "v1", "batches"] if method == "POST" => Route::SailCreate,
        ["sail", "v1", "batches", id] if method == "GET" => Route::SailPoll {
            batch_id: id.to_string(),
        },
        ["sail", "v1", "batches", id, custom] if method == "GET" => Route::SailItem {
            batch_id: id.to_string(),
            custom_id: custom.to_string(),
        },
        ["sail", "v1", "batches", id, "cancel"] if method == "POST" => Route::SailCancel {
            batch_id: id.to_string(),
        },
        // ---- Gemini ----
        ["gemini", "upload", "v1beta", "files"] | ["upload", "v1beta", "files"]
            if method == "POST" =>
        {
            Route::GeminiUpload
        }
        _ => match_gemini(method, &segs),
    }
}

fn match_gemini(method: &str, segs: &[&str]) -> Route {
    if segs.first() != Some(&"gemini") {
        return Route::NotFound;
    }
    let rest = &segs[1..];
    // POST /gemini/v1beta/models/{id...}:batchGenerateContent
    if method == "POST"
        && rest.first() == Some(&"v1beta")
        && rest.get(1) == Some(&"models")
        && let Some(last) = rest.last()
        && let Some(model_seg) = last.strip_suffix(":batchGenerateContent")
    {
        let mut model = rest[2..rest.len() - 1].join("/");
        if !model.is_empty() {
            model.push('/');
        }
        model.push_str(model_seg);
        if !model.is_empty() {
            return Route::GeminiCreate { model };
        }
        return Route::NotFound;
    }
    match (method, rest) {
        ("GET", ["v1beta", "batches", id]) | ("GET", ["v1beta", "operations", id]) => {
            Route::GeminiPoll { batch_id: id.to_string() }
        }
        ("POST", ["v1beta", "batches", seg]) | ("POST", ["v1beta", "operations", seg]) => {
            match strip_cancel(seg) {
                Some(id) => Route::GeminiCancel { batch_id: id.to_string() },
                None => Route::NotFound,
            }
        }
        ("GET", ["download", "v1beta", "files", seg]) | ("GET", ["v1beta", "files", seg]) => {
            match seg.strip_suffix(":download").map(|s| s.strip_prefix("files/").unwrap_or(s)) {
                Some(id) if !id.is_empty() => Route::GeminiDownload {
                    file_id: id.to_string(),
                },
                _ => Route::NotFound,
            }
        }
        _ => Route::NotFound,
    }
}

// ---------------------------------------------------------------------------
// uploads
// ---------------------------------------------------------------------------

pub struct ParsedUpload {
    pub filename: String,
    pub bytes: Vec<u8>,
}

/// OpenAI file upload: `multipart/form-data` with a `file` part, or a raw
/// JSONL body (simulator leniency).
pub fn parse_openai_upload(
    content_type: Option<&str>,
    body: &[u8],
) -> Result<ParsedUpload, SimError> {
    let ct = content_type.unwrap_or("");
    if !ct.to_lowercase().starts_with("multipart/") {
        return Ok(ParsedUpload {
            filename: "upload.jsonl".to_string(),
            bytes: body.to_vec(),
        });
    }
    let boundary = ct
        .split(';')
        .find_map(|p| {
            p.trim()
                .strip_prefix("boundary=")
                .map(|b| b.trim_matches('"').to_string())
        })
        .ok_or_else(|| SimError::bad_request("upload: multipart missing boundary"))?;
    let text = std::str::from_utf8(body)
        .map_err(|_| SimError::bad_request("upload: body must be UTF-8"))?;
    let sep = format!("--{boundary}");
    for part in text.split(&sep) {
        let part = part.trim().trim_end_matches("--").trim();
        if part.is_empty() {
            continue;
        }
        let (head, payload) = match part.find("\r\n\r\n").or_else(|| part.find("\n\n")) {
            Some(idx) => {
                let off = if part[idx..].starts_with("\r\n") { idx + 4 } else { idx + 2 };
                (&part[..idx], part[off..].trim_end_matches(['\r', '\n']))
            }
            None => continue,
        };
        if !head.contains("name=\"file\"") && !head.contains("name=file") {
            continue;
        }
        let filename = head
            .lines()
            .find(|l| l.contains("name="))
            .and_then(|disp| {
                disp.split(';').find_map(|p| {
                    p.trim()
                        .strip_prefix("filename=")
                        .map(|f| {
                            f.trim().trim_matches('"').split('"').next().unwrap_or("").to_string()
                        })
                        .filter(|f| !f.is_empty())
                })
            })
            .unwrap_or_else(|| "upload.jsonl".to_string());
        return Ok(ParsedUpload {
            filename,
            bytes: payload.as_bytes().to_vec(),
        });
    }
    Err(SimError::bad_request("upload: no 'file' part found"))
}

/// Splits validated JSONL lines. Empty lines skipped; numbering in later
/// errors refers to non-empty lines (documented leniency).
pub fn split_jsonl(body: &[u8]) -> Result<Vec<String>, SimError> {
    use crate::config::{MAX_LINE_BYTES, MAX_UPLOAD_BYTES};
    if body.len() > MAX_UPLOAD_BYTES {
        return Err(SimError::bad_request(format!(
            "upload exceeds the {} MB limit",
            MAX_UPLOAD_BYTES / (1024 * 1024)
        )));
    }
    let text =
        std::str::from_utf8(body).map_err(|_| SimError::bad_request("upload must be UTF-8"))?;
    let mut lines = Vec::new();
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > MAX_LINE_BYTES {
            return Err(SimError::bad_request(format!(
                "line {} exceeds the 1 MB line limit",
                lines.len() + 1
            )));
        }
        lines.push(line.to_string());
    }
    if lines.is_empty() {
        return Err(SimError::bad_request("upload contains no JSON lines"));
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_table() {
        use Route::*;
        assert_eq!(match_route("POST", "/openai/v1/files"), OpenAiUpload);
        assert_eq!(match_route("POST", "/openai/v1/batches"), OpenAiCreate);
        assert_eq!(
            match_route("GET", "/openai/v1/batches/b1"),
            OpenAiPoll { batch_id: "b1".to_string() }
        );
        assert_eq!(
            match_route("POST", "/openai/v1/batches/b1/cancel"),
            OpenAiCancel { batch_id: "b1".to_string() }
        );
        assert_eq!(
            match_route("GET", "/openai/v1/files/f1/content"),
            OpenAiFileContent { file_id: "f1".to_string() }
        );
        assert_eq!(match_route("POST", "/openrouter/api/v1/batches"), OpenRouterCreate);
        assert_eq!(
            match_route("GET", "/openrouter/api/v1/batches/b2"),
            OpenRouterPoll { batch_id: "b2".to_string() }
        );
        assert_eq!(match_route("POST", "/sail/v1/batches"), SailCreate);
        assert_eq!(
            match_route("GET", "/sail/v1/batches/b3/c9"),
            SailItem { batch_id: "b3".to_string(), custom_id: "c9".to_string() }
        );
        assert_eq!(match_route("POST", "/gemini/upload/v1beta/files"), GeminiUpload);
        assert_eq!(match_route("POST", "/upload/v1beta/files"), GeminiUpload);
        assert_eq!(
            match_route("POST", "/gemini/v1beta/models/gemini-2x:batchGenerateContent"),
            GeminiCreate { model: "gemini-2x".to_string() }
        );
        assert_eq!(
            match_route("GET", "/gemini/v1beta/batches/b4"),
            GeminiPoll { batch_id: "b4".to_string() }
        );
        assert_eq!(
            match_route("GET", "/gemini/v1beta/operations/b4"),
            GeminiPoll { batch_id: "b4".to_string() }
        );
        assert_eq!(
            match_route("POST", "/gemini/v1beta/batches/b4:cancel"),
            GeminiCancel { batch_id: "b4".to_string() }
        );
        assert_eq!(
            match_route(
                "GET",
                "/gemini/download/v1beta/files/file-out-b4:download?alt=media"
            ),
            GeminiDownload { file_id: "file-out-b4".to_string() }
        );
        // misses
        assert_eq!(match_route("GET", "/openai/v1/batches"), NotFound);
        assert_eq!(match_route("POST", "/nope/v1/x"), NotFound);
        assert_eq!(
            match_route("POST", "/gemini/v1beta/batches/b4"),
            NotFound
        );
        // trailing slash tolerance
        assert_eq!(match_route("GET", "/sail/v1/batches/b3/"), SailPoll {
            batch_id: "b3".to_string()
        });
    }

    #[test]
    fn uploads_parse() {
        // raw body
        let u = parse_openai_upload(None, b"{\"a\":1}\n").unwrap();
        assert_eq!(u.filename, "upload.jsonl");
        // multipart
        let mp = b"--b1\r\nContent-Disposition: form-data; name=\"purpose\"\r\n\r\nbatch\r\n--b1\r\nContent-Disposition: form-data; name=\"file\"; filename=\"in.jsonl\"\r\nContent-Type: application/jsonl\r\n\r\n{\"a\":1}\n{\"b\":2}\n\r\n--b1--";
        let u = parse_openai_upload(Some("multipart/form-data; boundary=b1"), mp).unwrap();
        assert_eq!(u.filename, "in.jsonl");
        let lines = split_jsonl(&u.bytes).unwrap();
        assert_eq!(lines.len(), 2);
        // errors
        assert!(split_jsonl(b"").is_err());
        assert!(split_jsonl(b"   \n\n").is_err());
        let big = vec![b'x'; crate::config::MAX_UPLOAD_BYTES + 1];
        assert!(split_jsonl(&big).is_err());
        let long_line = format!("{{\"a\":\"{}\"}}\n", "y".repeat(1024 * 1024));
        assert!(split_jsonl(long_line.as_bytes()).is_err());
    }
}
