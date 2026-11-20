//! Terminal-transition webhooks: HMAC signing (pure) + bounded inline
//! dispatch (worker glue). 3 attempts, 500ms/2s backoff, then `failed`.

use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;
use crate::config::{HDR_DEDUPE_KEY, HDR_WEBHOOK_SIG, WEBHOOK_BACKOFF_MS, WEBHOOK_MAX_ATTEMPTS};

type HmacSha256 = Hmac<Sha256>;

/// `sha256=<hex hmac-sha256>` header value, or `None` when no secret.
pub fn sign_header(secret: Option<&str>, body: &[u8]) -> Option<String> {
    let secret = secret?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(body);
    Some(format!("sha256={}", hex_encode(&mac.finalize().into_bytes())))
}

pub fn dedupe_value(batch_id: &str, status: &str) -> String {
    format!("{batch_id}:{status}")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// POSTs `payload` to `url`. Returns `true` on any 2xx within the attempt
/// budget. Never throws delivery past the alarm that called it.
pub async fn dispatch(
    url: &str,
    secret: Option<&str>,
    dedupe: &str,
    payload: &Value,
) -> bool {
    let body = serde_json::to_string(payload).unwrap_or_default();
    let signature = sign_header(secret, body.as_bytes());
    for attempt in 0..WEBHOOK_MAX_ATTEMPTS {
        if attempt > 0 {
            let backoff = WEBHOOK_BACKOFF_MS
                .get((attempt - 1) as usize)
                .copied()
                .unwrap_or(2000);
            worker::Delay::from(std::time::Duration::from_millis(backoff)).await;
        }
        match try_post(url, &body, dedupe, signature.as_deref()).await {
            Ok(delivered) if delivered => return true,
            Ok(_) => continue,
            Err(_) => continue,
        }
    }
    false
}

async fn try_post(
    url: &str,
    body: &str,
    dedupe: &str,
    signature: Option<&str>,
) -> Result<bool, worker::Error> {
    let headers = worker::Headers::new();
    headers.set("content-type", "application/json")?;
    headers.set(HDR_DEDUPE_KEY, dedupe)?;
    if let Some(sig) = signature {
        headers.set(HDR_WEBHOOK_SIG, sig)?;
    }
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post)
        .with_headers(headers)
        .with_body(Some(worker::wasm_bindgen::JsValue::from_str(body)));
    let req = worker::Request::new_with_init(url, &init)?;
    let resp = worker::Fetch::Request(req).send().await?;
    Ok((200..300).contains(&resp.status_code()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_vector_and_dedupe() {
        // RFC 4231-style sanity: deterministic, key-sensitive, prefixed.
        let a = sign_header(Some("key"), b"body").unwrap();
        assert!(a.starts_with("sha256="));
        assert_eq!(a.len(), 7 + 64);
        assert_eq!(sign_header(Some("key"), b"body").unwrap(), a);
        assert_ne!(sign_header(Some("other"), b"body").unwrap(), a);
        assert_eq!(sign_header(None, b"body"), None);
        assert_eq!(dedupe_value("batch_x", "completed"), "batch_x:completed");
    }
}
