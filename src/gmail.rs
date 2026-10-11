//! Crew email over Gmail: the company mailbox, watched and sent from.
//!
//! sonar/backend decisions/crew-email-2026-09-29.md: Google carries the mail and
//! this service never runs an MTA. One Workspace mailbox per company
//! (`GmailConfig.mailbox`), reached through the crew-mail service account with
//! domain-wide delegation, scoped to gmail.modify + gmail.send and nothing else.
//!
//! Four Google calls, all here:
//!   1. the JWT-bearer token exchange (RFC 7523) that impersonates the mailbox;
//!   2. `users.watch`, which asks Gmail to publish inbox changes to the Pub/Sub
//!      topic, renewed daily by `keep_watch`;
//!   3. `users.history.list`, the read a notification's historyId leads to;
//!   4. `users.messages.send`.
//!
//! The service-account key arrives as `Secrets.gmail_service_account_json` from
//! the pod env (operator SecretEnvVars), never from Config.
//!
//! LOGGING. Nothing here logs an address, a subject or a body, for the reason
//! main.rs gives: only ids, history ids and counts.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};

use mailbox_model::types::services::mailbox::mailbox::GmailConfig;

/// Google's Gmail API origin. Tests point the adapter at a fake instead.
pub const GMAIL_API: &str = "https://gmail.googleapis.com";

/// The two scopes domain-wide delegation grants the crew-mail service account.
/// Asking for any other scope fails the token exchange (`unauthorized_client`).
const SCOPES: &str =
    "https://www.googleapis.com/auth/gmail.modify https://www.googleapis.com/auth/gmail.send";

/// Lifetime of one JWT-bearer assertion; Google's maximum.
const ASSERTION_LIFETIME_SECS: i64 = 3600;

/// An access token is replaced this long before Google says it expires, so a
/// call never leaves with a token that dies in flight.
const TOKEN_REFRESH_MARGIN_SECS: i64 = 60;

/// How often the watch is renewed. Gmail drops a watch after 7 days and asks
/// callers to renew daily.
const WATCH_RENEW: Duration = Duration::from_secs(24 * 3600);

/// How soon a failed watch is retried. The likely failure before launch is the
/// missing publisher grant on the topic, and a fix there should not wait a day.
const WATCH_RETRY: Duration = Duration::from_secs(600);

/// The fields of a Google service-account JSON key this adapter uses.
#[derive(serde::Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    token_uri: String,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

/// What `users.watch` answers: the mailbox's current history id, where a reader
/// starts, and when Gmail will drop the watch (epoch ms, as Gmail's string).
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Watch {
    pub history_id: String,
    pub expiration: String,
}

/// The inbox messages added since a history id, and the history id to read from
/// next time.
#[derive(Debug, PartialEq)]
pub struct History {
    pub message_ids: Vec<String>,
    pub history_id: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryPage {
    #[serde(default)]
    history: Vec<HistoryRecord>,
    next_page_token: Option<String>,
    history_id: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryRecord {
    #[serde(default)]
    messages_added: Vec<MessageAdded>,
}

#[derive(serde::Deserialize)]
struct MessageAdded {
    message: MessageRef,
}

#[derive(serde::Deserialize)]
struct MessageRef {
    id: String,
}

/// One plain-text mail from the configured send-as address.
pub struct OutboundMail {
    pub to: String,
    pub subject: String,
    pub body: String,
    /// The crew member sending, for the `X-SeaBeam-Agent` header (e.g. "tally").
    pub agent: String,
}

pub struct GmailAdapter {
    http: reqwest::Client,
    api: String,
    cfg: GmailConfig,
    client_email: String,
    token_uri: String,
    key: SigningKey<Sha256>,
    /// The current access token and its expiry (unix seconds).
    token: tokio::sync::Mutex<Option<(String, i64)>>,
}

impl GmailAdapter {
    /// `service_account_json` is the key file as Google issues it; `api` is
    /// `GMAIL_API` outside tests.
    pub fn new(cfg: &GmailConfig, service_account_json: &str, api: &str) -> Result<Self, String> {
        let sa: ServiceAccountKey = serde_json::from_str(service_account_json)
            .map_err(|e| format!("gmail service account key: {e}"))?;
        // Google signs nothing but RS256 here, so the key is used directly: the
        // chassis key picks RS384 for a 2048-bit RSA key and has no RS256.
        let key = rsa::RsaPrivateKey::from_pkcs8_pem(&sa.private_key)
            .map_err(|e| format!("gmail service account private_key: {e}"))?;
        Ok(Self {
            http: reqwest::Client::new(),
            api: api.trim_end_matches('/').to_string(),
            cfg: cfg.clone(),
            client_email: sa.client_email,
            token_uri: sa.token_uri,
            key: SigningKey::<Sha256>::new(key),
            token: tokio::sync::Mutex::new(None),
        })
    }

    /// The signed JWT-bearer assertion: the service account (`iss`) asking for
    /// a token that acts as the mailbox (`sub`).
    fn assertion(&self, now: i64) -> String {
        let b64 = |v: serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string())
        };
        let header = b64(serde_json::json!({"alg": "RS256", "typ": "JWT"}));
        let claims = b64(serde_json::json!({
            "iss": self.client_email,
            "sub": self.cfg.mailbox,
            "scope": SCOPES,
            "aud": self.token_uri,
            "iat": now,
            "exp": now + ASSERTION_LIFETIME_SECS,
        }));
        let signing_input = format!("{header}.{claims}");
        let signature = self.key.sign(signing_input.as_bytes()).to_vec();
        format!(
            "{signing_input}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature)
        )
    }

    /// A live access token, exchanged for a fresh one when the held one is
    /// within `TOKEN_REFRESH_MARGIN_SECS` of expiry.
    async fn access_token(&self) -> Result<String, String> {
        let mut held = self.token.lock().await;
        let now = now_secs();
        if let Some((token, expires_at)) = held.as_ref() {
            if now < expires_at - TOKEN_REFRESH_MARGIN_SECS {
                return Ok(token.clone());
            }
        }
        let resp: TokenResponse = read_json(
            "gmail token exchange",
            self.http.post(&self.token_uri).form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &self.assertion(now)),
            ]),
        )
        .await?;
        *held = Some((resp.access_token.clone(), now + resp.expires_in));
        Ok(resp.access_token)
    }

    /// An authorized request against the mailbox's Gmail resource.
    async fn request(&self, method: reqwest::Method, path: &str) -> Result<reqwest::RequestBuilder, String> {
        let url = format!("{}/gmail/v1/users/me/{path}", self.api);
        Ok(self.http.request(method, url).bearer_auth(self.access_token().await?))
    }

    /// Start (or renew) the push of INBOX changes to `watch_topic`.
    ///
    /// INBOX only, so the mail this service sends does not notify it of itself.
    pub async fn watch(&self) -> Result<Watch, String> {
        let body = serde_json::json!({
            "topicName": self.cfg.watch_topic,
            "labelIds": ["INBOX"],
            "labelFilterBehavior": "INCLUDE",
        });
        read_json(
            "gmail users.watch",
            self.request(reqwest::Method::POST, "watch").await?.json(&body),
        )
        .await
    }

    /// Every INBOX message added after `start_history_id`, across all pages.
    // No caller yet: the Pub/Sub receive path that hands this a notification's
    // historyId is open (workers/mvp-gmail-questions.md Q1).
    #[allow(dead_code)]
    pub async fn history_since(&self, start_history_id: &str) -> Result<History, String> {
        let mut message_ids = Vec::new();
        let mut page_token = String::new();
        loop {
            let mut query = vec![
                ("startHistoryId", start_history_id),
                ("historyTypes", "messageAdded"),
                ("labelId", "INBOX"),
            ];
            if !page_token.is_empty() {
                query.push(("pageToken", &page_token));
            }
            let page: HistoryPage = read_json(
                "gmail users.history.list",
                self.request(reqwest::Method::GET, "history").await?.query(&query),
            )
            .await?;
            message_ids.extend(
                page.history
                    .into_iter()
                    .flat_map(|h| h.messages_added)
                    .map(|m| m.message.id),
            );
            match page.next_page_token {
                Some(next) if !next.is_empty() => page_token = next,
                _ => {
                    return Ok(History {
                        message_ids,
                        history_id: page.history_id,
                    })
                }
            }
        }
    }

    /// Send one mail; answers Gmail's message id.
    // No caller yet: the send surface (who may send, and the person's thumb in
    // front of it) is open (workers/mvp-gmail-questions.md Q2).
    #[allow(dead_code)]
    pub async fn send(&self, mail: &OutboundMail) -> Result<String, String> {
        #[derive(serde::Deserialize)]
        struct Sent {
            id: String,
        }
        let raw = self.rfc822(mail)?;
        let body = serde_json::json!({
            "raw": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw),
        });
        let sent: Sent = read_json(
            "gmail users.messages.send",
            self.request(reqwest::Method::POST, "messages/send").await?.json(&body),
        )
        .await?;
        Ok(sent.id)
    }

    /// The RFC 5322 message `send` hands Gmail.
    ///
    /// The headers declare an automated sender (crew-email decision, "Headers
    /// say AI"); the body is the sender's own.
    fn rfc822(&self, mail: &OutboundMail) -> Result<String, String> {
        for (name, value) in [
            ("to", &mail.to),
            ("subject", &mail.subject),
            ("agent", &mail.agent),
            ("send_as_name", &self.cfg.send_as_name),
        ] {
            if value.contains(['\r', '\n']) {
                return Err(format!("gmail send: {name} contains a line break"));
            }
        }
        let display_name = if self.cfg.send_as_name.is_ascii() {
            format!(
                "\"{}\"",
                self.cfg.send_as_name.replace('\\', "\\\\").replace('"', "\\\"")
            )
        } else {
            encoded_words(&self.cfg.send_as_name)
        };
        let subject = if mail.subject.is_ascii() {
            mail.subject.clone()
        } else {
            encoded_words(&mail.subject)
        };
        let body = base64::engine::general_purpose::STANDARD.encode(&mail.body);
        let body_lines: Vec<&str> = body
            .as_bytes()
            .chunks(76)
            .map(|c| std::str::from_utf8(c).unwrap_or_default())
            .collect();
        Ok(format!(
            "From: {display_name} <{}>\r\n\
             To: {}\r\n\
             Subject: {subject}\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=UTF-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             Auto-Submitted: auto-generated\r\n\
             X-SeaBeam-Agent: {}\r\n\
             \r\n\
             {}\r\n",
            self.cfg.send_as,
            mail.to,
            mail.agent,
            body_lines.join("\r\n"),
        ))
    }
}

/// Keep the watch alive for as long as the task runs: renew daily, retry a
/// failure after `WATCH_RETRY`. A failed watch stops no other path; sending
/// does not depend on it.
pub async fn keep_watch(gmail: GmailAdapter) {
    loop {
        let wait = match gmail.watch().await {
            Ok(w) => {
                tracing::info!(history_id = %w.history_id, expiration_ms = %w.expiration, "gmail watch renewed");
                WATCH_RENEW
            }
            Err(e) => {
                tracing::warn!(error = %e, "gmail watch failed; retrying");
                WATCH_RETRY
            }
        };
        tokio::time::sleep(wait).await;
    }
}

/// Send `req` and decode a 2xx JSON answer; any other status is an error that
/// carries Google's error body.
async fn read_json<T: serde::de::DeserializeOwned>(
    what: &str,
    req: reqwest::RequestBuilder,
) -> Result<T, String> {
    let resp = req.send().await.map_err(|e| format!("{what}: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("{what}: {e}"))?;
    if !status.is_success() {
        return Err(format!("{what}: HTTP {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("{what}: decoding response: {e}"))
}

/// RFC 2047 B-encoded words for a non-ASCII header value, each within the
/// 75-character limit and split on character boundaries.
fn encoded_words(value: &str) -> String {
    // 45 bytes encode to 60 characters; with "=?UTF-8?B?" and "?=" that is 72.
    const MAX_BYTES: usize = 45;
    let mut words = Vec::new();
    let mut chunk = String::new();
    for ch in value.chars() {
        if chunk.len() + ch.len_utf8() > MAX_BYTES {
            words.push(std::mem::take(&mut chunk));
        }
        chunk.push(ch);
    }
    words.push(chunk);
    words
        .iter()
        .map(|w| {
            format!(
                "=?UTF-8?B?{}?=",
                base64::engine::general_purpose::STANDARD.encode(w)
            )
        })
        .collect::<Vec<_>>()
        .join("\r\n ")
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
