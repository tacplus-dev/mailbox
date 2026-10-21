//! The Gmail adapter against a fake Google: a token endpoint that verifies the
//! JWT-bearer assertion the way Google does, and the three Gmail resources
//! (`watch`, `history`, `messages/send`) behind a bearer check. Nothing here
//! reaches Google.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base64::Engine;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::Verifier;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use mailbox_model::types::services::mailbox::mailbox::GmailConfig;

use crate::gmail::{GmailAdapter, OutboundMail};

/// A throwaway 2048-bit RSA key in the PKCS#8 PEM a Google key file carries.
const TEST_SA_PEM: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDbPM1T/8VZ4rO/\n",
    "l+URZ0roQup3dTeO4qgUWhRlGp39Se4CRoabNJVrOo7lXbdS5poSkgWewciNd7w0\n",
    "z+7vlIMMaoO4TexGB9z+3Z8Vu3HVBx3GvaAEYIuVL3wSkbf+K4Wd3LivCHZQTGbo\n",
    "nweOAsvB4HqQjGwyUMjk5vdSATR+MX5qjs4g38OJyOZHSOh2fxn6uzJTYKT9YLjW\n",
    "/F6NGCMZmpXgHns92ztX1ptK9AjFlu26QTPAKkYb/jZSfzumOcOGOQIsHyv9nOBi\n",
    "tz7FOA11i84flORDqdrfh5eKKo+4jxmK0xgtIMGEmypzg1C+vtxZv2jqHxb0bhuO\n",
    "VylOLKP5AgMBAAECggEAA8INOPobkr/Pdmmu+JOtavpbG/vgx/rYT0iOAfFwP4X3\n",
    "LPldgoqRfHNWPStGIpVgIUl4777ku63WsRxk59Xfl6SdUtkrimBwBJ8PWcO817qy\n",
    "OJSfbaA5XvcDPg7pDoywxLTptGl+OM566HDjo/wl826MlC68BoCMjlsISigtduVh\n",
    "mmIEoK0XRB3cdMeiLS8tUb8xwx8AYYemaoldw9uFfu7cGqqYd2JoQblNnIa5q4Hy\n",
    "dsO0MUqPc4ifrzXS5B5qKCYAbgL1LxTfUvwblEOO9pez9kD8JWwb3NfSTlVkncYX\n",
    "1amhVf6/pgQ4zcjFLCFJYAV0eIEJlsYtEb5QzT4RGwKBgQD7R4XAFkcQK/OEo/Gv\n",
    "A2ryKBlhTOzxkWzXXf25cwYH+l/bm76ppeJcNBlRTR8Kd/Mhe/iRfF8r1fQOT+FO\n",
    "OY5To66qsbfzKe+pPstDDEZR+aUWyjzmUdda0T7L/MhmnZlT2o4+joSFZZPis/i+\n",
    "widCrWBLUoQeeVAb1ZCGrVQMqwKBgQDfWy4/co/nAq1VY5Bh0ECkcN4dUa539nJ4\n",
    "AmgMKtsk1Hqj1w8096XnMxUcmx7SBAuXhDFMZHZM9LTqrzxOvj65p1S4lACUduP6\n",
    "q5PkccaZJ0GAgyW90SbXF6Lc9mq+LZLFU5EnIlhBXYkc1DyBWUYEdFayzQd1jhj7\n",
    "s5lNQrkJ6wKBgQDmgYLRZd5EmHzoRVhUmscFYwF9P4rwQt5cZm11LG3w7Z33n9R0\n",
    "XabbvDMpYJzEBqrzIX0xQK0xUjBy3JYrIyNL2JBHJ4Y+O08Nq3vcgqsTUfY+hMD+\n",
    "6z6pod+AUuAnOhVIVeo8upIYqMjToxLhMsu/H7qA/i4Somh5UB0nXmMGxwKBgEPK\n",
    "VeAV8k0oB9nZzTGS1nvSceeuKejpGNcJy/CddUPZZZuio0/tNiJU47x57EHLzoTf\n",
    "L2NDPpCe2OY952zy2vDYM6ON8BsfdpRXTRzR13gWbRAHPgY9m68+0kSEWeN8y4Yu\n",
    "leityOGV9Q6V8ANGSDUD5miASBpK1ZRXb1j+vlCfAoGBAPUkVx1Req3yJDzV4elC\n",
    "EkvBhmQTWoHhPvRnrOoWkaRVDnh62/IPwaDAI2Y58Nkzb7Ia1wdgV6DbOEluLBiP\n",
    "aGJC4+xRUNp1g7zVwvS7ooa/zdZET6UZ9TpIdnsdqjbcR9uXhl7Iy7AywQJrj8/J\n",
    "2ExX0/KfnUYcLNMIi3DE/FMO\n",
    "-----END PRIVATE KEY-----\n",
);

const SA_EMAIL: &str = "crew-mail@test-project.iam.gserviceaccount.com";
const MAILBOX: &str = "hello@seabeam.test";
const SEND_AS: &str = "tally@seabeam.test";
const TOPIC: &str = "projects/test-project/topics/crew-mail-gmail-watch";
const SCOPES: &str =
    "https://www.googleapis.com/auth/gmail.modify https://www.googleapis.com/auth/gmail.send";

pub(crate) fn gmail_config() -> GmailConfig {
    GmailConfig {
        mailbox: MAILBOX.to_string(),
        send_as: SEND_AS.to_string(),
        send_as_name: "Tally for SeaBeam".to_string(),
        watch_topic: TOPIC.to_string(),
        ..Default::default()
    }
}

/// A service-account key file as Google issues it, pointing at `token_uri`.
pub(crate) fn key_file(token_uri: &str) -> String {
    json!({
        "type": "service_account",
        "client_email": SA_EMAIL,
        "private_key": TEST_SA_PEM,
        "token_uri": token_uri,
    })
    .to_string()
}

struct Request {
    method: String,
    /// Path and query, as sent.
    target: String,
    authorization: String,
    body: String,
}

struct FakeGoogle {
    base: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl FakeGoogle {
    fn token_uri(&self) -> String {
        format!("{}/token", self.base)
    }

    fn adapter(&self) -> GmailAdapter {
        GmailAdapter::new(&gmail_config(), &key_file(&self.token_uri()), &self.base).unwrap()
    }

    fn requests_to(&self, path: &str) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.target.split('?').next() == Some(path))
            .map(|r| Request {
                method: r.method.clone(),
                target: r.target.clone(),
                authorization: r.authorization.clone(),
                body: r.body.clone(),
            })
            .collect()
    }
}

/// How the fake behaves where a test needs it to differ from Google's happy path.
#[derive(Clone, Copy)]
struct Behaviour {
    /// `expires_in` on every issued token.
    token_lifetime: i64,
    /// Answer `users.watch` with 403, as Gmail does while
    /// gmail-api-push@system.gserviceaccount.com cannot publish to the topic.
    watch_forbidden: bool,
}

const GOOGLE: Behaviour = Behaviour {
    token_lifetime: 3599,
    watch_forbidden: false,
};

fn b64url_json(part: &str) -> serde_json::Value {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(part)
        .unwrap_or_default();
    serde_json::from_slice(&bytes).unwrap_or_default()
}

/// What Google's token endpoint checks before it issues a delegated token: the
/// JWT-bearer grant, an RS256 assertion signed by the service account's key,
/// issued by the service account, for the token endpoint, impersonating the
/// mailbox, asking only for the delegated scopes.
fn google_accepts(form: &HashMap<String, String>, token_uri: &str) -> bool {
    if form.get("grant_type").map(String::as_str)
        != Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
    {
        return false;
    }
    let assertion = form.get("assertion").map(String::as_str).unwrap_or("");
    let parts: Vec<&str> = assertion.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    let public = rsa::RsaPrivateKey::from_pkcs8_pem(TEST_SA_PEM)
        .unwrap()
        .to_public_key();
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[2])
        .ok()
        .and_then(|s| Signature::try_from(s.as_slice()).ok());
    let signed = signature.is_some_and(|sig| {
        VerifyingKey::<Sha256>::new(public)
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig)
            .is_ok()
    });
    let header = b64url_json(parts[0]);
    let claims = b64url_json(parts[1]);
    let (iat, exp) = (claims["iat"].as_i64().unwrap_or(0), claims["exp"].as_i64().unwrap_or(0));
    signed
        && header["alg"] == "RS256"
        && claims["iss"] == SA_EMAIL
        && claims["sub"] == MAILBOX
        && claims["aud"] == token_uri
        && claims["scope"] == SCOPES
        && exp > iat
        && exp - iat <= 3600
}

/// Read one HTTP/1.1 request.
async fn read_request(sock: &mut TcpStream) -> Option<Request> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = sock.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut start = head.split_whitespace();
    let method = start.next().unwrap_or("").to_string();
    let target = start.next().unwrap_or("").to_string();
    let header = |name: &str| {
        head.lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
            })
            .unwrap_or_default()
    };
    let content_length = header("content-length").parse::<usize>().unwrap_or(0);
    let authorization = header("authorization");
    while buf.len() < header_end + content_length {
        let n = sock.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = String::from_utf8_lossy(&buf[header_end..]).to_string();
    Some(Request {
        method,
        target,
        authorization,
        body,
    })
}

async fn spawn_fake_google(behaviour: Behaviour) -> FakeGoogle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let token_uri = format!("{base}/token");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let issued = Arc::new(Mutex::new(Vec::<String>::new()));
    let log = Arc::clone(&requests);

    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let (token_uri, log, issued) = (token_uri.clone(), Arc::clone(&log), Arc::clone(&issued));
            tokio::spawn(async move {
                let Some(req) = read_request(&mut sock).await else {
                    return;
                };
                let path = req.target.split('?').next().unwrap_or("").to_string();
                let query: HashMap<String, String> =
                    serde_urlencoded::from_str(req.target.split_once('?').map(|(_, q)| q).unwrap_or(""))
                        .unwrap_or_default();
                let bearer_ok = req
                    .authorization
                    .strip_prefix("Bearer ")
                    .is_some_and(|t| issued.lock().unwrap().iter().any(|i| i == t));
                let (status, resp) = match (req.method.as_str(), path.as_str()) {
                    ("POST", "/token") => {
                        let form: HashMap<String, String> =
                            serde_urlencoded::from_str(&req.body).unwrap_or_default();
                        if google_accepts(&form, &token_uri) {
                            let mut issued = issued.lock().unwrap();
                            let token = format!("ya29.fake-{}", issued.len() + 1);
                            issued.push(token.clone());
                            let resp = json!({
                                "access_token": token,
                                "expires_in": behaviour.token_lifetime,
                                "token_type": "Bearer",
                            });
                            ("200 OK", resp.to_string())
                        } else {
                            ("400 Bad Request", json!({"error": "invalid_grant"}).to_string())
                        }
                    }
                    _ if !bearer_ok => ("401 Unauthorized", json!({"error": {"code": 401}}).to_string()),
                    ("POST", "/gmail/v1/users/me/watch") if behaviour.watch_forbidden => (
                        "403 Forbidden",
                        json!({"error": {"code": 403, "message": "User not authorized to perform this action."}})
                            .to_string(),
                    ),
                    ("POST", "/gmail/v1/users/me/watch") => (
                        "200 OK",
                        json!({"historyId": "1234", "expiration": "1760659200000"}).to_string(),
                    ),
                    ("GET", "/gmail/v1/users/me/history") => {
                        let page = match query.get("pageToken").map(String::as_str) {
                            None => json!({
                                "history": [
                                    {"id": "1235", "messagesAdded": [{"message": {"id": "m1", "threadId": "t1"}}]},
                                    {"id": "1236", "labelsAdded": [{"message": {"id": "m0"}}]},
                                ],
                                "nextPageToken": "page-2",
                                "historyId": "1240",
                            }),
                            Some("page-2") => json!({
                                "history": [
                                    {"id": "1239", "messagesAdded": [
                                        {"message": {"id": "m2", "threadId": "t1"}},
                                        {"message": {"id": "m3", "threadId": "t2"}},
                                    ]},
                                ],
                                "historyId": "1240",
                            }),
                            Some(_) => json!({"historyId": "1240"}),
                        };
                        ("200 OK", page.to_string())
                    }
                    ("POST", "/gmail/v1/users/me/messages/send") => (
                        "200 OK",
                        json!({"id": "sent-1", "threadId": "t9", "labelIds": ["SENT"]}).to_string(),
                    ),
                    _ => ("404 Not Found", "{}".to_string()),
                };
                log.lock().unwrap().push(req);
                let out = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                    resp.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });

    FakeGoogle { base, requests }
}

fn mail() -> OutboundMail {
    OutboundMail {
        to: "estimator@example.com".to_string(),
        subject: "Your pricing request".to_string(),
        body: "Hello,\r\nthe base price is attached.\r\n".to_string(),
        agent: Some("tally".to_string()),
    }
}

/// The message Gmail was handed by the one `messages/send` call.
fn sent_message(google: &FakeGoogle) -> String {
    let sends = google.requests_to("/gmail/v1/users/me/messages/send");
    assert_eq!(sends.len(), 1);
    let body: serde_json::Value = serde_json::from_str(&sends[0].body).unwrap();
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body["raw"].as_str().unwrap())
        .unwrap();
    String::from_utf8(raw).unwrap()
}

fn header_of<'a>(message: &'a str, name: &str) -> Option<&'a str> {
    let head = message.split("\r\n\r\n").next().unwrap();
    head.split("\r\n").find_map(|l| {
        let (k, v) = l.split_once(": ")?;
        (k == name).then_some(v)
    })
}

#[tokio::test]
async fn watch_asks_gmail_to_publish_the_inbox_to_the_topic() {
    let google = spawn_fake_google(GOOGLE).await;

    let watch = google.adapter().watch().await.unwrap();

    assert_eq!(watch.history_id, "1234");
    assert_eq!(watch.expiration, "1760659200000");
    let calls = google.requests_to("/gmail/v1/users/me/watch");
    assert_eq!(calls.len(), 1);
    let body: serde_json::Value = serde_json::from_str(&calls[0].body).unwrap();
    assert_eq!(
        body,
        json!({"topicName": TOPIC, "labelIds": ["INBOX"], "labelFilterBehavior": "INCLUDE"})
    );
    // The fake issues a token only for an assertion that impersonates the
    // mailbox with exactly the two delegated scopes, signed by the key file.
    assert_eq!(calls[0].authorization, "Bearer ya29.fake-1");
}

#[tokio::test]
async fn the_access_token_is_reused_until_it_nears_expiry() {
    let google = spawn_fake_google(GOOGLE).await;
    let gmail = google.adapter();

    gmail.watch().await.unwrap();
    gmail.watch().await.unwrap();

    assert_eq!(google.requests_to("/token").len(), 1);
}

#[tokio::test]
async fn a_token_inside_the_refresh_margin_is_exchanged_again() {
    let google = spawn_fake_google(Behaviour {
        token_lifetime: 30,
        ..GOOGLE
    })
    .await;
    let gmail = google.adapter();

    gmail.watch().await.unwrap();
    gmail.watch().await.unwrap();

    assert_eq!(google.requests_to("/token").len(), 2);
    let watches = google.requests_to("/gmail/v1/users/me/watch");
    assert_eq!(watches[1].authorization, "Bearer ya29.fake-2");
}

#[tokio::test]
async fn an_assertion_google_refuses_fails_before_any_gmail_call() {
    let google = spawn_fake_google(GOOGLE).await;
    // Same key, wrong mailbox: Google refuses a delegation it was not granted.
    let cfg = GmailConfig {
        mailbox: "someone-else@seabeam.test".to_string(),
        ..gmail_config()
    };
    let gmail = GmailAdapter::new(&cfg, &key_file(&google.token_uri()), &google.base).unwrap();

    let err = gmail.watch().await.unwrap_err();

    assert!(err.contains("gmail token exchange: HTTP 400"), "{err}");
    assert!(google.requests_to("/gmail/v1/users/me/watch").is_empty());
}

#[tokio::test]
async fn watch_refused_for_the_missing_publisher_grant_does_not_stop_send() {
    let google = spawn_fake_google(Behaviour {
        watch_forbidden: true,
        ..GOOGLE
    })
    .await;
    let gmail = google.adapter();

    let err = gmail.watch().await.unwrap_err();
    let sent = gmail.send(&mail()).await.unwrap();

    assert!(err.contains("gmail users.watch: HTTP 403"), "{err}");
    assert_eq!(sent, "sent-1");
}

#[tokio::test]
async fn history_since_reads_every_page_of_inbox_additions() {
    let google = spawn_fake_google(GOOGLE).await;

    let history = google.adapter().history_since("1234").await.unwrap();

    assert_eq!(
        history,
        crate::gmail::History {
            message_ids: vec!["m1".to_string(), "m2".to_string(), "m3".to_string()],
            history_id: "1240".to_string(),
        }
    );
    let calls = google.requests_to("/gmail/v1/users/me/history");
    assert_eq!(calls.len(), 2);
    let first: HashMap<String, String> =
        serde_urlencoded::from_str(calls[0].target.split_once('?').unwrap().1).unwrap();
    assert_eq!(first.get("startHistoryId").map(String::as_str), Some("1234"));
    assert_eq!(first.get("historyTypes").map(String::as_str), Some("messageAdded"));
    assert_eq!(first.get("labelId").map(String::as_str), Some("INBOX"));
    assert!(!first.contains_key("pageToken"));
    assert!(calls[1].target.contains("pageToken=page-2"));
    assert!(calls.iter().all(|c| c.method == "GET"));
}

#[tokio::test]
async fn send_is_from_the_alias_and_declares_an_automated_sender() {
    let google = spawn_fake_google(GOOGLE).await;

    let id = google.adapter().send(&mail()).await.unwrap();

    assert_eq!(id, "sent-1");
    let message = sent_message(&google);
    assert_eq!(header_of(&message, "From"), Some("\"Tally for SeaBeam\" <tally@seabeam.test>"));
    assert_eq!(header_of(&message, "To"), Some("estimator@example.com"));
    assert_eq!(header_of(&message, "Subject"), Some("Your pricing request"));
    assert_eq!(header_of(&message, "Auto-Submitted"), Some("auto-generated"));
    assert_eq!(header_of(&message, "X-SeaBeam-Agent"), Some("tally"));
    assert_eq!(header_of(&message, "Content-Transfer-Encoding"), Some("base64"));
    let body: String = message
        .split_once("\r\n\r\n")
        .unwrap()
        .1
        .split("\r\n")
        .collect();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.decode(body).unwrap(),
        mail().body.as_bytes()
    );
}

#[tokio::test]
async fn a_non_ascii_subject_is_sent_as_encoded_words() {
    let google = spawn_fake_google(GOOGLE).await;
    let subject = "Angebot für Jimerson — Preisübersicht und Rabattstaffel für 2027 ✓".to_string();

    google
        .adapter()
        .send(&OutboundMail {
            subject: subject.clone(),
            ..mail()
        })
        .await
        .unwrap();

    let message = sent_message(&google);
    let head = message.split("\r\n\r\n").next().unwrap();
    let start = head.find("Subject: ").unwrap() + "Subject: ".len();
    let folded = &head[start..head[start..].find("\r\nMIME-Version").unwrap() + start];
    let words: Vec<&str> = folded.split("\r\n ").collect();
    assert!(words.len() > 1, "a long subject folds into several words");
    let decoded: Vec<u8> = words
        .iter()
        .flat_map(|w| {
            assert!(w.len() <= 75, "{w}");
            let b64 = w.strip_prefix("=?UTF-8?B?").unwrap().strip_suffix("?=").unwrap();
            base64::engine::general_purpose::STANDARD.decode(b64).unwrap()
        })
        .collect();
    assert_eq!(String::from_utf8(decoded).unwrap(), subject);
}

#[tokio::test]
async fn a_line_break_in_a_header_value_is_refused_before_sending() {
    let google = spawn_fake_google(GOOGLE).await;

    let err = google
        .adapter()
        .send(&OutboundMail {
            subject: "Hi\r\nBcc: everyone@example.com".to_string(),
            ..mail()
        })
        .await
        .unwrap_err();

    assert!(err.contains("subject contains a line break"), "{err}");
    assert!(google.requests_to("/gmail/v1/users/me/messages/send").is_empty());
}

#[test]
fn a_key_file_without_a_usable_private_key_is_refused() {
    let no_key = json!({"client_email": SA_EMAIL, "token_uri": "https://oauth2.googleapis.com/token"});
    let bad_key = json!({
        "client_email": SA_EMAIL,
        "private_key": "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    });

    let missing = GmailAdapter::new(&gmail_config(), &no_key.to_string(), crate::gmail::GMAIL_API);
    let garbage = GmailAdapter::new(&gmail_config(), &bad_key.to_string(), crate::gmail::GMAIL_API);

    assert!(missing.err().unwrap().contains("gmail service account key"));
    assert!(garbage.err().unwrap().contains("private_key"));
}

#[tokio::test]
async fn platform_mail_is_from_the_mailbox_itself_with_no_agent_header() {
    let google = spawn_fake_google(GOOGLE).await;

    google
        .adapter()
        .send(&OutboundMail { agent: None, ..mail() })
        .await
        .unwrap();

    let message = sent_message(&google);
    assert_eq!(header_of(&message, "From"), Some("\"SeaBeam\" <hello@seabeam.test>"));
    assert_eq!(header_of(&message, "Auto-Submitted"), Some("auto-generated"));
    assert_eq!(header_of(&message, "X-SeaBeam-Agent"), None);
}

/// The decoded text body of a sent message.
fn body_of(message: &str) -> String {
    let body: String = message.split_once("\r\n\r\n").unwrap().1.split("\r\n").collect();
    String::from_utf8(base64::engine::general_purpose::STANDARD.decode(body).unwrap()).unwrap()
}

#[tokio::test]
async fn a_sign_in_mail_carries_the_link_from_the_mailbox() {
    use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::sign_in_mail::SignInMailHandler;
    use mailbox_model::types::services::mailbox::mailbox::SendSignInMailRequest;

    let google = spawn_fake_google(GOOGLE).await;
    let svc = crate::service::SignInMailService::default();
    svc.set_gmail(Some(Arc::new(google.adapter())));
    let link = "https://app.example.com/signin/callback?sign_in_link=c2lnbi1pbi1saW5r";

    svc.send_sign_in_mail(
        None,
        SendSignInMailRequest {
            to: "pat@selfhosted.example".to_string(),
            link: link.to_string(),
            expires_in_seconds: 900,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let message = sent_message(&google);
    assert_eq!(header_of(&message, "To"), Some("pat@selfhosted.example"));
    assert_eq!(header_of(&message, "From"), Some("\"SeaBeam\" <hello@seabeam.test>"));
    assert_eq!(header_of(&message, "Subject"), Some("Your SeaBeam sign-in link"));
    let body = body_of(&message);
    assert!(body.contains(link), "{body}");
    assert!(body.contains("within 15 minutes"), "{body}");
}

#[tokio::test]
async fn a_sign_in_mail_without_gmail_is_a_failed_precondition() {
    use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::sign_in_mail::SignInMailHandler;
    use mailbox_model::types::services::mailbox::mailbox::SendSignInMailRequest;

    let err = crate::service::SignInMailService::default()
        .send_sign_in_mail(
            None,
            SendSignInMailRequest {
                to: "pat@selfhosted.example".to_string(),
                link: "https://app.example.com/?sign_in_link=x".to_string(),
                expires_in_seconds: 900,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}
