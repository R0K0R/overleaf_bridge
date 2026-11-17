//! A minimal, independent Overleaf client ("another browser") used by the
//! integration test: login, REST, socket.io 0.9 with acks, and a tiny
//! ShareJS doc client. Not used by the bridge itself.
use crate::ot::{self, Op};
use crate::sio::{self, Packet};
use futures_util::{SinkExt, StreamExt};
use reqwest::cookie::{CookieStore, Jar};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

pub struct Raw {
    pub http: reqwest::Client,
    pub jar: Arc<Jar>,
    pub base: url::Url,
    pub csrf: String,
}

fn meta(html: &str, name: &str) -> Option<String> {
    let i = html.find(&format!("name=\"{name}\""))?;
    let ts = html[..i].rfind('<')?;
    let te = i + html[i..].find('>')?;
    let tag = &html[ts..te];
    let c = tag.find("content=\"")? + 9;
    Some(tag[c..c + tag[c..].find('"')?].replace("&quot;", "\"").replace("&amp;", "&"))
}

impl Raw {
    pub async fn login(base: &str, email: &str, password: &str) -> Result<Raw, String> {
        let jar = Arc::new(Jar::default());
        let http = reqwest::Client::builder().cookie_provider(jar.clone()).redirect(reqwest::redirect::Policy::none()).build().map_err(|e| e.to_string())?;
        let base = url::Url::parse(base).map_err(|e| e.to_string())?;
        let html = http.get(base.join("/login").unwrap()).send().await.map_err(|e| e.to_string())?.text().await.map_err(|e| e.to_string())?;
        let csrf = meta(&html, "ol-csrfToken").ok_or("no csrf on /login")?;
        let r = http.post(base.join("/login").unwrap()).header("X-Csrf-Token", &csrf).json(&json!({"_csrf": csrf, "email": email, "password": password})).send().await.map_err(|e| e.to_string())?;
        if !r.status().is_success() && !r.status().is_redirection() {
            return Err(format!("login HTTP {}", r.status()));
        }
        let html = http.get(base.join("/project").unwrap()).send().await.map_err(|e| e.to_string())?;
        if html.status().is_redirection() {
            return Err("login rejected".into());
        }
        let html = html.text().await.map_err(|e| e.to_string())?;
        let csrf = meta(&html, "ol-csrfToken").ok_or("no csrf on /project")?;
        Ok(Raw { http, jar, base, csrf })
    }
    pub async fn post(&self, path: &str, body: Value) -> Result<Value, String> {
        let r = self.http.post(self.base.join(path).unwrap()).header("X-Csrf-Token", &self.csrf).json(&body).send().await.map_err(|e| e.to_string())?;
        let st = r.status();
        let t = r.text().await.unwrap_or_default();
        if !st.is_success() {
            return Err(format!("HTTP {st}: {}", t.chars().take(200).collect::<String>()));
        }
        Ok(serde_json::from_str(&t).unwrap_or(json!({})))
    }
    pub async fn delete(&self, path: &str) -> Result<(), String> {
        let r = self.http.delete(self.base.join(path).unwrap()).header("X-Csrf-Token", &self.csrf).send().await.map_err(|e| e.to_string())?;
        if r.status().is_success() { Ok(()) } else { Err(format!("HTTP {}", r.status())) }
    }
    pub async fn create_project(&self, name: &str) -> Result<String, String> {
        let v = self.post("/project/new", json!({"projectName": name, "template": "none"})).await?;
        v.get("project_id").and_then(|x| x.as_str()).map(String::from).ok_or_else(|| format!("no project_id in {v}"))
    }
    pub async fn connect(&self, project_id: &str) -> Result<Sock, String> {
        let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        let cookie = self.jar.cookies(&self.base).and_then(|h| h.to_str().ok().map(String::from)).unwrap_or_default();
        let origin = self.base.as_str().trim_end_matches('/').to_string();
        let body = self.http.get(self.base.join(&format!("/socket.io/1/?projectId={project_id}&t={t}")).unwrap()).header("Cookie", &cookie).header("Origin", &origin).send().await.map_err(|e| e.to_string())?.text().await.map_err(|e| e.to_string())?;
        let sid = body.split(':').next().unwrap_or("").to_string();
        let host = format!("{}{}", self.base.host_str().unwrap(), self.base.port().map(|p| format!(":{p}")).unwrap_or_default());
        let scheme = if self.base.scheme() == "https" { "wss" } else { "ws" };
        let mut req = format!("{scheme}://{host}/socket.io/1/websocket/{sid}?projectId={project_id}&t={t}").into_client_request().map_err(|e| e.to_string())?;
        req.headers_mut().insert("Cookie", cookie.parse().unwrap());
        req.headers_mut().insert("Origin", origin.parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.map_err(|e| format!("ws: {e}"))?;
        let (mut sink, mut stream) = ws.split();
        let (wtx, mut wrx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Some(s) = wrx.recv().await {
                if sink.send(Message::Text(s.into())).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let acks: Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<Value>>>>> = Arc::default();
        let (etx, erx) = mpsc::unbounded_channel::<(String, Vec<Value>)>();
        let (a2, w2) = (acks.clone(), wtx.clone());
        tokio::spawn(async move {
            while let Some(Ok(m)) = stream.next().await {
                let Message::Text(t) = m else { continue };
                match sio::decode(&t) {
                    Ok(Packet::Heartbeat) => {
                        let _ = w2.send("2::".into());
                    }
                    Ok(Packet::Ack { id, args }) => {
                        if let Some(tx) = id.parse::<u64>().ok().and_then(|i| a2.lock().unwrap().remove(&i)) {
                            let _ = tx.send(args);
                        }
                    }
                    Ok(Packet::Event { name, args, .. }) => {
                        let _ = etx.send((name, args));
                    }
                    Ok(Packet::Disconnect) => {
                        let _ = etx.send(("__disconnect".into(), vec![]));
                    }
                    _ => {}
                }
            }
            let _ = etx.send(("__closed".into(), vec![]));
        });
        let hb = wtx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                if hb.send("2::".into()).is_err() {
                    break;
                }
            }
        });
        let mut s = Sock { wtx, acks, ctr: 0, events: erx, public_id: String::new(), project: Value::Null };
        // wait for joinProjectResponse
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let ev = tokio::time::timeout_at(deadline, s.events.recv()).await.map_err(|_| "no joinProjectResponse")?.ok_or("closed")?;
            if ev.0 == "joinProjectResponse" {
                s.public_id = ev.1[0]["publicId"].as_str().unwrap_or("").into();
                s.project = ev.1[0]["project"].clone();
                break;
            }
            if ev.0 == "connectionRejected" || ev.0 == "__closed" {
                return Err(format!("rejected: {:?}", ev));
            }
        }
        Ok(s)
    }
}

pub struct Sock {
    wtx: mpsc::UnboundedSender<String>,
    acks: Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<Value>>>>>,
    ctr: u64,
    pub events: mpsc::UnboundedReceiver<(String, Vec<Value>)>,
    pub public_id: String,
    pub project: Value,
}

impl Sock {
    pub fn emit(&mut self, name: &str, args: Vec<Value>) {
        let _ = self.wtx.send(sio::encode_event(None, name, args));
    }
    pub async fn call(&mut self, name: &str, args: Vec<Value>) -> Result<Vec<Value>, String> {
        self.ctr += 1;
        let (tx, rx) = oneshot::channel();
        self.acks.lock().unwrap().insert(self.ctr, tx);
        let _ = self.wtx.send(sio::encode_event(Some(self.ctr), name, args));
        let r = tokio::time::timeout(Duration::from_secs(20), rx).await.map_err(|_| format!("{name}: ack timeout"))?.map_err(|_| "closed".to_string())?;
        if let Some(e) = r.first().filter(|e| !e.is_null()) {
            return Err(format!("{name}: {e}"));
        }
        Ok(r)
    }
    pub fn close(&self) {
        let _ = self.wtx.send("0::".into());
    }
    /// Join a doc: (text, version, type)
    pub async fn join_doc(&mut self, id: &str, history: bool) -> Result<(String, i64, String), String> {
        let r = self.call("joinDoc", vec![json!(id), json!({"supportsHistoryOT": history})]).await?;
        let ty = r.get(5).and_then(|x| x.as_str()).unwrap_or("sharejs-text-ot").to_string();
        let text = if ty == "history-ot" {
            r[1].get("content").and_then(|x| x.as_str()).unwrap_or("").to_string()
        } else {
            r[1].as_array().map(|a| a.iter().map(|l| sio::unpack_latin1_utf8(l.as_str().unwrap_or(""))).collect::<Vec<_>>().join("\n")).unwrap_or_default()
        };
        Ok((text, r[2].as_i64().unwrap_or(0), ty))
    }
}

/// A tiny ShareJS client for one doc: one op in flight, no buffer.
pub struct RawDoc {
    pub id: String,
    pub base: Vec<char>,
    pub text: Vec<char>,
    pub version: i64,
    pub inflight: Option<Op>,
    pub history: bool,
}
impl RawDoc {
    pub fn new(id: &str, text: &str, version: i64) -> Self {
        let t: Vec<char> = text.chars().collect();
        RawDoc { id: id.into(), base: t.clone(), text: t, version, inflight: None, history: false }
    }
    /// Start an edit (needs no op in flight); returns the `applyOtUpdate` payload.
    pub fn submit(&mut self, op: Op) -> Result<Value, String> {
        if self.inflight.is_some() {
            return Err("op already in flight".into());
        }
        let comps = if self.history { ot::to_history(&op, &self.base)? } else { Value::Array(ot::to_sharejs(&op, &self.base)?) };
        self.text = ot::apply(&self.text, &op)?;
        self.inflight = Some(op);
        Ok(json!({"doc": self.id, "op": comps, "v": self.version}))
    }
    /// Feed an `otUpdateApplied` event for this doc; `public_id` is ours.
    pub fn on_update(&mut self, u: &Value, public_id: &str) -> Result<(), String> {
        let v = u["v"].as_i64().unwrap_or(-1);
        // history-ot reports the submitted version, not the applied one: no checks
        if !self.history {
            if v < self.version {
                return Ok(());
            }
            if v > self.version {
                return Err(format!("gap: have {} got {v}", self.version));
            }
        }
        let own = u["meta"]["source"].as_str() == Some(public_id);
        let no_op = u.get("op").map(|o| o.is_null()).unwrap_or(true);
        if no_op || own {
            if let Some(inf) = self.inflight.take() {
                self.base = ot::apply(&self.base, &inf)?;
                self.version += 1;
            }
            return Ok(());
        }
        let s = if self.history { ot::from_history(&u["op"], &self.base)? } else { ot::from_sharejs(u["op"].as_array().ok_or("op")?, &self.base)? };
        let mut s1 = s.clone();
        if let Some(inf) = self.inflight.take() {
            let (i2, s2) = ot::transform(&inf, &s1)?;
            self.inflight = Some(i2);
            s1 = s2;
        }
        self.base = ot::apply(&self.base, &s)?;
        self.text = ot::apply(&self.text, &s1)?;
        self.version += 1;
        Ok(())
    }
}
