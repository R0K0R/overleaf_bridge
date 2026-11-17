//! The real Overleaf backend: login, REST calls and the socket.io 0.9
//! websocket. Parts ported in spirit from nvim-overleaf (MIT, mei28):
//! cookie/CSRF handling and the socket.io 0.9 client structure. Bugs listed in
//! the emacs-collab research notes were fixed (ack is `otUpdateApplied`, not
//! the emit callback; op strings are not Latin-1 decoded).
use crate::net::*;
use crate::sio::{self, Packet};
use crate::Backend;
use futures_util::{SinkExt, StreamExt};
use reqwest::cookie::{CookieStore, Jar};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct RealBackend {
    tx: UnboundedSender<NetCmd>,
}
impl Backend for RealBackend {
    fn send(&mut self, cmd: NetCmd) {
        let _ = self.tx.send(cmd);
    }
}

pub fn spawn(ev: UnboundedSender<NetEvent>) -> RealBackend {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut n = Net::new(ev);
        n.run(rx).await;
    });
    RealBackend { tx }
}

enum AckKind {
    JoinDoc(String),
    Update(String),
    Users,
}

struct Net {
    ev: UnboundedSender<NetEvent>,
    http: reqwest::Client,
    jar: Arc<Jar>,
    base: Option<url::Url>,
    creds: Option<Credentials>,
    project_id: String,
    csrf: String,
    user_id: String,
    user_name: String,
    ws: Option<Ws>,
    acks: HashMap<u64, AckKind>,
    ack_ctr: u64,
    hb: Duration,
    relogged: bool,
}

fn meta(html: &str, name: &str) -> Option<String> {
    let key = format!("name=\"{name}\"");
    let i = html.find(&key)?;
    let tag_start = html[..i].rfind('<')?;
    let tag_end = i + html[i..].find('>')?;
    let tag = &html[tag_start..tag_end];
    let c = tag.find("content=\"")? + 9;
    let end = tag[c..].find('"')?;
    Some(unescape(&tag[c..c + end]))
}
fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn parse_folder(v: &Value) -> Folder {
    let s = |x: &Value, k: &str| x.get(k).and_then(|y| y.as_str()).unwrap_or("").to_string();
    let list = |k: &str| -> Vec<(String, String)> {
        v.get(k)
            .and_then(|x| x.as_array())
            .map(|a| a.iter().map(|e| (s(e, "_id"), s(e, "name"))).collect())
            .unwrap_or_default()
    };
    Folder {
        id: s(v, "_id"),
        name: s(v, "name"),
        docs: list("docs"),
        files: list("fileRefs"),
        folders: v
            .get("folders")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().map(parse_folder).collect())
            .unwrap_or_default(),
    }
}

fn parse_update(u: &Value) -> WireUpdate {
    WireUpdate {
        v: u.get("v").and_then(|x| x.as_i64()).unwrap_or(-1),
        op: u.get("op").cloned().filter(|o| !o.is_null()),
        source: u
            .get("meta")
            .and_then(|m| m.get("source"))
            .and_then(|s| s.as_str())
            .map(String::from),
    }
}

impl Net {
    fn new(ev: UnboundedSender<NetEvent>) -> Self {
        let jar = Arc::new(Jar::default());
        let http = reqwest::Client::builder()
            .cookie_provider(jar.clone())
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("overleaf_bridge/0.1 (emacs-collab)")
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        Net {
            ev,
            http,
            jar,
            base: None,
            creds: None,
            project_id: String::new(),
            csrf: String::new(),
            user_id: String::new(),
            user_name: String::new(),
            ws: None,
            acks: HashMap::new(),
            ack_ctr: 0,
            hb: Duration::from_secs(25),
            relogged: false,
        }
    }
    fn emit(&self, e: NetEvent) {
        let _ = self.ev.send(e);
    }
    fn log(&self, s: impl Into<String>) {
        self.emit(NetEvent::Log(s.into()));
    }
    fn url(&self, path: &str) -> url::Url {
        self.base.as_ref().unwrap().join(path).unwrap()
    }
    fn origin(&self) -> String {
        let u = self.base.as_ref().unwrap();
        let mut s = format!("{}://{}", u.scheme(), u.host_str().unwrap_or(""));
        if let Some(p) = u.port() {
            s.push_str(&format!(":{p}"));
        }
        s
    }
    fn cookie_header(&self) -> String {
        self.jar
            .cookies(self.base.as_ref().unwrap())
            .and_then(|h| h.to_str().ok().map(String::from))
            .unwrap_or_default()
    }

    async fn run(&mut self, mut rx: UnboundedReceiver<NetCmd>) {
        let mut hb = tokio::time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    if !self.cmd(cmd).await { break }
                }
                frame = async { self.ws.as_mut().unwrap().next().await }, if self.ws.is_some() => {
                    match frame {
                        Some(Ok(Message::Text(t))) => self.on_text(&t).await,
                        Some(Ok(Message::Close(_))) | None => self.lost("websocket closed", true),
                        Some(Ok(_)) => {}
                        Some(Err(e)) => self.lost(&format!("websocket error: {e}"), true),
                    }
                }
                _ = hb.tick() => {
                    if self.ws.is_some() {
                        let hbd = self.hb;
                        let _ = hbd;
                        self.raw("2::").await;
                    }
                }
            }
        }
    }

    fn lost(&mut self, reason: &str, retry: bool) {
        if self.ws.take().is_some() {
            self.acks.clear();
            self.emit(NetEvent::Disconnected {
                reason: reason.into(),
                retry,
            });
        }
    }

    async fn raw(&mut self, s: &str) {
        if let Some(ws) = self.ws.as_mut() {
            if let Err(e) = ws.send(Message::Text(s.to_string())).await {
                self.lost(&format!("send failed: {e}"), true);
            }
        }
    }

    async fn emit_event(&mut self, name: &str, args: Vec<Value>, ack: Option<AckKind>) {
        let id = ack.map(|a| {
            self.ack_ctr += 1;
            self.acks.insert(self.ack_ctr, a);
            self.ack_ctr
        });
        let s = sio::encode_event(id, name, args);
        self.raw(&s).await;
    }

    /// Returns false to stop the task.
    async fn cmd(&mut self, cmd: NetCmd) -> bool {
        match cmd {
            NetCmd::Open { params, creds } => self.open(params, creds).await,
            NetCmd::Reconnect => {
                self.close_ws().await;
                self.connect().await;
            }
            NetCmd::JoinDoc {
                doc_id,
                from_version,
            } => {
                if self.ws.is_none() {
                    self.emit(NetEvent::DocJoined {
                        doc_id,
                        result: Err("offline".into()),
                    });
                } else {
                    let opts = json!({"supportsHistoryOT": true});
                    let args = match from_version {
                        Some(v) => vec![json!(doc_id), json!(v), opts],
                        None => vec![json!(doc_id), opts],
                    };
                    self.emit_event("joinDoc", args, Some(AckKind::JoinDoc(doc_id)))
                        .await;
                }
            }
            NetCmd::Update {
                doc_id,
                v,
                op,
                dup_if_source,
            } => {
                let mut u = json!({"doc": doc_id, "op": op, "v": v});
                if !dup_if_source.is_empty() {
                    u["dupIfSource"] = json!(dup_if_source);
                }
                self.emit_event(
                    "applyOtUpdate",
                    vec![json!(doc_id), u],
                    Some(AckKind::Update(doc_id)),
                )
                .await;
            }
            NetCmd::Cursor {
                doc_id,
                row,
                column,
            } => {
                self.emit_event(
                    "clientTracking.updatePosition",
                    vec![json!({"row": row, "column": column, "doc_id": doc_id})],
                    None,
                )
                .await;
            }
            NetCmd::FetchFile { file_id } => {
                let (http, url, ev) = (
                    self.http.clone(),
                    self.url(&format!("/project/{}/file/{}", self.project_id, file_id)),
                    self.ev.clone(),
                );
                let cookie = self.cookie_header();
                tokio::spawn(async move {
                    let r = async {
                        let resp = http
                            .get(url)
                            .header("Cookie", cookie)
                            .send()
                            .await
                            .map_err(|e| e.to_string())?;
                        if !resp.status().is_success() {
                            return Err(format!("HTTP {}", resp.status()));
                        }
                        resp.bytes()
                            .await
                            .map(|b| b.to_vec())
                            .map_err(|e| e.to_string())
                    }
                    .await;
                    let _ = ev.send(NetEvent::FileData { file_id, result: r });
                });
            }
            NetCmd::Rest { tag, req } => self.rest(tag, req),
            NetCmd::DropConnection => self.lost("dropped on request", true),
            NetCmd::TestRemote { .. } => {}
            NetCmd::Close => {
                self.close_ws().await;
                return false;
            }
        }
        true
    }

    async fn close_ws(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.send(Message::Text("0::".to_string())).await;
            let _ = ws.close(None).await;
            self.acks.clear();
        }
    }

    // ---- REST ---------------------------------------------------------------
    fn rest(&mut self, tag: u64, req: RestReq) {
        let pid = self.project_id.clone();
        let (method, path, body): (&str, String, Option<Value>) = match &req {
            RestReq::CreateDoc { parent, name } => (
                "POST",
                format!("/project/{pid}/doc"),
                Some(json!({"name": name, "parent_folder_id": parent})),
            ),
            RestReq::CreateFolder { parent, name } => (
                "POST",
                format!("/project/{pid}/folder"),
                Some(json!({"name": name, "parent_folder_id": parent})),
            ),
            RestReq::Rename { kind, id, name } => (
                "POST",
                format!("/project/{pid}/{kind}/{id}/rename"),
                Some(json!({"name": name})),
            ),
            RestReq::Move { kind, id, folder } => (
                "POST",
                format!("/project/{pid}/{kind}/{id}/move"),
                Some(json!({"folder_id": folder})),
            ),
            RestReq::Delete { kind, id } => ("DELETE", format!("/project/{pid}/{kind}/{id}"), None),
        };
        let (http, url, ev, csrf, cookie) = (
            self.http.clone(),
            self.url(&path),
            self.ev.clone(),
            self.csrf.clone(),
            self.cookie_header(),
        );
        tokio::spawn(async move {
            let r = async {
                let mut rb = if method == "POST" {
                    http.post(url)
                } else {
                    http.delete(url)
                };
                rb = rb
                    .header("X-Csrf-Token", csrf)
                    .header("Cookie", cookie)
                    .header("Accept", "application/json");
                if let Some(b) = body {
                    rb = rb.json(&b);
                }
                let resp = rb.send().await.map_err(|e| e.to_string())?;
                let st = resp.status();
                let text = resp.text().await.unwrap_or_default();
                if !st.is_success() {
                    return Err(format!(
                        "HTTP {st}: {}",
                        text.chars().take(200).collect::<String>()
                    ));
                }
                Ok(serde_json::from_str::<Value>(&text).unwrap_or(json!({})))
            }
            .await;
            let _ = ev.send(NetEvent::RestDone { tag, result: r });
        });
    }

    // ---- login ----------------------------------------------------------------
    async fn open(&mut self, params: Value, creds: Option<Credentials>) {
        let server = params.get("server").and_then(|x| x.as_str()).unwrap_or("");
        let Ok(mut base) = url::Url::parse(server) else {
            self.emit(NetEvent::Fatal {
                message: format!("bad params.server {server:?}"),
            });
            return;
        };
        base.set_path("/");
        self.base = Some(base);
        self.creds = creds;
        match self.login().await {
            Ok(()) => {}
            Err(AuthErr::Need(r)) => {
                self.emit(NetEvent::NeedAuth { reason: r });
                return;
            }
            Err(AuthErr::Fatal(m)) => {
                self.emit(NetEvent::Fatal { message: m });
                return;
            }
        }
        // find the project
        let pid = match params.get("project_id").and_then(|x| x.as_str()) {
            Some(p) => p.to_string(),
            None => {
                let name = params
                    .get("project_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                match self.find_project(name).await {
                    Ok(p) => p,
                    Err(e) => {
                        self.emit(NetEvent::Fatal { message: e });
                        return;
                    }
                }
            }
        };
        self.project_id = pid;
        self.connect().await;
    }

    async fn login(&mut self) -> Result<(), AuthErr> {
        let Some(c) = self.creds.clone() else {
            return Err(AuthErr::Need("credentials required".into()));
        };
        let base = self.base.clone().unwrap();
        if c.method == "cookie" {
            let cookie = c
                .cookie
                .clone()
                .ok_or(AuthErr::Need("cookie missing".into()))?;
            let name = c.cookie_name.clone().unwrap_or_else(|| {
                if base
                    .host_str()
                    .map(|h| h.ends_with("overleaf.com"))
                    .unwrap_or(false)
                {
                    "overleaf_session2".into()
                } else {
                    "overleaf.sid".into()
                }
            });
            let kv = if cookie.contains('=') {
                cookie
            } else {
                format!("{name}={cookie}")
            };
            for part in kv.split(';') {
                self.jar.add_cookie_str(part.trim(), &base);
            }
        } else {
            let (Some(email), Some(password)) = (c.email.clone(), c.password.clone()) else {
                return Err(AuthErr::Need("email/password missing".into()));
            };
            let r = self
                .http
                .get(self.url("/login"))
                .send()
                .await
                .map_err(|e| AuthErr::Fatal(format!("cannot reach server: {e}")))?;
            let html = r.text().await.unwrap_or_default();
            let csrf = meta(&html, "ol-csrfToken")
                .or_else(|| {
                    html.find("name=\"_csrf\"").and_then(|i| {
                        let t = &html[i..];
                        let v = t.find("value=\"")? + 7;
                        Some(t[v..v + t[v..].find('"')?].to_string())
                    })
                })
                .ok_or(AuthErr::Fatal("no CSRF token on the login page".into()))?;
            let r = self
                .http
                .post(self.url("/login"))
                .header("X-Csrf-Token", &csrf)
                .json(&json!({"_csrf": csrf, "email": email, "password": password}))
                .send()
                .await
                .map_err(|e| AuthErr::Fatal(format!("login failed: {e}")))?;
            let st = r.status();
            let body = r.text().await.unwrap_or_default();
            if st.as_u16() == 401 || st.as_u16() == 400 {
                return Err(AuthErr::Need("email or password rejected".into()));
            }
            if st.as_u16() == 429 {
                return Err(AuthErr::Fatal("login rate limited; wait a minute".into()));
            }
            if !(st.is_success() || st.is_redirection()) {
                return Err(AuthErr::Fatal(format!(
                    "login HTTP {st}: {}",
                    body.chars().take(200).collect::<String>()
                )));
            }
            if body.contains("\"message\"") && body.contains("\"error\"") {
                return Err(AuthErr::Need("email or password rejected".into()));
            }
        }
        // CSRF + identity from /project; also validates the session
        let r = self
            .http
            .get(self.url("/project"))
            .send()
            .await
            .map_err(|e| AuthErr::Fatal(format!("cannot reach server: {e}")))?;
        if r.status().is_redirection() {
            return Err(AuthErr::Need(
                "session rejected (redirected to login)".into(),
            ));
        }
        let html = r.text().await.unwrap_or_default();
        let Some(csrf) = meta(&html, "ol-csrfToken") else {
            return Err(AuthErr::Need("not logged in (no CSRF token)".into()));
        };
        self.csrf = csrf;
        self.user_id = meta(&html, "ol-user_id").unwrap_or_default();
        if let Some(u) = meta(&html, "ol-user").and_then(|s| serde_json::from_str::<Value>(&s).ok())
        {
            let g = |k: &str| u.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
            let n = format!("{} {}", g("first_name"), g("last_name"))
                .trim()
                .to_string();
            self.user_name = if n.is_empty() { g("email") } else { n };
        }
        if self.user_name.is_empty() {
            self.user_name = "overleaf".into();
        }
        // GCLB affinity cookie (harmless elsewhere)
        let _ = self
            .http
            .get(self.url("/socket.io/socket.io.js"))
            .send()
            .await;
        Ok(())
    }

    async fn find_project(&mut self, name: &str) -> Result<String, String> {
        let r = self
            .http
            .get(self.url("/project"))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let html = r.text().await.map_err(|e| e.to_string())?;
        let blob = meta(&html, "ol-prefetchedProjectsBlob")
            .ok_or("project list not found; pass params.project_id")?;
        let v: Value = serde_json::from_str(&blob).map_err(|e| e.to_string())?;
        let ps = v
            .get("projects")
            .and_then(|x| x.as_array())
            .ok_or("no projects in list")?;
        let m: Vec<&Value> = ps
            .iter()
            .filter(|p| p.get("name").and_then(|x| x.as_str()) == Some(name))
            .collect();
        match m.len() {
            1 => Ok(m[0]
                .get("id")
                .or_else(|| m[0].get("_id"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string()),
            0 => Err(format!("no project named {name:?}")),
            _ => Err(format!(
                "{} projects named {name:?}; pass params.project_id",
                m.len()
            )),
        }
    }

    // ---- socket ---------------------------------------------------------------
    async fn connect(&mut self) {
        match self.connect_inner().await {
            Ok(()) => {}
            Err(ConnErr::Auth(r)) => {
                // session expired: log in again once (password), else ask
                if !self.relogged
                    && self
                        .creds
                        .as_ref()
                        .map(|c| c.method == "password")
                        .unwrap_or(false)
                {
                    self.relogged = true;
                    if self.login().await.is_ok() {
                        if let Err(e) = self.connect_inner().await {
                            self.conn_failed(e);
                        } else {
                            self.relogged = false;
                        }
                        return;
                    }
                }
                self.emit(NetEvent::Disconnected {
                    reason: r,
                    retry: false,
                });
            }
            Err(e) => self.conn_failed(e),
        }
    }
    fn conn_failed(&self, e: ConnErr) {
        match e {
            ConnErr::Auth(r) => self.emit(NetEvent::Disconnected {
                reason: r,
                retry: false,
            }),
            ConnErr::Other(r) => self.emit(NetEvent::Disconnected {
                reason: r,
                retry: true,
            }),
        }
    }

    async fn connect_inner(&mut self) -> Result<(), ConnErr> {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let hs_url = self.url(&format!(
            "/socket.io/1/?projectId={}&t={t}",
            self.project_id
        ));
        let r = self
            .http
            .get(hs_url)
            .header("Origin", self.origin())
            .header("Cookie", self.cookie_header())
            .send()
            .await
            .map_err(|e| ConnErr::Other(format!("handshake: {e}")))?;
        let st = r.status();
        let body = r.text().await.unwrap_or_default();
        if st.as_u16() == 401 || st.as_u16() == 403 || st.is_redirection() {
            return Err(ConnErr::Auth(format!("handshake HTTP {st}")));
        }
        if !st.is_success() {
            return Err(ConnErr::Other(format!(
                "handshake HTTP {st}: {}",
                body.chars().take(100).collect::<String>()
            )));
        }
        let parts: Vec<&str> = body.split(':').collect();
        if parts.len() < 4 {
            return Err(ConnErr::Other(format!("bad handshake {body:.80}")));
        }
        let sid = parts[0];
        let hbt: u64 = parts[1].parse().unwrap_or(25);
        self.hb = Duration::from_secs((hbt as f64 * 0.8) as u64);
        let base = self.base.clone().unwrap();
        let scheme = if base.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        let mut host = base.host_str().unwrap_or("").to_string();
        if let Some(p) = base.port() {
            host.push_str(&format!(":{p}"));
        }
        let ws_url = format!(
            "{scheme}://{host}/socket.io/1/websocket/{sid}?projectId={}&t={t}",
            self.project_id
        );
        let mut req = ws_url
            .into_client_request()
            .map_err(|e| ConnErr::Other(e.to_string()))?;
        req.headers_mut().insert(
            "Cookie",
            self.cookie_header()
                .parse()
                .map_err(|_| ConnErr::Other("bad cookie".into()))?,
        );
        req.headers_mut().insert(
            "Origin",
            self.origin()
                .parse()
                .map_err(|_| ConnErr::Other("bad origin".into()))?,
        );
        let (ws, _) = tokio::time::timeout(
            Duration::from_secs(20),
            tokio_tungstenite::connect_async(req),
        )
        .await
        .map_err(|_| ConnErr::Other("websocket connect timeout".into()))?
        .map_err(|e| ConnErr::Other(format!("websocket: {e}")))?;
        self.ws = Some(ws);
        Ok(())
    }

    async fn on_text(&mut self, t: &str) {
        let p = match sio::decode(t) {
            Ok(p) => p,
            Err(e) => {
                self.log(format!("undecodable frame: {e}"));
                return;
            }
        };
        match p {
            Packet::Heartbeat => self.raw("2::").await,
            Packet::Disconnect => self.lost("server disconnected", true),
            Packet::Error(e) => {
                self.log(format!("socket error: {e}"));
                let auth = e.contains("unauthorized") || e.contains("not authorized");
                self.lost(&format!("socket error: {e}"), !auth);
            }
            Packet::Ack { id, args } => {
                let Some(k) = id.parse::<u64>().ok().and_then(|i| self.acks.remove(&i)) else {
                    return;
                };
                self.on_ack(k, args);
            }
            Packet::Event { name, args, .. } => self.on_event(&name, args).await,
            _ => {}
        }
    }

    fn on_ack(&mut self, k: AckKind, args: Vec<Value>) {
        let err = args.first().filter(|e| !e.is_null()).map(|e| {
            e.get("message")
                .and_then(|m| m.as_str())
                .map(String::from)
                .unwrap_or_else(|| e.to_string())
        });
        match k {
            AckKind::Update(doc_id) => {
                if let Some(message) = err {
                    self.emit(NetEvent::UpdateError { doc_id, message });
                }
                // success only means "queued": the real ack is otUpdateApplied
            }
            AckKind::Users => {
                if let Some(users) = args.get(1).and_then(|x| x.as_array()) {
                    for u in users {
                        let cd = u.get("cursorData");
                        let id = u
                            .get("client_id")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        let usr = u.get("user");
                        let name = usr
                            .map(|x| {
                                format!(
                                    "{} {}",
                                    x.get("first_name").and_then(|y| y.as_str()).unwrap_or(""),
                                    x.get("last_name").and_then(|y| y.as_str()).unwrap_or("")
                                )
                                .trim()
                                .to_string()
                            })
                            .filter(|s| !s.is_empty())
                            .or_else(|| {
                                usr.and_then(|x| x.get("email"))
                                    .and_then(|y| y.as_str())
                                    .map(String::from)
                            })
                            .unwrap_or_else(|| "user".into());
                        self.emit(NetEvent::Peer(Peer {
                            id,
                            name,
                            doc_id: cd
                                .and_then(|c| c.get("doc_id"))
                                .and_then(|x| x.as_str())
                                .map(String::from),
                            row: cd
                                .and_then(|c| c.get("row"))
                                .and_then(|x| x.as_u64())
                                .map(|x| x as usize),
                            column: cd
                                .and_then(|c| c.get("column"))
                                .and_then(|x| x.as_u64())
                                .map(|x| x as usize),
                        }));
                    }
                }
            }
            AckKind::JoinDoc(doc_id) => {
                if let Some(message) = err {
                    self.emit(NetEvent::DocJoined {
                        doc_id,
                        result: Err(message),
                    });
                    return;
                }
                let kind = if args.get(5).and_then(|x| x.as_str()) == Some("history-ot") {
                    DocType::History
                } else {
                    DocType::Share
                };
                let lines = args.get(1).cloned().unwrap_or(Value::Null);
                let text = match (&kind, &lines) {
                    (DocType::History, v) => v
                        .get("content")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    (_, Value::Array(a)) => a
                        .iter()
                        .map(|l| sio::unpack_latin1_utf8(l.as_str().unwrap_or("")))
                        .collect::<Vec<_>>()
                        .join("\n"),
                    _ => String::new(),
                };
                let version = args.get(2).and_then(|x| x.as_i64()).unwrap_or(0);
                let updates = args
                    .get(3)
                    .and_then(|x| x.as_array())
                    .map(|a| a.iter().map(parse_update).collect())
                    .unwrap_or_default();
                self.emit(NetEvent::DocJoined {
                    doc_id,
                    result: Ok(DocSnapshot {
                        text,
                        version,
                        kind,
                        updates,
                    }),
                });
            }
        }
    }

    async fn on_event(&mut self, name: &str, args: Vec<Value>) {
        let s = |i: usize| {
            args.get(i)
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string()
        };
        match name {
            "connectionRejected" => {
                let msg = args
                    .first()
                    .and_then(|a| a.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("rejected")
                    .to_string();
                let retry = msg == "retry" || msg.contains("retry");
                self.lost(&format!("connection rejected: {msg}"), retry);
            }
            "reconnectGracefully" | "forceDisconnect" => {
                self.log(format!("{name}: {:?}", args));
                self.close_ws().await;
                self.emit(NetEvent::Disconnected {
                    reason: name.into(),
                    retry: true,
                });
            }
            "joinProjectResponse" => {
                let Some(r) = args.first() else { return };
                let public_id = r
                    .get("publicId")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let p = r.get("project").cloned().unwrap_or(Value::Null);
                let root = p
                    .get("rootFolder")
                    .and_then(|x| x.get(0))
                    .map(parse_folder)
                    .unwrap_or_default();
                let project = ProjectInfo {
                    id: p.get("_id").and_then(|x| x.as_str()).unwrap_or("").into(),
                    name: p
                        .get("name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("project")
                        .into(),
                    root,
                };
                self.emit(NetEvent::Connected {
                    public_id,
                    project,
                    user_name: self.user_name.clone(),
                    user_id: self.user_id.clone(),
                });
                self.emit_event(
                    "clientTracking.getConnectedUsers",
                    vec![],
                    Some(AckKind::Users),
                )
                .await;
            }
            "otUpdateApplied" => {
                if let Some(u) = args.first() {
                    let doc_id = u
                        .get("doc")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    self.emit(NetEvent::Update {
                        doc_id,
                        update: parse_update(u),
                    });
                }
            }
            "otUpdateError" => {
                let msg = args
                    .first()
                    .map(|e| {
                        e.get("message")
                            .and_then(|m| m.as_str())
                            .map(String::from)
                            .unwrap_or_else(|| e.to_string())
                    })
                    .unwrap_or_default();
                let doc_id = args
                    .get(1)
                    .and_then(|m| m.get("doc_id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                self.emit(NetEvent::UpdateError {
                    doc_id,
                    message: msg,
                });
            }
            "clientTracking.clientUpdated" => {
                if let Some(u) = args.first() {
                    let g = |k: &str| u.get(k).and_then(|x| x.as_str()).map(String::from);
                    let name = g("name")
                        .filter(|n| !n.is_empty())
                        .or_else(|| g("email"))
                        .unwrap_or_else(|| "user".into());
                    self.emit(NetEvent::Peer(Peer {
                        id: g("id").unwrap_or_default(),
                        name,
                        doc_id: g("doc_id"),
                        row: u.get("row").and_then(|x| x.as_u64()).map(|x| x as usize),
                        column: u.get("column").and_then(|x| x.as_u64()).map(|x| x as usize),
                    }));
                }
            }
            "clientTracking.clientDisconnected" => self.emit(NetEvent::PeerLeft { id: s(0) }),
            "reciveNewDoc" => self.tree_new(&args, 0),
            "reciveNewFile" => self.tree_new(&args, 1),
            "reciveNewFolder" => self.tree_new(&args, 2),
            "reciveEntityRename" => self.emit(NetEvent::Tree(TreeEvent::Rename {
                id: s(0),
                name: s(1),
            })),
            "reciveEntityMove" => self.emit(NetEvent::Tree(TreeEvent::Move {
                id: s(0),
                folder: s(1),
            })),
            "removeEntity" => self.emit(NetEvent::Tree(TreeEvent::Remove { id: s(0) })),
            _ => {}
        }
    }

    fn tree_new(&self, args: &[Value], kind: u8) {
        let parent = args
            .first()
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let Some(e) = args.get(1) else { return };
        let id = e
            .get("_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let name = e
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        self.emit(NetEvent::Tree(match kind {
            0 => TreeEvent::NewDoc { parent, id, name },
            1 => TreeEvent::NewFile { parent, id, name },
            _ => TreeEvent::NewFolder { parent, id, name },
        }));
    }
}

enum AuthErr {
    Need(String),
    Fatal(String),
}
enum ConnErr {
    Auth(String),
    Other(String),
}
