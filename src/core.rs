//! The bridge core: a synchronous state machine. It speaks emacs-collab to
//! Emacs (messages in, messages out) and talks to the network layer through
//! `NetCmd`/`NetEvent` (see net.rs). Two-party sync with Emacs follows
//! PROTOCOL.md section 4; the Overleaf side keeps, per document, the
//! server-confirmed text (`base`), one inflight op awaiting its ack, and one
//! composed buffer of later local edits (as the ShareJS client does).
use crate::net::*;
use crate::ot::{self, Op, C};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const ACK_DELAY: Duration = Duration::from_millis(300);
const RESEND_AFTER: Duration = Duration::from_secs(5);
const GAP_REJOIN_AFTER: Duration = Duration::from_secs(10);
const CAPS: &[&str] = &["cursors", "files", "binary"];

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Doc,
    File,
    Folder,
}
impl Kind {
    fn rest(&self) -> &'static str {
        match self {
            Kind::Doc => "doc",
            Kind::File => "file",
            Kind::Folder => "folder",
        }
    }
}
struct Ent {
    kind: Kind,
    name: String,
    parent: Option<String>,
}

struct Doc {
    id: String,
    path: String,
    kind: DocType,
    /// Joined on the current connection.
    joined: bool,
    base: Vec<char>,
    version: i64,
    inflight: Option<Op>,
    /// Version the inflight op was sent at.
    inflight_v: i64,
    sent_at: Instant,
    buffer: Option<Op>,
    text: Vec<char>,
    ooo: BTreeMap<i64, WireUpdate>,
    ooo_since: Option<Instant>,
    /// After an update error: rejoin from scratch and take the server's text.
    hard_resync: bool,
    rejoining: bool,
    // emacs-collab state
    attached: bool,
    broken: bool,
    sent: u64,
    seen: u64,
    pending: Vec<(u64, Op)>,
    ack_due: Option<Instant>,
    dirty: bool,
    pending_text: Option<String>,
}

enum Job {
    CreateDoc {
        path: String,
        text: Option<String>,
    },
    Mkdir {
        rest: Vec<String>,
        parent: String,
        path_so_far: String,
        then: Box<Job>,
    },
    Rename {
        id: String,
        kind: Kind,
        from: String,
        to: String,
        step: u8,
    },
    Delete {
        id: String,
        kind: Kind,
        path: String,
    },
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Phase {
    Idle,
    Opening,
    Initial,
    Ready,
}

pub struct Bridge {
    pub test: bool,
    /// Debug hooks (x-test-text/-pending/-drop) outside --test, for integration tests.
    pub hooks: bool,
    pub emacs_out: Vec<Value>,
    pub net_out: Vec<NetCmd>,
    pub exit: Option<i32>,
    now: Instant,
    phase: Phase,
    params: Value,
    root: Option<PathBuf>,
    public_id: String,
    own_ids: Vec<String>,
    user_name: String,
    user_id: String,
    project_name: String,
    root_id: String,
    ents: HashMap<String, Ent>,
    docs: HashMap<String, Doc>,
    init_wait: HashSet<String>,
    rejoin_wait: HashSet<String>,
    offline: bool,
    retry_at: Option<Instant>,
    attempts: u32,
    jobs: HashMap<u64, Job>,
    next_tag: u64,
    pending_texts: HashMap<String, String>,
    binary_fail: Vec<String>,
    sanitize_astral: bool,
    /// The open credentials are a session cookie: need_auth asks for one.
    cookie_auth: bool,
}

fn log(m: &str) {
    eprintln!("[overleaf_bridge] {m}");
}

fn s2c(s: &str) -> Vec<char> {
    s.chars().collect()
}
fn c2s(c: &[char]) -> String {
    c.iter().collect()
}

fn color_for(id: &str) -> String {
    const P: &[&str] = &[
        "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4", "#008080", "#9a6324", "#800000",
        "#808000", "#000075",
    ];
    let mut h: u32 = 2166136261;
    for b in id.bytes() {
        h = (h ^ b as u32).wrapping_mul(16777619);
    }
    P[(h as usize) % P.len()].to_string()
}

pub fn valid_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('/')
        && !p.contains('\\')
        && p.split('/').all(|c| !c.is_empty() && c != "..")
}

fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = path.with_file_name(format!(
        ".olb-{}-{}",
        std::process::id(),
        path.file_name().and_then(|s| s.to_str()).unwrap_or("x")
    ));
    std::fs::write(&tmp, data)?;
    if let Ok(md) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, md.permissions());
    }
    std::fs::rename(&tmp, path)
}

impl Bridge {
    pub fn new(test: bool) -> Self {
        Bridge {
            test,
            hooks: false,
            emacs_out: vec![],
            net_out: vec![],
            exit: None,
            now: Instant::now(),
            phase: Phase::Idle,
            params: Value::Null,
            root: None,
            public_id: String::new(),
            own_ids: vec![],
            user_name: String::new(),
            user_id: String::new(),
            project_name: String::new(),
            root_id: String::new(),
            ents: HashMap::new(),
            docs: HashMap::new(),
            init_wait: HashSet::new(),
            rejoin_wait: HashSet::new(),
            offline: false,
            retry_at: None,
            attempts: 0,
            jobs: HashMap::new(),
            next_tag: 1,
            pending_texts: HashMap::new(),
            binary_fail: vec![],
            sanitize_astral: !test,
            cookie_auth: false,
        }
    }

    // ---- output helpers ---------------------------------------------------
    fn emit(&mut self, v: Value) {
        self.emacs_out.push(v);
    }
    fn error(&mut self, code: &str, message: &str, path: Option<&str>, fatal: bool) {
        let mut m = json!({"t":"error","code":code,"message":message});
        if let Some(p) = path {
            m["path"] = json!(p);
        }
        if fatal {
            m["fatal"] = json!(true);
            self.exit = Some(1);
        }
        self.emit(m);
    }
    fn status(&mut self, state: &str, detail: Option<&str>) {
        let mut m = json!({"t":"status","state":state});
        if let Some(d) = detail {
            m["detail"] = json!(d);
        }
        self.emit(m);
    }
    fn cmd(&mut self, c: NetCmd) {
        self.net_out.push(c);
    }

    // ---- tree helpers -----------------------------------------------------
    fn path_of(&self, id: &str) -> Option<String> {
        let mut parts = vec![];
        let mut cur = id.to_string();
        let mut guard = 0;
        loop {
            let e = self.ents.get(&cur)?;
            match &e.parent {
                None => break,
                Some(p) => {
                    parts.push(e.name.clone());
                    cur = p.clone();
                }
            }
            guard += 1;
            if guard > 200 {
                return None;
            }
        }
        parts.reverse();
        Some(parts.join("/"))
    }
    fn ent_by_path(&self, path: &str) -> Option<String> {
        self.ents
            .keys()
            .find(|id| self.path_of(id).as_deref() == Some(path) && !path.is_empty())
            .cloned()
    }
    fn doc_by_path(&self, path: &str) -> Option<String> {
        self.docs
            .values()
            .find(|d| d.path == path)
            .map(|d| d.id.clone())
    }
    fn full(&self, path: &str) -> Option<PathBuf> {
        let r = self.root.as_ref()?;
        if !valid_path(path) {
            return None;
        }
        Some(r.join(path))
    }
    fn refresh_paths(&mut self) {
        let ids: Vec<String> = self.docs.keys().cloned().collect();
        for id in ids {
            if let Some(p) = self.path_of(&id) {
                if let Some(d) = self.docs.get_mut(&id) {
                    d.path = p;
                }
            }
        }
    }
    fn add_tree(&mut self, f: &Folder, parent: Option<&str>, is_root: bool) {
        self.ents.insert(
            f.id.clone(),
            Ent {
                kind: Kind::Folder,
                name: if is_root {
                    String::new()
                } else {
                    f.name.clone()
                },
                parent: parent.map(String::from),
            },
        );
        for (id, name) in &f.docs {
            self.ents.insert(
                id.clone(),
                Ent {
                    kind: Kind::Doc,
                    name: name.clone(),
                    parent: Some(f.id.clone()),
                },
            );
        }
        for (id, name) in &f.files {
            self.ents.insert(
                id.clone(),
                Ent {
                    kind: Kind::File,
                    name: name.clone(),
                    parent: Some(f.id.clone()),
                },
            );
        }
        for sub in &f.folders {
            self.add_tree(sub, Some(&f.id), false);
        }
    }
    fn all_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .ents
            .iter()
            .filter(|(_, e)| e.kind != Kind::Folder)
            .filter_map(|(id, _)| self.path_of(id))
            .collect();
        v.sort();
        v
    }

    // ---- mirror -----------------------------------------------------------
    fn write_mirror_doc(&mut self, id: &str) {
        let Some(d) = self.docs.get_mut(id) else {
            return;
        };
        d.dirty = false;
        let (path, text) = (d.path.clone(), c2s(&d.text));
        if let Some(full) = self.full(&path) {
            if let Err(e) = write_atomic(&full, text.as_bytes()) {
                self.error("write_failed", &e.to_string(), Some(&path), false);
            }
        }
    }
    fn flush_dirty(&mut self) {
        let ids: Vec<String> = self
            .docs
            .values()
            .filter(|d| d.dirty && !d.attached)
            .map(|d| d.id.clone())
            .collect();
        for id in ids {
            self.write_mirror_doc(&id);
            if let Some(p) = self.docs.get(&id).map(|d| d.path.clone()) {
                self.emit(json!({"t":"file_event","op":"change","path":p}));
            }
        }
    }

    // ---- Emacs messages ---------------------------------------------------
    pub fn handle_emacs(&mut self, m: Value) {
        self.now = Instant::now();
        let Some(t) = m.get("t").and_then(|x| x.as_str()) else {
            return;
        };
        if t.starts_with("x-test-")
            && !self.test
            && !(self.hooks && matches!(t, "x-test-text" | "x-test-pending" | "x-test-drop"))
        {
            return;
        }
        if self.root.is_none() && !matches!(t, "hello" | "open" | "close") {
            self.error("not_open", "open the project first", None, false);
            return;
        }
        match t {
            "hello" => self.emit(
                json!({"t":"hello","bridge":"overleaf_bridge","version":0,"capabilities":CAPS}),
            ),
            "open" => self.on_open(&m),
            "attach" => self.on_attach(&m),
            "detach" => self.on_detach(&m),
            "edit" => self.on_edit(&m),
            "ack" => self.on_ack(&m),
            "cursor" => self.on_cursor(&m),
            "file" => self.on_file(&m),
            "close" => {
                self.cmd(NetCmd::Close);
                self.exit = Some(0);
            }
            "x-test-remote" => self.on_x_test_remote(&m),
            "x-test-text" => {
                let path = m
                    .get("path")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                match self.doc_by_path(&path).and_then(|id| self.docs.get(&id)) {
                    Some(d) => {
                        let text = c2s(&d.text);
                        self.emit(json!({"t":"x-test-text","path":path,"text":text}));
                    }
                    None => self.error("test_failed", "no such doc", Some(&path), false),
                }
            }
            "x-test-pending" => {
                let path = m
                    .get("path")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let count = self
                    .doc_by_path(&path)
                    .and_then(|id| self.docs.get(&id))
                    .map(|d| d.pending.len())
                    .unwrap_or(0);
                self.emit(json!({"t":"x-test-pending","path":path,"count":count}));
            }
            "x-test-peer" => {
                let mut out = json!({"t":"peer"});
                for k in ["id", "name", "color", "path", "pos", "mark"] {
                    if let Some(v) = m.get(k) {
                        out[k] = v.clone();
                    }
                }
                self.emit(out);
            }
            "x-test-drop" => self.cmd(NetCmd::DropConnection),
            _ => log(&format!("ignoring message type {t}")),
        }
        self.flush_all_inner();
    }

    fn flush_all_inner(&mut self) {
        if self.test {
            self.flush_dirty();
        }
    }

    fn on_open(&mut self, m: &Value) {
        // `root` may be unset before open; handle_emacs lets "open" through.
        if self.phase != Phase::Idle {
            log("open while already open; ignored");
            return;
        }
        let params = m.get("params").cloned().unwrap_or(json!({}));
        let creds = m.get("credentials").filter(|c| c.is_object()).map(|c| {
            let g = |k: &str| c.get(k).and_then(|x| x.as_str()).map(String::from);
            Credentials {
                method: g("method").unwrap_or_else(|| "password".into()),
                email: g("email"),
                password: g("password"),
                cookie_name: g("cookie_name"),
                cookie: g("cookie"),
            }
        });
        self.cookie_auth = creds.as_ref().is_some_and(|c| c.method == "cookie");
        if !self.test {
            let ok = match &creds {
                Some(c) if c.method == "cookie" => c.cookie.is_some(),
                Some(c) => c.email.is_some() && c.password.is_some(),
                None => false,
            };
            if !ok {
                self.emit(json!({"t":"need_auth","fields":self.auth_fields(),"reason":"credentials required (password, or a session cookie)"}));
                return;
            }
        }
        if let Some(b) = params.get("sanitize_astral").and_then(|x| x.as_bool()) {
            self.sanitize_astral = b;
        }
        self.params = params.clone();
        self.phase = Phase::Opening;
        self.status("connecting", None);
        self.cmd(NetCmd::Open { params, creds });
        // `root` is set once the project is known; use a placeholder marker so
        // that other messages are accepted only after ready.
    }

    fn require_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    fn on_attach(&mut self, m: &Value) {
        let path = m
            .get("path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if !self.require_ready() {
            self.error("not_open", "project not ready", Some(&path), false);
            return;
        }
        let Some(id) = self.doc_by_path(&path) else {
            self.error(
                "attach_failed",
                "not a text document of this project",
                Some(&path),
                false,
            );
            return;
        };
        let d = self.docs.get_mut(&id).unwrap();
        d.attached = true;
        d.broken = false;
        d.sent = 0;
        d.seen = 0;
        d.pending.clear();
        d.ack_due = None;
        let text = c2s(&d.text);
        self.emit(json!({"t":"doc","path":path,"text":text,"seen":0}));
    }

    fn on_detach(&mut self, m: &Value) {
        let path = m.get("path").and_then(|x| x.as_str()).unwrap_or("");
        let Some(id) = self.doc_by_path(path) else {
            return;
        };
        let d = self.docs.get_mut(&id).unwrap();
        d.attached = false;
        d.broken = false;
        d.sent = 0;
        d.seen = 0;
        d.pending.clear();
        d.ack_due = None;
        self.write_mirror_doc(&id);
    }

    fn desync(&mut self, path: &str, id: Option<&str>, why: &str) {
        log(&format!("desync on {path}: {why}"));
        if let Some(d) = id.and_then(|i| self.docs.get_mut(i)) {
            d.broken = true;
        }
        self.error("desync", why, Some(path), false);
    }

    fn on_ack(&mut self, m: &Value) {
        let path = m
            .get("path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let Some(id) = self.doc_by_path(&path) else {
            return;
        };
        let seen = m.get("seen").and_then(|x| x.as_u64());
        let d = self.docs.get_mut(&id).unwrap();
        if !d.attached || d.broken {
            return;
        }
        match seen {
            Some(s) if s <= d.sent => d.pending.retain(|(q, _)| *q > s),
            _ => self.desync(&path, Some(&id), "ack.seen out of range"),
        }
    }

    fn on_edit(&mut self, m: &Value) {
        let path = m
            .get("path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let Some(id) = self.doc_by_path(&path) else {
            self.desync(&path, None, "edit for an unknown document");
            return;
        };
        {
            let d = &self.docs[&id];
            if !d.attached {
                self.desync(&path, None, "edit for a document that is not attached");
                return;
            }
            if d.broken {
                return;
            }
        }
        match self.edit_inner(&id, m) {
            Ok(()) => {}
            Err(e) => self.desync(&path, Some(&id), &e),
        }
    }

    fn edit_inner(&mut self, id: &str, m: &Value) -> Result<(), String> {
        let seq = m
            .get("seq")
            .and_then(|x| x.as_u64())
            .ok_or("seq must be an integer")?;
        let s = m
            .get("seen")
            .and_then(|x| x.as_u64())
            .ok_or("seen must be an integer")?;
        let mut op = ot::op_from_json(m.get("op").ok_or("missing op")?)?;
        let now = self.now;
        let sanitize = self.sanitize_astral;
        let d = self.docs.get_mut(id).unwrap();
        if seq != d.seen + 1 {
            return Err(format!(
                "sequence gap: expected {}, got {}",
                d.seen + 1,
                seq
            ));
        }
        if s > d.sent {
            return Err(format!("seen {s} out of range (sent {})", d.sent));
        }
        let mut pending: Vec<(u64, Op)> =
            d.pending.iter().filter(|(q, _)| *q > s).cloned().collect();
        for p in pending.iter_mut() {
            let (p2, op2) = ot::transform(&p.1, &op)?; // the bridge's edit first
            p.1 = p2;
            op = op2;
        }
        let mut text = ot::apply(&d.text, &op)?;
        let mut full = op.clone();
        let mut correction = None;
        if let Some(c) = ot::astral_fix(&text).filter(|_| sanitize) {
            text = ot::apply(&text, &c)?;
            full = ot::compose(&full, &c)?;
            correction = Some(c);
        }
        d.pending = pending;
        d.seen = seq;
        d.text = text;
        d.buffer = Some(match d.buffer.take() {
            Some(b) => ot::compose(&b, &full)?,
            None => full,
        });
        d.ack_due = Some(now + ACK_DELAY);
        if let Some(c) = correction {
            self.send_remote(id, c);
        }
        self.flush_doc(id);
        Ok(())
    }

    /// Send an edit (already applied to `d.text`) to Emacs.
    fn send_remote(&mut self, id: &str, op: Op) {
        let d = self.docs.get_mut(id).unwrap();
        d.sent += 1;
        d.pending.push((d.sent, op.clone()));
        d.ack_due = None;
        let m =
            json!({"t":"remote","path":d.path,"seq":d.sent,"seen":d.seen,"op":ot::op_to_json(&op)});
        self.emit(m);
    }

    fn on_cursor(&mut self, m: &Value) {
        let path = m.get("path").and_then(|x| x.as_str()).unwrap_or("");
        let Some(id) = self.doc_by_path(path) else {
            return;
        };
        let Some(pos) = m.get("pos").and_then(|x| x.as_u64()) else {
            return;
        };
        let d = &self.docs[&id];
        if !d.joined || self.offline {
            return;
        }
        let pos = (pos as usize).min(d.text.len());
        let (mut row, mut col) = (0usize, 0usize);
        for c in &d.text[..pos] {
            if *c == '\n' {
                row += 1;
                col = 0;
            } else {
                col += c.len_utf16();
            }
        }
        self.cmd(NetCmd::Cursor {
            doc_id: id,
            row,
            column: col,
        });
    }

    // ---- Overleaf-side OT -------------------------------------------------
    fn flush_doc(&mut self, id: &str) {
        let offline = self.offline;
        let now = self.now;
        let Some(d) = self.docs.get_mut(id) else {
            return;
        };
        if offline || !d.joined || d.inflight.is_some() || d.rejoining || d.hard_resync {
            return;
        }
        let Some(buf) = d.buffer.take() else { return };
        if ot::is_noop(&buf) {
            return;
        }
        match wire_op(&d.kind, &buf, &d.base) {
            Ok(Some(w)) => {
                d.inflight = Some(buf);
                d.inflight_v = d.version;
                d.sent_at = now;
                let c = NetCmd::Update {
                    doc_id: id.to_string(),
                    v: d.version,
                    op: w,
                    dup_if_source: vec![],
                };
                self.cmd(c);
            }
            Ok(None) => {}
            Err(e) => {
                log(&format!("cannot encode update for {}: {e}", d.path));
                d.buffer = Some(buf);
            }
        }
    }

    fn apply_update(&mut self, id: &str, u: WireUpdate) {
        let Some(d) = self.docs.get_mut(id) else {
            return;
        };
        let own = u
            .source
            .as_ref()
            .map(|s| self.own_ids.contains(s))
            .unwrap_or(false);
        let history = d.kind == DocType::History;
        if history {
            // history-ot updates carry the version they were *submitted* at (not
            // where they were applied when the server rebased them), so `v` is
            // useless for ordering; the websocket delivers them in applied order.
            if u.op.is_none() || own {
                if d.inflight.is_some() && u.v >= d.inflight_v {
                    self.ack_inflight(id);
                    self.flush_doc(id);
                }
                return;
            }
        } else {
            if u.v < d.version {
                return; // already applied (duplicate)
            }
            if u.v > d.version {
                d.ooo.insert(u.v, u);
                d.ooo_since.get_or_insert(self.now);
                return;
            }
            if d.inflight.is_some() && (u.op.is_none() || own) {
                self.ack_inflight(id);
                self.after_applied(id);
                return;
            }
            if u.op.is_none() {
                return; // stray ack
            }
        }
        let w = u.op.as_ref().unwrap();
        let r = decode_wire(&d.kind, w, &d.base).and_then(|s| self.integrate(id, s));
        if let Err(e) = r {
            self.resync_error(id, &e);
            return;
        }
        if history {
            self.flush_doc(id);
        } else {
            self.after_applied(id);
        }
    }

    fn ack_inflight(&mut self, id: &str) {
        let d = self.docs.get_mut(id).unwrap();
        let inf = d.inflight.take().unwrap();
        match ot::apply(&d.base, &inf) {
            Ok(b) => d.base = b,
            Err(e) => {
                let p = d.path.clone();
                self.error(
                    "internal",
                    &format!("ack apply failed: {e}"),
                    Some(&p),
                    false,
                );
                return;
            }
        }
        self.docs.get_mut(id).unwrap().version += 1;
    }

    /// Drain buffered out-of-order updates that became contiguous, then flush.
    fn after_applied(&mut self, id: &str) {
        let d = self.docs.get_mut(id).unwrap();
        let v = d.version;
        if let Some(n) = d.ooo.remove(&v) {
            if d.ooo.is_empty() {
                d.ooo_since = None;
            }
            self.apply_update(id, n);
            return;
        }
        if d.ooo.is_empty() {
            d.ooo_since = None;
        }
        self.flush_doc(id);
    }

    /// Integrate a server op `s` (over `base`).
    fn integrate(&mut self, id: &str, s: Op) -> Result<(), String> {
        let d = self.docs.get_mut(id).unwrap();
        let mut s1 = s.clone();
        if let Some(inf) = d.inflight.take() {
            let (inf2, s2) = ot::transform(&inf, &s1)?; // our op first on ties
            d.inflight = Some(inf2);
            s1 = s2;
        }
        if let Some(buf) = d.buffer.take() {
            let (b2, s2) = ot::transform(&buf, &s1)?;
            d.buffer = Some(b2);
            s1 = s2;
        }
        d.base = ot::apply(&d.base, &s)?;
        d.version += 1;
        d.text = ot::apply(&d.text, &s1)?;
        if ot::is_noop(&s1) {
            return Ok(());
        }
        if d.attached && !d.broken {
            self.send_remote(id, s1);
        } else if !d.attached {
            d.dirty = true;
        }
        Ok(())
    }

    fn resync_error(&mut self, id: &str, why: &str) {
        let path = self.docs[id].path.clone();
        log(&format!("OT failure on {path}: {why}; rejoining"));
        let d = self.docs.get_mut(id).unwrap();
        d.hard_resync = true;
        d.inflight = None;
        d.buffer = None;
        d.rejoining = true;
        self.cmd(NetCmd::JoinDoc {
            doc_id: id.to_string(),
            from_version: None,
        });
    }

    fn auth_fields(&self) -> Value {
        if self.cookie_auth { json!(["cookie"]) } else { json!(["email", "password"]) }
    }

    // ---- network events ---------------------------------------------------
    pub fn handle_net(&mut self, ev: NetEvent) {
        self.now = Instant::now();
        match ev {
            NetEvent::Log(s) => log(&s),
            NetEvent::NeedAuth { reason } => {
                self.phase = Phase::Idle;
                self.emit(json!({"t":"need_auth","fields":self.auth_fields(),"reason":reason}));
            }
            NetEvent::Fatal { message } => self.error("open_failed", &message, None, true),
            NetEvent::Connected {
                public_id,
                project,
                user_name,
                user_id,
            } => self.on_connected(public_id, project, user_name, user_id),
            NetEvent::Disconnected { reason, retry } => self.on_disconnected(&reason, retry),
            NetEvent::DocJoined { doc_id, result } => self.on_doc_joined(&doc_id, result),
            NetEvent::Update { doc_id, update } => self.apply_update(&doc_id, update),
            NetEvent::UpdateError { doc_id, message } => {
                if let Some(d) = self.docs.get_mut(&doc_id) {
                    log(&format!("update error on {}: {message}", d.path));
                    d.hard_resync = true;
                    d.inflight = None;
                    d.buffer = None;
                    // the server disconnects us after an update error
                    self.retry_at = Some(self.now + Duration::from_millis(1500));
                }
            }
            NetEvent::Peer(p) => self.on_peer(p),
            NetEvent::PeerLeft { id } => self.emit(json!({"t":"peer_left","id":id})),
            NetEvent::Tree(t) => self.on_tree(t),
            NetEvent::FileData { file_id, result } => self.on_file_data(&file_id, result),
            NetEvent::RestDone { tag, result } => self.on_rest_done(tag, result),
        }
        self.flush_all_inner();
    }

    fn on_connected(
        &mut self,
        public_id: String,
        project: ProjectInfo,
        user_name: String,
        user_id: String,
    ) {
        self.public_id = public_id.clone();
        if !self.own_ids.contains(&public_id) {
            self.own_ids.push(public_id);
        }
        self.user_name = user_name;
        self.user_id = user_id;
        self.attempts = 0;
        self.retry_at = None;
        if self.phase == Phase::Ready {
            // reconnection
            self.offline = false;
            self.status("resyncing", None);
            self.reconcile_tree(&project.root);
            self.rejoin_wait.clear();
            let ids: Vec<String> = self.docs.keys().cloned().collect();
            for id in ids {
                let d = self.docs.get_mut(&id).unwrap();
                d.joined = false;
                d.rejoining = true;
                let from = if d.hard_resync { None } else { Some(d.version) };
                self.rejoin_wait.insert(id.clone());
                self.cmd(NetCmd::JoinDoc {
                    doc_id: id,
                    from_version: from,
                });
            }
            if self.rejoin_wait.is_empty() {
                self.status("connected", None);
            }
            return;
        }
        if self.phase != Phase::Opening {
            return;
        }
        self.phase = Phase::Initial;
        self.project_name = project.name.clone();
        self.root_id = project.root.id.clone();
        let root = match self
            .params
            .get("mirror_dir")
            .or_else(|| self.params.get("dir"))
            .and_then(|x| x.as_str())
        {
            Some(d) => PathBuf::from(d),
            None => {
                let host = self
                    .params
                    .get("server")
                    .and_then(|x| x.as_str())
                    .and_then(|s| url::Url::parse(s).ok())
                    .and_then(|u| u.host_str().map(String::from))
                    .unwrap_or_else(|| "overleaf".into());
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home)
                    .join("collab/overleaf")
                    .join(host)
                    .join(project.name.replace('/', "_"))
            }
        };
        if let Err(e) = std::fs::create_dir_all(&root) {
            self.error(
                "open_failed",
                &format!("cannot create mirror {}: {e}", root.display()),
                None,
                true,
            );
            return;
        }
        let root = std::fs::canonicalize(&root).unwrap_or(root);
        self.root = Some(root);
        let tree = project.root.clone();
        self.add_tree(&tree, None, true);
        // folders, then docs and files
        let mut folders: Vec<String> = self
            .ents
            .iter()
            .filter(|(_, e)| e.kind == Kind::Folder)
            .filter_map(|(i, _)| self.path_of(i))
            .filter(|p| !p.is_empty())
            .collect();
        folders.sort();
        for f in folders {
            if let Some(p) = self.full(&f) {
                let _ = std::fs::create_dir_all(p);
            }
        }
        let items: Vec<(String, Kind)> = self
            .ents
            .iter()
            .filter(|(_, e)| e.kind != Kind::Folder)
            .map(|(i, e)| (i.clone(), e.kind))
            .collect();
        for (id, kind) in items {
            self.init_wait.insert(id.clone());
            match kind {
                Kind::Doc => self.cmd(NetCmd::JoinDoc {
                    doc_id: id,
                    from_version: None,
                }),
                _ => self.cmd(NetCmd::FetchFile { file_id: id }),
            }
        }
        self.maybe_ready();
    }

    fn maybe_ready(&mut self) {
        if self.phase != Phase::Initial || !self.init_wait.is_empty() {
            return;
        }
        self.phase = Phase::Ready;
        let files = self.all_paths();
        let main = ["main.tex", "main.typ"]
            .iter()
            .find(|c| files.iter().any(|f| f == *c))
            .map(|s| s.to_string())
            .or_else(|| files.iter().find(|f| f.ends_with(".tex")).cloned());
        let mut project = json!({"name":self.project_name,"root":self.root.as_ref().unwrap().to_string_lossy(),"files":files});
        if let Some(m) = main {
            project["main"] = json!(m);
        }
        let me =
            json!({"id":self.public_id,"name":self.user_name,"color":color_for(&self.public_id)});
        self.emit(json!({"t":"ready","project":project,"self":me}));
        self.status("connected", None);
        let bf = std::mem::take(&mut self.binary_fail);
        for p in bf {
            self.error("file_failed", "could not download", Some(&p), false);
        }
    }

    fn on_disconnected(&mut self, reason: &str, retry: bool) {
        log(&format!("disconnected: {reason}"));
        if self.phase == Phase::Opening || self.phase == Phase::Initial {
            // initial connection failed
            if !retry || self.phase == Phase::Opening {
                self.error(
                    "open_failed",
                    &format!("connection failed: {reason}"),
                    None,
                    true,
                );
                return;
            }
        }
        if !retry {
            self.phase = Phase::Idle;
            self.emit(json!({"t":"need_auth","fields":self.auth_fields(),"reason":reason}));
            return;
        }
        self.offline = true;
        for d in self.docs.values_mut() {
            d.joined = false;
        }
        self.status("offline", Some(reason));
        let backoff = [500u64, 1000, 2000, 4000, 8000, 16000, 30000];
        let ms = backoff[(self.attempts as usize).min(backoff.len() - 1)];
        self.attempts += 1;
        self.retry_at = Some(self.now + Duration::from_millis(ms));
    }

    fn on_doc_joined(&mut self, id: &str, result: Result<DocSnapshot, String>) {
        let known = self.docs.contains_key(id);
        if !known {
            // initial join (or a new doc)
            let Some(path) = self.path_of(id) else { return };
            match result {
                Ok(s) => {
                    let text = s2c(&s.text);
                    let d = Doc {
                        id: id.to_string(),
                        path: path.clone(),
                        kind: s.kind,
                        joined: true,
                        base: text.clone(),
                        version: s.version,
                        inflight: None,
                        inflight_v: 0,
                        sent_at: self.now,
                        buffer: None,
                        text,
                        ooo: BTreeMap::new(),
                        ooo_since: None,
                        hard_resync: false,
                        rejoining: false,
                        attached: false,
                        broken: false,
                        sent: 0,
                        seen: 0,
                        pending: vec![],
                        ack_due: None,
                        dirty: false,
                        pending_text: self.pending_texts.remove(id),
                    };
                    self.docs.insert(id.to_string(), d);
                    self.write_mirror_doc(id);
                    if let Some(t) = self.docs.get_mut(id).unwrap().pending_text.take() {
                        self.local_insert(id, &t);
                    }
                }
                Err(e) => {
                    self.error("join_failed", &e, Some(&path), false);
                }
            }
            if self.init_wait.remove(id) {
                self.maybe_ready();
            } else if self.phase == Phase::Ready {
                self.emit(json!({"t":"file_event","op":"change","path":path}));
            }
            return;
        }
        // rejoin of a known doc
        let snap = match result {
            Ok(s) => s,
            Err(e) => {
                log(&format!("rejoin of {id} failed: {e}"));
                let d = self.docs.get_mut(id).unwrap();
                d.hard_resync = true;
                d.rejoining = false;
                self.rejoin_wait.remove(id);
                self.retry_at = Some(self.now + Duration::from_secs(2));
                return;
            }
        };
        let d = self.docs.get_mut(id).unwrap();
        let mut hard = d.hard_resync;
        if !hard {
            // replay missed updates in order
            let from = d.version;
            d.joined = true;
            d.rejoining = false;
            let history = snap.kind == DocType::History;
            for u in snap.updates.iter().cloned() {
                if history || u.v >= from {
                    self.apply_update(id, u);
                }
            }
            let d = self.docs.get_mut(id).unwrap();
            if d.version != snap.version || !d.ooo.is_empty() {
                hard = true;
            }
        }
        if hard {
            self.hard_resync(id, snap);
        } else {
            let d = self.docs.get_mut(id).unwrap();
            d.joined = true;
            d.rejoining = false;
            // resend an unacknowledged op (it may have been applied already)
            if d.inflight.is_some() {
                self.resend(id);
            }
            self.flush_doc(id);
        }
        self.rejoin_wait.remove(id);
        if self.rejoin_wait.is_empty() && !self.offline && self.phase == Phase::Ready {
            self.status("connected", None);
        }
    }

    fn hard_resync(&mut self, id: &str, snap: DocSnapshot) {
        let text = s2c(&snap.text);
        let d = self.docs.get_mut(id).unwrap();
        d.kind = snap.kind;
        d.base = text.clone();
        d.text = text;
        d.version = snap.version;
        d.inflight = None;
        d.buffer = None;
        d.ooo.clear();
        d.ooo_since = None;
        d.hard_resync = false;
        d.rejoining = false;
        d.joined = true;
        let (path, att) = (d.path.clone(), d.attached);
        if att {
            self.error(
                "resync",
                "document was reloaded from the server; local unsent edits were dropped",
                Some(&path),
                false,
            );
            let d = self.docs.get_mut(id).unwrap();
            d.sent = 0;
            d.seen = 0;
            d.pending.clear();
            d.broken = false;
            d.ack_due = None;
            let t = c2s(&d.text);
            self.emit(json!({"t":"doc","path":path,"text":t,"seen":0}));
        } else {
            self.write_mirror_doc(id);
            self.emit(json!({"t":"file_event","op":"change","path":path}));
        }
    }

    fn resend(&mut self, id: &str) {
        let now = self.now;
        let own = self.own_ids.clone();
        let Some(d) = self.docs.get_mut(id) else {
            return;
        };
        let Some(inf) = &d.inflight else { return };
        match wire_op(&d.kind, inf, &d.base) {
            Ok(Some(w)) => {
                d.sent_at = now;
                let c = NetCmd::Update {
                    doc_id: id.to_string(),
                    v: d.version,
                    op: w,
                    dup_if_source: own,
                };
                self.cmd(c);
            }
            _ => {
                d.sent_at = now;
            }
        }
    }

    /// A local (bridge-originated) insert of `t` at the start of a doc.
    fn local_insert(&mut self, id: &str, t: &str) {
        let d = self.docs.get_mut(id).unwrap();
        let Ok(op) = ot::from_change(d.text.len(), 0, 0, t) else {
            return;
        };
        let Ok(text) = ot::apply(&d.text, &op) else {
            return;
        };
        d.text = text;
        d.buffer = Some(match d.buffer.take() {
            Some(b) => ot::compose(&b, &op).unwrap_or(b),
            None => op,
        });
        d.dirty = true;
        self.flush_doc(id);
        self.write_mirror_doc(id);
    }

    fn on_peer(&mut self, p: Peer) {
        if p.id == self.public_id || self.own_ids.contains(&p.id) {
            return;
        }
        let mut m = json!({"t":"peer","id":p.id,"name":p.name,"color":color_for(&p.id)});
        if let Some(doc_id) = &p.doc_id {
            if let Some(d) = self.docs.get(doc_id) {
                m["path"] = json!(d.path);
                if let (Some(row), Some(col)) = (p.row, p.column) {
                    // row/column -> code point position
                    let mut line = 0usize;
                    let mut i = 0usize;
                    while line < row && i < d.text.len() {
                        if d.text[i] == '\n' {
                            line += 1;
                        }
                        i += 1;
                    }
                    let mut u = 0usize;
                    while i < d.text.len() && d.text[i] != '\n' && u < col {
                        u += d.text[i].len_utf16();
                        i += 1;
                    }
                    m["pos"] = json!(i);
                }
            }
        }
        self.emit(m);
    }

    // ---- tree events ------------------------------------------------------
    fn mirror_mkdir(&mut self, path: &str) {
        if let Some(p) = self.full(path) {
            let _ = std::fs::create_dir_all(p);
        }
    }

    fn add_entity(&mut self, kind: Kind, parent: &str, id: &str, name: &str) -> bool {
        if self.ents.contains_key(id) {
            return false;
        }
        if !self.ents.contains_key(parent) {
            log(&format!("tree event for unknown parent {parent}"));
            return false;
        }
        self.ents.insert(
            id.to_string(),
            Ent {
                kind,
                name: name.to_string(),
                parent: Some(parent.to_string()),
            },
        );
        let Some(path) = self.path_of(id) else {
            return false;
        };
        match kind {
            Kind::Folder => self.mirror_mkdir(&path),
            Kind::Doc => {
                if let Some(f) = self.full(&path) {
                    if !f.exists() {
                        let _ = write_atomic(&f, b"");
                    }
                }
                self.cmd(NetCmd::JoinDoc {
                    doc_id: id.to_string(),
                    from_version: None,
                });
            }
            Kind::File => self.cmd(NetCmd::FetchFile {
                file_id: id.to_string(),
            }),
        }
        self.emit(json!({"t":"file_event","op":"create","path":path}));
        true
    }

    fn remove_entity(&mut self, id: &str) {
        let Some(path) = self.path_of(id) else { return };
        let kind = self.ents[id].kind;
        // remove descendants
        let kids: Vec<String> = self
            .ents
            .iter()
            .filter(|(_, e)| e.parent.as_deref() == Some(id))
            .map(|(i, _)| i.clone())
            .collect();
        for k in kids {
            self.remove_entity_silent(&k);
        }
        self.ents.remove(id);
        self.docs.remove(id);
        if let Some(f) = self.full(&path) {
            let _ = if kind == Kind::Folder {
                std::fs::remove_dir_all(&f)
            } else {
                std::fs::remove_file(&f)
            };
        }
        self.emit(json!({"t":"file_event","op":"delete","path":path}));
    }
    fn remove_entity_silent(&mut self, id: &str) {
        let kids: Vec<String> = self
            .ents
            .iter()
            .filter(|(_, e)| e.parent.as_deref() == Some(id))
            .map(|(i, _)| i.clone())
            .collect();
        for k in kids {
            self.remove_entity_silent(&k);
        }
        self.ents.remove(id);
        self.docs.remove(id);
    }

    fn relocate(&mut self, id: &str, new_parent: Option<&str>, new_name: Option<&str>) {
        if let Some((old, new)) = self.move_quiet(id, new_parent, new_name) {
            self.refresh_paths();
            self.emit(json!({"t":"file_event","op":"rename","path":old,"to":new}));
        }
    }

    fn move_quiet(
        &mut self,
        id: &str,
        new_parent: Option<&str>,
        new_name: Option<&str>,
    ) -> Option<(String, String)> {
        let old = self.path_of(id)?;
        let e = self.ents.get_mut(id)?;
        if let Some(p) = new_parent {
            e.parent = Some(p.to_string());
        }
        if let Some(n) = new_name {
            e.name = n.to_string();
        }
        let new = self.path_of(id)?;
        if old == new {
            return None;
        }
        if let (Some(a), Some(b)) = (self.full(&old), self.full(&new)) {
            if let Some(d) = b.parent() {
                let _ = std::fs::create_dir_all(d);
            }
            let _ = std::fs::rename(a, b);
        }
        Some((old, new))
    }

    fn on_tree(&mut self, t: TreeEvent) {
        if self.phase != Phase::Ready {
            return;
        }
        match t {
            TreeEvent::NewDoc { parent, id, name } => {
                self.add_entity(Kind::Doc, &parent, &id, &name);
            }
            TreeEvent::NewFile { parent, id, name } => {
                self.add_entity(Kind::File, &parent, &id, &name);
            }
            TreeEvent::NewFolder { parent, id, name } => {
                self.add_entity(Kind::Folder, &parent, &id, &name);
            }
            TreeEvent::Rename { id, name } => self.relocate(&id, None, Some(&name)),
            TreeEvent::Move { id, folder } => self.relocate(&id, Some(&folder), None),
            TreeEvent::Remove { id } => self.remove_entity(&id),
        }
    }

    /// After a reconnect: diff the server's tree against ours.
    fn reconcile_tree(&mut self, root: &Folder) {
        let mut fresh = Bridge::new(true);
        fresh.add_tree(root, None, true);
        let gone: Vec<String> = self
            .ents
            .keys()
            .filter(|i| !fresh.ents.contains_key(*i))
            .cloned()
            .collect();
        for g in gone {
            if self.ents.contains_key(&g) {
                self.remove_entity(&g);
            }
        }
        // parents before children
        let mut news: Vec<String> = fresh
            .ents
            .keys()
            .filter(|i| !self.ents.contains_key(*i))
            .cloned()
            .collect();
        news.sort_by_key(|i| {
            fresh
                .path_of(i)
                .map(|p| p.matches('/').count())
                .unwrap_or(0)
        });
        for n in news {
            let e = &fresh.ents[&n];
            if let Some(p) = e.parent.clone() {
                self.add_entity(e.kind, &p, &n, &e.name.clone());
            }
        }
        let moved: Vec<(String, Option<String>, String)> = fresh
            .ents
            .iter()
            .filter(|(i, e)| {
                self.ents
                    .get(*i)
                    .map(|o| o.name != e.name || o.parent != e.parent)
                    .unwrap_or(false)
            })
            .map(|(i, e)| (i.clone(), e.parent.clone(), e.name.clone()))
            .collect();
        for (i, p, n) in moved {
            self.relocate(&i, p.as_deref(), Some(&n));
        }
    }

    fn on_file_data(&mut self, id: &str, result: Result<Vec<u8>, String>) {
        let path = self.path_of(id);
        match (path, result) {
            (Some(p), Ok(bytes)) => {
                if let Some(f) = self.full(&p) {
                    if let Err(e) = write_atomic(&f, &bytes) {
                        self.error("write_failed", &e.to_string(), Some(&p), false);
                    }
                }
            }
            (Some(p), Err(e)) => {
                log(&format!("download of {p} failed: {e}"));
                if self.phase == Phase::Initial {
                    self.binary_fail.push(p);
                } else {
                    self.error("file_failed", &e, Some(&p), false);
                }
            }
            _ => {}
        }
        if self.init_wait.remove(id) {
            self.maybe_ready();
        }
    }

    // ---- file operations from Emacs ---------------------------------------
    fn on_file(&mut self, m: &Value) {
        let op = m.get("op").and_then(|x| x.as_str()).unwrap_or("");
        let path = m
            .get("path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if !self.require_ready() || !valid_path(&path) {
            self.error(
                "file_failed",
                "bad path or project not ready",
                Some(&path),
                false,
            );
            return;
        }
        match op {
            "create" => {
                if self.ent_by_path(&path).is_some() {
                    self.error("file_failed", "already exists", Some(&path), false);
                    return;
                }
                let text = m
                    .get("text")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from);
                self.start_create(&path, text);
            }
            "rename" => {
                let Some(to) = m
                    .get("to")
                    .and_then(|x| x.as_str())
                    .filter(|t| valid_path(t))
                    .map(String::from)
                else {
                    self.error("file_failed", "bad target path", Some(&path), false);
                    return;
                };
                let Some(id) = self.ent_by_path(&path) else {
                    self.error("file_failed", "no such file", Some(&path), false);
                    return;
                };
                if self.ent_by_path(&to).is_some() {
                    self.error("file_failed", "target exists", Some(&path), false);
                    return;
                }
                let kind = self.ents[&id].kind;
                self.run_job(Job::Rename {
                    id,
                    kind,
                    from: path,
                    to,
                    step: 0,
                });
            }
            "delete" => {
                let Some(id) = self.ent_by_path(&path) else {
                    self.error("file_failed", "no such file", Some(&path), false);
                    return;
                };
                let kind = self.ents[&id].kind;
                self.run_job(Job::Delete { id, kind, path });
            }
            other => log(&format!("unknown file op {other}")),
        }
    }

    fn start_create(&mut self, path: &str, text: Option<String>) {
        let parts: Vec<&str> = path.split('/').collect();
        let dirs = &parts[..parts.len() - 1];
        // deepest existing folder
        let mut parent = self.root_id.clone();
        let mut so_far = String::new();
        let mut i = 0;
        while i < dirs.len() {
            let p = if so_far.is_empty() {
                dirs[i].to_string()
            } else {
                format!("{so_far}/{}", dirs[i])
            };
            match self.ent_by_path(&p) {
                Some(id) if self.ents[&id].kind == Kind::Folder => {
                    parent = id;
                    so_far = p;
                    i += 1;
                }
                Some(_) => {
                    self.error("file_failed", "a file is in the way", Some(path), false);
                    return;
                }
                None => break,
            }
        }
        let job = Job::CreateDoc {
            path: path.to_string(),
            text,
        };
        if i < dirs.len() {
            let rest: Vec<String> = dirs[i..].iter().map(|s| s.to_string()).collect();
            self.run_job(Job::Mkdir {
                rest,
                parent,
                path_so_far: so_far,
                then: Box::new(job),
            });
        } else {
            self.run_job_in(job, &parent);
        }
    }

    fn new_tag(&mut self) -> u64 {
        self.next_tag += 1;
        self.next_tag
    }

    fn run_job(&mut self, job: Job) {
        let root = self.root_id.clone();
        self.run_job_in(job, &root)
    }

    fn mkdir_chain(&mut self, dir_path: &str, then: Job) {
        let dirs: Vec<String> = dir_path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let mut par = self.root_id.clone();
        let mut so_far = String::new();
        let mut i = 0;
        while i < dirs.len() {
            let p = if so_far.is_empty() {
                dirs[i].clone()
            } else {
                format!("{so_far}/{}", dirs[i])
            };
            if let Some(f) = self.ent_by_path(&p) {
                par = f;
                so_far = p;
                i += 1;
            } else {
                break;
            }
        }
        let rest = dirs[i..].to_vec();
        if rest.is_empty() {
            self.run_job_in(then, &par);
        } else {
            self.run_job(Job::Mkdir {
                rest,
                parent: par,
                path_so_far: so_far,
                then: Box::new(then),
            });
        }
    }

    /// Issue the REST call for the job's next step; `parent` is the folder a
    /// create step happens in.
    fn run_job_in(&mut self, job: Job, parent: &str) {
        let tag = self.new_tag();
        let req = match &job {
            Job::CreateDoc { path, .. } => RestReq::CreateDoc {
                parent: parent.to_string(),
                name: path.rsplit('/').next().unwrap().to_string(),
            },
            Job::Mkdir { rest, parent, .. } => RestReq::CreateFolder {
                parent: parent.clone(),
                name: rest[0].clone(),
            },
            Job::Delete { id, kind, .. } => RestReq::Delete {
                kind: kind.rest().into(),
                id: id.clone(),
            },
            Job::Rename {
                id,
                kind,
                from,
                to,
                step,
            } => {
                let (fp, tp) = (parent_of(from), parent_of(to));
                if *step == 0 && fp != tp {
                    let dest = if tp.is_empty() {
                        Some(self.root_id.clone())
                    } else {
                        self.ent_by_path(tp)
                    };
                    match dest {
                        Some(dest) => RestReq::Move {
                            kind: kind.rest().into(),
                            id: id.clone(),
                            folder: dest,
                        },
                        None => {
                            let tp = tp.to_string();
                            self.mkdir_chain(&tp, job);
                            return;
                        }
                    }
                } else {
                    RestReq::Rename {
                        kind: kind.rest().into(),
                        id: id.clone(),
                        name: to.rsplit('/').next().unwrap().to_string(),
                    }
                }
            }
        };
        self.jobs.insert(tag, job);
        self.cmd(NetCmd::Rest { tag, req });
    }

    fn on_rest_done(&mut self, tag: u64, result: Result<Value, String>) {
        let Some(job) = self.jobs.remove(&tag) else {
            return;
        };
        let v = match result {
            Ok(v) => v,
            Err(e) => {
                let p = match job {
                    Job::CreateDoc { path, .. } | Job::Delete { path, .. } => path,
                    Job::Rename { from, .. } => from,
                    Job::Mkdir { path_so_far, .. } => path_so_far,
                };
                self.error("file_failed", &e, Some(&p), false);
                return;
            }
        };
        let rid = v
            .get("_id")
            .or_else(|| v.get("id"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        match job {
            Job::CreateDoc { path, text } => {
                let parent = if parent_of(&path).is_empty() {
                    Some(self.root_id.clone())
                } else {
                    self.ent_by_path(parent_of(&path))
                };
                if rid.is_empty() || parent.is_none() {
                    self.error(
                        "file_failed",
                        "server returned no id (or parent vanished)",
                        Some(&path),
                        false,
                    );
                    return;
                }
                let name = path.rsplit('/').next().unwrap().to_string();
                if let Some(t) = text {
                    if self.docs.contains_key(&rid) {
                        self.local_insert(&rid, &t);
                    } else {
                        self.pending_texts.insert(rid.clone(), t.clone());
                    }
                    if let Some(f) = self.full(&path) {
                        let _ = write_atomic(&f, t.as_bytes());
                    }
                }
                self.add_entity(Kind::Doc, &parent.unwrap(), &rid, &name);
            }
            Job::Mkdir {
                mut rest,
                parent,
                path_so_far,
                then,
            } => {
                let name = rest.remove(0);
                if rid.is_empty() {
                    self.error(
                        "file_failed",
                        "server returned no folder id",
                        Some(&path_so_far),
                        false,
                    );
                    return;
                }
                self.add_entity(Kind::Folder, &parent, &rid, &name);
                let so_far = if path_so_far.is_empty() {
                    name
                } else {
                    format!("{path_so_far}/{name}")
                };
                if rest.is_empty() {
                    self.run_job_in(*then, &rid);
                } else {
                    self.run_job(Job::Mkdir {
                        rest,
                        parent: rid,
                        path_so_far: so_far,
                        then,
                    });
                }
            }
            Job::Delete { id, .. } => {
                if self.ents.contains_key(&id) {
                    self.remove_entity(&id);
                }
            }
            Job::Rename {
                id,
                kind,
                from,
                to,
                step,
            } => {
                let (fp, tp) = (parent_of(&from), parent_of(&to));
                let (fname, tname) = (
                    from.rsplit('/').next().unwrap(),
                    to.rsplit('/').next().unwrap(),
                );
                if step == 0 && fp != tp {
                    let dest = if tp.is_empty() {
                        self.root_id.clone()
                    } else {
                        self.ent_by_path(tp).unwrap_or_else(|| self.root_id.clone())
                    };
                    self.move_quiet(&id, Some(&dest), None);
                    if fname != tname {
                        self.run_job(Job::Rename {
                            id,
                            kind,
                            from,
                            to,
                            step: 1,
                        });
                        return;
                    }
                } else {
                    self.move_quiet(&id, None, Some(tname));
                }
                self.refresh_paths();
                self.emit(json!({"t":"file_event","op":"rename","path":from,"to":to}));
            }
        }
    }

    // ---- test hooks -------------------------------------------------------
    fn on_x_test_remote(&mut self, m: &Value) {
        let path = m
            .get("path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let Some(id) = self.doc_by_path(&path) else {
            self.error("test_failed", "no such doc", Some(&path), false);
            return;
        };
        match ot::op_from_json(m.get("op").unwrap_or(&Value::Null)) {
            Ok(op) => self.cmd(NetCmd::TestRemote { doc_id: id, op }),
            Err(e) => self.error("test_failed", &e, Some(&path), false),
        }
    }

    // ---- timers -----------------------------------------------------------
    pub fn tick(&mut self, now: Instant) {
        self.now = now;
        // acks to Emacs
        let due: Vec<String> = self
            .docs
            .values()
            .filter(|d| d.attached && !d.broken && d.ack_due.map(|t| t <= now).unwrap_or(false))
            .map(|d| d.id.clone())
            .collect();
        for id in due {
            let d = self.docs.get_mut(&id).unwrap();
            d.ack_due = None;
            let m = json!({"t":"ack","path":d.path,"seen":d.seen});
            self.emit(m);
        }
        // mirror writes
        self.flush_dirty();
        // resends
        if !self.offline {
            let ids: Vec<String> = self
                .docs
                .values()
                .filter(|d| {
                    d.joined
                        && d.inflight.is_some()
                        && now.duration_since(d.sent_at) >= RESEND_AFTER
                })
                .map(|d| d.id.clone())
                .collect();
            for id in ids {
                log(&format!("resending update for {id}"));
                self.resend(&id);
            }
            let ids: Vec<String> = self
                .docs
                .values()
                .filter(|d| {
                    d.joined
                        && d.ooo_since
                            .map(|t| now.duration_since(t) >= GAP_REJOIN_AFTER)
                            .unwrap_or(false)
                })
                .map(|d| d.id.clone())
                .collect();
            for id in ids {
                let d = self.docs.get_mut(&id).unwrap();
                d.ooo_since = None;
                d.rejoining = true;
                let v = d.version;
                self.cmd(NetCmd::JoinDoc {
                    doc_id: id,
                    from_version: Some(v),
                });
            }
        }
        if let Some(t) = self.retry_at {
            if t <= now && self.phase == Phase::Ready {
                self.retry_at = None;
                if !self.offline {
                    // after an update error the server drops us anyway; go offline ourselves
                    self.offline = true;
                    for d in self.docs.values_mut() {
                        d.joined = false;
                    }
                    self.status("offline", Some("update rejected"));
                }
                self.cmd(NetCmd::Reconnect);
            }
        }
    }
}

fn parent_of(p: &str) -> &str {
    match p.rfind('/') {
        Some(i) => &p[..i],
        None => "",
    }
}

pub fn wire_op(kind: &DocType, op: &Op, base: &[char]) -> Result<Option<Value>, String> {
    match kind {
        DocType::Share => {
            let c = ot::to_sharejs(op, base)?;
            Ok(if c.is_empty() {
                None
            } else {
                Some(Value::Array(c))
            })
        }
        DocType::History => {
            if ot::is_noop(op) {
                Ok(None)
            } else {
                Ok(Some(ot::to_history(op, base)?))
            }
        }
    }
}
pub fn decode_wire(kind: &DocType, v: &Value, base: &[char]) -> Result<Op, String> {
    match kind {
        DocType::Share => ot::from_sharejs(v.as_array().ok_or("op must be a list")?, base),
        DocType::History => ot::from_history(v, base),
    }
}

#[allow(dead_code)]
fn _unused(_: C) {}
