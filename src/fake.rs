//! An in-process fake Overleaf: enough of the server's behaviour (versioned
//! docs, server-side transform of stale updates, tree REST calls, tree
//! broadcasts) to run the conformance suite and unit tests without a network.
use crate::core::{decode_wire, wire_op};
use crate::net::*;
use crate::ot::{self, Op};
use crate::Backend;
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

struct FDoc {
    kind: DocType,
    /// texts[v] is the text at version v.
    texts: Vec<Vec<char>>,
    ops: Vec<Op>,
    sources: Vec<Option<String>>,
}
impl FDoc {
    fn version(&self) -> i64 {
        self.ops.len() as i64
    }
    fn text(&self) -> &Vec<char> {
        self.texts.last().unwrap()
    }
}

struct FEnt {
    kind: u8, // 0 doc, 1 file, 2 folder
    name: String,
    parent: Option<String>,
}

pub struct FakeServer {
    tx: UnboundedSender<NetEvent>,
    docs: HashMap<String, FDoc>,
    ents: HashMap<String, FEnt>,
    files: HashMap<String, Vec<u8>>,
    root: String,
    next: u64,
    history: bool,
    pub project_name: String,
    /// Hold updates instead of acknowledging them (for tests).
    pub hold_acks: bool,
    pub held: Vec<(String, i64, Value, Vec<String>)>,
    connections: u32,
}

pub const SELF_ID: &str = "fake-self";
pub const OTHER_ID: &str = "fake-other";

impl FakeServer {
    pub fn new(tx: UnboundedSender<NetEvent>) -> Self {
        let mut s = FakeServer {
            tx,
            docs: HashMap::new(),
            ents: HashMap::new(),
            files: HashMap::new(),
            root: "root".into(),
            next: 1,
            history: false,
            project_name: "fake-project".into(),
            hold_acks: false,
            held: vec![],
            connections: 0,
        };
        s.ents.insert("root".into(), FEnt { kind: 2, name: String::new(), parent: None });
        s
    }
    fn id(&mut self) -> String {
        self.next += 1;
        format!("{:024x}", self.next)
    }
    fn ev(&self, e: NetEvent) {
        let _ = self.tx.send(e);
    }
    pub fn use_history(&mut self, h: bool) {
        self.history = h;
    }
    pub fn add_doc(&mut self, parent: &str, name: &str, text: &str) -> String {
        let id = self.id();
        self.ents.insert(id.clone(), FEnt { kind: 0, name: name.into(), parent: Some(parent.into()) });
        let t: Vec<char> = text.chars().collect();
        self.docs.insert(id.clone(), FDoc { kind: if self.history { DocType::History } else { DocType::Share }, texts: vec![t], ops: vec![], sources: vec![] });
        id
    }
    pub fn add_folder(&mut self, parent: &str, name: &str) -> String {
        let id = self.id();
        self.ents.insert(id.clone(), FEnt { kind: 2, name: name.into(), parent: Some(parent.into()) });
        id
    }
    /// Seed from a directory.
    pub fn seed_dir(&mut self, dir: &std::path::Path) {
        fn walk(s: &mut FakeServer, dir: &std::path::Path, parent: &str) {
            let mut es: Vec<_> = std::fs::read_dir(dir).map(|r| r.flatten().collect()).unwrap_or_default();
            es.sort_by_key(|e| e.file_name());
            for e in es {
                let name = e.file_name().to_string_lossy().to_string();
                if name.starts_with('.') {
                    continue;
                }
                let p = e.path();
                if p.is_dir() {
                    let id = s.add_folder(parent, &name);
                    walk(s, &p, &id);
                } else if let Ok(b) = std::fs::read(&p) {
                    match String::from_utf8(b.clone()) {
                        Ok(t) if !t.contains('\0') => {
                            s.add_doc(parent, &name, &t.replace("\r\n", "\n"));
                        }
                        _ => {
                            let id = s.id();
                            s.ents.insert(id.clone(), FEnt { kind: 1, name, parent: Some(parent.into()) });
                            s.files.insert(id, b);
                        }
                    }
                }
            }
        }
        let root = self.root.clone();
        walk(self, dir, &root);
    }
    pub fn doc_text(&self, id: &str) -> Option<String> {
        self.docs.get(id).map(|d| d.text().iter().collect())
    }
    pub fn doc_version(&self, id: &str) -> i64 {
        self.docs[id].version()
    }
    pub fn doc_id_by_name(&self, name: &str) -> Option<String> {
        self.ents.iter().find(|(_, e)| e.kind == 0 && e.name == name).map(|(i, _)| i.clone())
    }

    fn folder(&self, id: &str) -> Folder {
        let e = &self.ents[id];
        let mut f = Folder { id: id.into(), name: e.name.clone(), ..Default::default() };
        let mut kids: Vec<_> = self.ents.iter().filter(|(_, k)| k.parent.as_deref() == Some(id)).collect();
        kids.sort_by_key(|(i, _)| i.to_string());
        for (i, k) in kids {
            match k.kind {
                0 => f.docs.push((i.clone(), k.name.clone())),
                1 => f.files.push((i.clone(), k.name.clone())),
                _ => f.folders.push(self.folder(i)),
            }
        }
        f
    }

    fn connected(&mut self) {
        self.connections += 1;
        let project = ProjectInfo { id: "fakeproject".into(), name: self.project_name.clone(), root: self.folder(&self.root.clone()) };
        self.ev(NetEvent::Connected { public_id: format!("{SELF_ID}-{}", self.connections), project, user_name: "Fake Self".into(), user_id: "fakeuser".into() });
    }

    fn wire(&self, id: &str, op: &Op, v: i64) -> Value {
        let d = &self.docs[id];
        wire_op(&d.kind, op, &d.texts[v as usize]).unwrap().unwrap_or(json!([]))
    }

    /// Apply `op` (cp, based on version `v`) as a new version from `source`.
    fn commit(&mut self, id: &str, v: i64, mut op: Op, source: &str) -> Result<i64, String> {
        let d = self.docs.get_mut(id).ok_or("no such doc")?;
        if v < 0 || v > d.version() {
            return Err("bad version".into());
        }
        for i in v as usize..d.ops.len() {
            let (a, _) = ot::transform(&op, &d.ops[i])?; // the new op goes first on ties
            op = a;
        }
        let nt = ot::apply(d.text(), &op)?;
        let at = d.version();
        d.texts.push(nt);
        d.ops.push(op);
        d.sources.push(Some(source.to_string()));
        Ok(at)
    }

    fn broadcast(&self, id: &str, at: i64, source: &str, skip_op: bool) {
        let d = &self.docs[id];
        let op = if skip_op { None } else { Some(self.wire(id, &d.ops[at as usize], at)) };
        // the wire op is relative to texts[at] only for the committed (transformed) op
        self.ev(NetEvent::Update { doc_id: id.into(), update: WireUpdate { v: at, op, source: Some(source.into()) } });
    }

    /// Another client edits `id` (op over the current text).
    pub fn other_edit(&mut self, id: &str, op: Op) -> Result<(), String> {
        let v = self.docs[id].version();
        let at = self.commit(id, v, op, OTHER_ID)?;
        self.broadcast(id, at, OTHER_ID, false);
        Ok(())
    }
    /// Another client edits `id`, based on an older version.
    pub fn other_edit_at(&mut self, id: &str, v: i64, op: Op) -> Result<(), String> {
        let at = self.commit(id, v, op, OTHER_ID)?;
        self.broadcast(id, at, OTHER_ID, false);
        Ok(())
    }

    fn current_source(&self) -> String {
        format!("{SELF_ID}-{}", self.connections)
    }

    pub fn release_held(&mut self) {
        let held = std::mem::take(&mut self.held);
        for (doc_id, v, op, dup) in held {
            self.update(&doc_id, v, op, dup);
        }
    }

    fn update(&mut self, doc_id: &str, v: i64, op: Value, dup: Vec<String>) {
        let Some(d) = self.docs.get(doc_id) else { return };
        if v < 0 || v > d.version() {
            self.ev(NetEvent::UpdateError { doc_id: doc_id.into(), message: "Invalid version".into() });
            return;
        }
        // duplicate detection
        for i in v as usize..d.ops.len() {
            if let Some(s) = &d.sources[i] {
                if dup.contains(s) {
                    // already applied: acknowledge at that version
                    self.ev(NetEvent::Update { doc_id: doc_id.into(), update: WireUpdate { v: i as i64, op: None, source: Some(s.clone()) } });
                    return;
                }
            }
        }
        let cp = match decode_wire(&d.kind, &op, &d.texts[v as usize]) {
            Ok(o) => o,
            Err(e) => {
                self.ev(NetEvent::UpdateError { doc_id: doc_id.into(), message: e });
                return;
            }
        };
        let src = self.current_source();
        let d_is_history = self.docs[doc_id].kind == DocType::History;
        match self.commit(doc_id, v, cp, &src) {
            Ok(at) => {
                // other clients would get the op; we get the ack (no op)
                // history-ot: like the real server, report the submitted version
                let shown = if d_is_history { v } else { at };
                self.ev(NetEvent::Update { doc_id: doc_id.into(), update: WireUpdate { v: shown, op: None, source: Some(src) } });
            }
            Err(e) => self.ev(NetEvent::UpdateError { doc_id: doc_id.into(), message: e }),
        }
    }
}

impl Backend for FakeServer {
    fn send(&mut self, cmd: NetCmd) {
        match cmd {
            NetCmd::Open { params, .. } => {
                if let Some(d) = params.get("mirror_dir").or_else(|| params.get("dir")).and_then(|x| x.as_str()) {
                    self.seed_dir(std::path::Path::new(d));
                }
                if params.get("fake_history").and_then(|x| x.as_bool()).unwrap_or(false) {
                    self.history = true;
                    for d in self.docs.values_mut() {
                        d.kind = DocType::History;
                    }
                }
                self.connected();
            }
            NetCmd::Reconnect => self.connected(),
            NetCmd::DropConnection => self.ev(NetEvent::Disconnected { reason: "test drop".into(), retry: true }),
            NetCmd::JoinDoc { doc_id, from_version } => {
                let Some(d) = self.docs.get(&doc_id) else {
                    self.ev(NetEvent::DocJoined { doc_id, result: Err("no such doc".into()) });
                    return;
                };
                let mut updates = vec![];
                if let Some(f) = from_version {
                    for i in (f.max(0) as usize)..d.ops.len() {
                        let w = wire_op(&d.kind, &d.ops[i], &d.texts[i]).unwrap().unwrap_or(json!([]));
                        updates.push(WireUpdate { v: i as i64, op: Some(w), source: d.sources[i].clone() });
                    }
                }
                let snap = DocSnapshot { text: d.text().iter().collect(), version: d.version(), kind: d.kind.clone(), updates };
                self.ev(NetEvent::DocJoined { doc_id, result: Ok(snap) });
            }
            NetCmd::Update { doc_id, v, op, dup_if_source } => {
                if self.hold_acks {
                    self.held.push((doc_id, v, op, dup_if_source));
                } else {
                    self.update(&doc_id, v, op, dup_if_source);
                }
            }
            NetCmd::TestRemote { doc_id, op } => {
                if let Err(e) = self.other_edit(&doc_id, op) {
                    self.ev(NetEvent::Log(format!("test remote failed: {e}")));
                }
            }
            NetCmd::Cursor { .. } | NetCmd::Close => {}
            NetCmd::FetchFile { file_id } => {
                let r = self.files.get(&file_id).cloned().ok_or_else(|| "no such file".to_string());
                self.ev(NetEvent::FileData { file_id, result: r });
            }
            NetCmd::Rest { tag, req } => {
                let res = self.rest(req);
                self.ev(NetEvent::RestDone { tag, result: res });
            }
        }
    }
}

impl FakeServer {
    fn rest(&mut self, req: RestReq) -> Result<Value, String> {
        let dup = |s: &FakeServer, parent: &str, name: &str| s.ents.values().any(|e| e.parent.as_deref() == Some(parent) && e.name == name);
        match req {
            RestReq::CreateDoc { parent, name } => {
                if dup(self, &parent, &name) {
                    return Err("file already exists".into());
                }
                let id = self.add_doc(&parent, &name, "");
                self.ev(NetEvent::Tree(TreeEvent::NewDoc { parent, id: id.clone(), name: name.clone() }));
                Ok(json!({"_id":id,"name":name}))
            }
            RestReq::CreateFolder { parent, name } => {
                if dup(self, &parent, &name) {
                    return Err("folder already exists".into());
                }
                let id = self.add_folder(&parent, &name);
                self.ev(NetEvent::Tree(TreeEvent::NewFolder { parent, id: id.clone(), name: name.clone() }));
                Ok(json!({"_id":id,"name":name}))
            }
            RestReq::Rename { id, name, .. } => {
                let e = self.ents.get_mut(&id).ok_or("no such entity")?;
                e.name = name.clone();
                self.ev(NetEvent::Tree(TreeEvent::Rename { id, name }));
                Ok(json!({}))
            }
            RestReq::Move { id, folder, .. } => {
                let e = self.ents.get_mut(&id).ok_or("no such entity")?;
                e.parent = Some(folder.clone());
                self.ev(NetEvent::Tree(TreeEvent::Move { id, folder }));
                Ok(json!({}))
            }
            RestReq::Delete { id, .. } => {
                let mut stack = vec![id.clone()];
                while let Some(i) = stack.pop() {
                    stack.extend(self.ents.iter().filter(|(_, e)| e.parent.as_deref() == Some(&i)).map(|(k, _)| k.clone()));
                    self.ents.remove(&i);
                    self.docs.remove(&i);
                    self.files.remove(&i);
                }
                self.ev(NetEvent::Tree(TreeEvent::Remove { id }));
                Ok(json!({}))
            }
        }
    }
}
