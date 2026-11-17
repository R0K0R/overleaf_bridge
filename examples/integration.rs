//! Integration test against a real Overleaf CE: plays "Emacs" against the
//! bridge binary and "another browser" with a raw socket.io client.
//!
//!   OLB_INTEGRATION=1 cargo run --example integration -- [path/to/overleaf_bridge]
//!
//! Credentials come from .test-credentials (JSON: server,email,password).
//! Only the dedicated test account's own throwaway project is touched.
use overleaf_bridge::ot::{self, Op};
use overleaf_bridge::rawclient::{Raw, RawDoc, Sock};
use rand::{Rng, SeedableRng};
use reqwest::cookie::CookieStore;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

struct EDoc {
    text: Vec<char>,
    sent: u64,
    seen: u64,
    pending: Vec<(u64, Op)>,
}

struct Emacs {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    rx: mpsc::UnboundedReceiver<Value>,
    docs: HashMap<String, EDoc>,
    log: Vec<Value>,
}

impl Emacs {
    async fn start(bin: &str, extra: &[&str]) -> Emacs {
        let mut child = tokio::process::Command::new(bin).args(extra).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().expect("spawn bridge");
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut l = BufReader::new(out).lines();
            while let Ok(Some(line)) = l.next_line().await {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    let _ = tx.send(v);
                }
            }
        });
        Emacs { child, stdin, rx, docs: HashMap::new(), log: vec![] }
    }
    async fn send(&mut self, v: Value) {
        let mut s = v.to_string();
        s.push('\n');
        self.stdin.write_all(s.as_bytes()).await.unwrap();
        self.stdin.flush().await.unwrap();
    }
    fn process(&mut self, m: &Value) {
        let path = m["path"].as_str().unwrap_or("").to_string();
        match m["t"].as_str() {
            Some("remote") => {
                let d = self.docs.get_mut(&path).expect("remote for unattached doc");
                assert_eq!(m["seq"].as_u64().unwrap(), d.seen + 1, "remote seq gap");
                let s = m["seen"].as_u64().unwrap();
                d.pending.retain(|(q, _)| *q > s);
                let mut op = ot::op_from_json(&m["op"]).unwrap();
                for p in d.pending.iter_mut() {
                    let (o2, p2) = ot::transform(&op, &p.1).unwrap();
                    op = o2;
                    p.1 = p2;
                }
                d.text = ot::apply(&d.text, &op).expect("remote apply");
                d.seen = m["seq"].as_u64().unwrap();
            }
            Some("ack") => {
                if let Some(d) = self.docs.get_mut(&path) {
                    let s = m["seen"].as_u64().unwrap();
                    d.pending.retain(|(q, _)| *q > s);
                }
            }
            _ => {}
        }
    }
    /// Read messages (processing remotes/acks) until `pred` matches.
    async fn wait(&mut self, what: &str, secs: u64, pred: impl Fn(&Value) -> bool) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let m = match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => panic!("bridge closed while waiting for {what}"),
                Err(_) => panic!("timeout waiting for {what}; recent: {:?}", self.log.iter().rev().take(5).collect::<Vec<_>>()),
            };
            self.process(&m);
            if pred(&m) {
                return m;
            }
            self.log.push(m);
        }
    }
    /// Process whatever arrives within `ms`.
    async fn pump(&mut self, ms: u64) {
        let end = tokio::time::Instant::now() + Duration::from_millis(ms);
        while let Ok(Some(m)) = tokio::time::timeout_at(end, self.rx.recv()).await {
            self.process(&m);
            self.log.push(m);
        }
    }
    async fn attach(&mut self, path: &str) -> String {
        self.send(json!({"t":"attach","path":path})).await;
        let m = self.wait("doc", 20, |m| m["t"] == "doc" && m["path"] == path).await;
        let text = m["text"].as_str().unwrap().to_string();
        self.docs.insert(path.into(), EDoc { text: text.chars().collect(), sent: 0, seen: 0, pending: vec![] });
        text
    }
    async fn edit(&mut self, path: &str, op: Op) {
        let d = self.docs.get_mut(path).unwrap();
        d.text = ot::apply(&d.text, &op).unwrap();
        d.sent += 1;
        d.pending.push((d.sent, op.clone()));
        let m = json!({"t":"edit","path":path,"seq":d.sent,"seen":d.seen,"op":ot::op_to_json(&op)});
        self.send(m).await;
    }
    fn text(&self, path: &str) -> String {
        self.docs[path].text.iter().collect()
    }
    async fn bridge_text(&mut self, path: &str) -> String {
        self.send(json!({"t":"x-test-text","path":path})).await;
        let p = path.to_string();
        self.wait("x-test-text", 20, move |m| m["t"] == "x-test-text" && m["path"] == p.as_str()).await["text"].as_str().unwrap().to_string()
    }
}

fn rand_op(rng: &mut impl Rng, len: usize) -> Op {
    const A: &[&str] = &["a", "b", "xy", "é", "漢", " ", "\n", "Q"];
    let pos = rng.gen_range(0..=len);
    let del = if len - pos > 0 && rng.gen_bool(0.35) { rng.gen_range(1..=(len - pos).min(3)) } else { 0 };
    let ins = if del == 0 || rng.gen_bool(0.5) { A[rng.gen_range(0..A.len())] } else { "" };
    ot::from_change(len, pos, del, ins).unwrap()
}

macro_rules! check {
    ($c:expr, $($a:tt)*) => { if !$c { println!("FAIL: {}", format!($($a)*)); std::process::exit(1); } else { println!("ok   : {}", format!($($a)*)); } };
}

async fn pump_raw(sock: &mut Sock, doc: &mut RawDoc, ms: u64, tree: &mut Vec<(String, Vec<Value>)>, cursors: &mut Vec<Value>) {
    let end = tokio::time::Instant::now() + Duration::from_millis(ms);
    while let Ok(Some((name, args))) = tokio::time::timeout_at(end, sock.events.recv()).await {
        if std::env::var("OLB_DEBUG").is_ok() { eprintln!("RAW EVENT {name} {}", serde_json::to_string(&args).unwrap().chars().take(300).collect::<String>()); }
        match name.as_str() {
            "otUpdateApplied" => {
                if args[0]["doc"] == doc.id.as_str() {
                    doc.on_update(&args[0], &sock.public_id).expect("raw doc update");
                }
            }
            "clientTracking.clientUpdated" => cursors.push(args[0].clone()),
            n if n.starts_with("recive") || n == "removeEntity" => tree.push((name, args)),
            _ => {}
        }
    }
}

async fn send_raw(sock: &mut Sock, doc: &mut RawDoc, op: Op) {
    let payload = doc.submit(op).unwrap();
    let id = doc.id.clone();
    sock.call("applyOtUpdate", vec![json!(id), payload]).await.expect("applyOtUpdate");
}

#[tokio::main]
async fn main() {
    if std::env::var("OLB_INTEGRATION").is_err() {
        println!("set OLB_INTEGRATION=1 to run (needs the CE test account in .test-credentials)");
        return;
    }
    let bin = std::env::args().nth(1).unwrap_or_else(|| "target/debug/overleaf_bridge".into());
    let creds: Value = serde_json::from_str(&std::fs::read_to_string(".test-credentials").expect(".test-credentials")).unwrap();
    let (server, email, password) = (creds["server"].as_str().unwrap(), creds["email"].as_str().unwrap(), creds["password"].as_str().unwrap());
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let t0 = Instant::now();

    // ---- setup: project owned by the test account ----
    let raw = Raw::login(server, email, password).await.expect("raw login");
    let pname = format!("olb-it-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    let pid = raw.create_project(&pname).await.expect("create project");
    println!("project {pname} ({pid})");
    let history = std::env::var("OLB_HISTORY").is_ok();
    if history {
        // Migrate only this throwaway project to history-ot (nothing has loaded its docs yet).
        let cmd = format!("docker exec mongo mongosh --quiet sharelatex --eval \"db.projects.updateOne({{_id: ObjectId('{pid}')}}, {{\\$max: {{'overleaf.history.otMigrationStage': 1}}}})\"");
        let out = std::process::Command::new("ssh").args(["yulee", &format!("bash -c '{}'", cmd.replace('\'', "'\\''"))]).output().expect("ssh");
        println!("history-ot migration of the test project: {}", String::from_utf8_lossy(&out.stdout).trim().replace('\n', " "));
    }
    let mut sock = raw.connect(&pid).await.expect("raw socket");
    let root = &sock.project["rootFolder"][0];
    let root_id = root["_id"].as_str().unwrap().to_string();
    let main_id = root["docs"].as_array().unwrap().iter().find(|d| d["name"] == "main.tex").expect("main.tex")["_id"].as_str().unwrap().to_string();
    let (t, v, ty) = sock.join_doc(&main_id, true).await.expect("raw joinDoc");
    println!("main.tex type={ty} version={v} len={}", t.chars().count());
    let mut rawdoc = RawDoc::new(&main_id, &t, v);
    rawdoc.history = ty == "history-ot";
    if history { check!(rawdoc.history, "project docs are history-ot"); }
    let mut tree: Vec<(String, Vec<Value>)> = vec![];
    let mut cursors: Vec<Value> = vec![];

    // ---- bridge ----
    let mirror = std::env::temp_dir().join(format!("olb-mirror-{pid}"));
    let mut em = Emacs::start(&bin, &["--hooks"]).await;
    em.send(json!({"t":"hello","client":"olb-integration","version":0})).await;
    em.wait("hello", 10, |m| m["t"] == "hello").await;
    // bad credentials -> need_auth
    em.send(json!({"t":"open","params":{"server":server,"project_id":pid,"mirror_dir":mirror},"credentials":{"method":"password","email":email,"password":"wrong"}})).await;
    let na = em.wait("need_auth", 30, |m| m["t"] == "need_auth" || m["t"] == "error").await;
    check!(na["t"] == "need_auth", "wrong password gives need_auth ({})", na["reason"]);
    em.send(json!({"t":"open","params":{"server":server,"project_id":pid,"mirror_dir":mirror},"credentials":{"method":"password","email":email,"password":password}})).await;
    let ready = em.wait("ready", 60, |m| m["t"] == "ready" || m["t"] == "error").await;
    check!(ready["t"] == "ready", "ready: {}", ready);
    let files: Vec<String> = ready["project"]["files"].as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_string()).collect();
    println!("files: {files:?}");
    check!(files.iter().any(|f| f == "main.tex"), "main.tex mirrored");
    let on_disk = std::fs::read_to_string(mirror.join("main.tex")).unwrap();
    check!(on_disk == t, "mirror main.tex == server text ({} bytes)", on_disk.len());
    for f in &files {
        check!(mirror.join(f).is_file(), "mirror has {f}");
    }

    // ---- S4: concurrent edits ----
    let text = em.attach("main.tex").await;
    check!(text == t, "attach text == server text");
    let (mut n_em, mut n_raw) = (0, 0);
    for round in 0..14 {
        for _ in 0..rng.gen_range(1..=3) {
            let len = em.docs["main.tex"].text.len();
            let op = rand_op(&mut rng, len);
            em.edit("main.tex", op).await;
            n_em += 1;
        }
        if rawdoc.inflight.is_none() {
            let op = rand_op(&mut rng, rawdoc.text.len());
            send_raw(&mut sock, &mut rawdoc, op).await;
            n_raw += 1;
        }
        let _ = round;
        pump_raw(&mut sock, &mut rawdoc, 120, &mut tree, &mut cursors).await;
        em.pump(10).await;
    }
    // settle
    for _ in 0..40 {
        pump_raw(&mut sock, &mut rawdoc, 250, &mut tree, &mut cursors).await;
        em.pump(50).await;
        if rawdoc.inflight.is_none() && em.docs["main.tex"].pending.is_empty() && em.text("main.tex") == rawdoc.text.iter().collect::<String>() {
            break;
        }
    }
    let mut s2 = raw.connect(&pid).await.unwrap();
    let (server_text, sv, _) = s2.join_doc(&main_id, true).await.unwrap();
    s2.close();
    let bt = em.bridge_text("main.tex").await;
    println!("edits: emacs={n_em} browser={n_raw} server version={sv}");
    check!(em.text("main.tex") == server_text, "S4 emacs text == fresh joinDoc text ({} chars)", server_text.chars().count());
    check!(bt == server_text, "S4 bridge text == server text");
    check!(rawdoc.text.iter().collect::<String>() == server_text, "S4 browser text == server text");
    check!(em.docs["main.tex"].pending.is_empty(), "S4 bridge acked all emacs edits (ack msgs)");

    // ---- S5: unicode ----
    let len = em.docs["main.tex"].text.len();
    em.edit("main.tex", ot::from_change(len, 0, 0, "é漢 ").unwrap()).await;
    em.pump(1500).await;
    pump_raw(&mut sock, &mut rawdoc, 500, &mut tree, &mut cursors).await;
    let mut s2 = raw.connect(&pid).await.unwrap();
    let (st, _, _) = s2.join_doc(&main_id, true).await.unwrap();
    s2.close();
    check!(st.starts_with("é漢 "), "S5 BMP non-ASCII inserted at the start; server has {:?}", &st.chars().take(6).collect::<String>());
    check!(rawdoc.text.iter().collect::<String>() == st, "S5 browser sees it");
    // an astral char: Overleaf replaces surrogates; the bridge sanitises and tells Emacs
    em.edit("main.tex", ot::from_change(em.docs["main.tex"].text.len(), 0, 0, "😀").unwrap()).await;
    em.pump(2000).await;
    pump_raw(&mut sock, &mut rawdoc, 500, &mut tree, &mut cursors).await;
    let mut s2 = raw.connect(&pid).await.unwrap();
    let (st2, _, _) = s2.join_doc(&main_id, true).await.unwrap();
    s2.close();
    println!("astral: server starts with {:?}, emacs starts with {:?}", st2.chars().take(4).collect::<String>(), em.text("main.tex").chars().take(4).collect::<String>());
    check!(em.text("main.tex") == st2, "S5 emacs == server after an astral insert");
    check!(rawdoc.text.iter().collect::<String>() == st2, "S5 browser == server after astral insert");

    // ---- S6: cursors ----
    sock.emit("clientTracking.updatePosition", vec![json!({"row": 1, "column": 2, "doc_id": main_id})]);
    let peer = em.wait("peer", 15, |m| m["t"] == "peer").await;
    check!(peer["path"] == "main.tex" && peer["pos"].is_number(), "S6 peer from browser: {}", peer);
    em.send(json!({"t":"cursor","path":"main.tex","pos":5})).await;
    let mut got = None;
    for _ in 0..30 {
        pump_raw(&mut sock, &mut rawdoc, 200, &mut tree, &mut cursors).await;
        em.pump(10).await;
        if let Some(c) = cursors.iter().find(|c| c["id"] != sock.public_id.as_str() && c["doc_id"] == main_id.as_str()) {
            got = Some(c.clone());
            break;
        }
    }
    check!(got.is_some(), "S6 browser got the bridge's cursor: {:?}", got);

    // ---- S7: tree ----
    let r = raw.post(&format!("/project/{pid}/doc"), json!({"name":"it-new.tex","parent_folder_id":root_id})).await.expect("create doc");
    let nid = r["_id"].as_str().unwrap().to_string();
    let m = em.wait("file_event create", 20, |m| m["t"] == "file_event" && m["op"] == "create" && m["path"] == "it-new.tex").await;
    let _ = m;
    check!(mirror.join("it-new.tex").is_file(), "S7 browser-created doc is mirrored");
    raw.post(&format!("/project/{pid}/doc/{nid}/rename"), json!({"name":"it-renamed.tex"})).await.unwrap();
    em.wait("file_event rename", 20, |m| m["t"] == "file_event" && m["op"] == "rename" && m["to"] == "it-renamed.tex").await;
    check!(mirror.join("it-renamed.tex").is_file() && !mirror.join("it-new.tex").exists(), "S7 rename mirrored");
    raw.delete(&format!("/project/{pid}/doc/{nid}")).await.unwrap();
    em.wait("file_event delete", 20, |m| m["t"] == "file_event" && m["op"] == "delete" && m["path"] == "it-renamed.tex").await;
    check!(!mirror.join("it-renamed.tex").exists(), "S7 delete mirrored");
    // Emacs -> server
    em.send(json!({"t":"file","op":"create","path":"sub/dir/x.tex","text":"hello\n"})).await;
    em.wait("create event", 30, |m| m["t"] == "file_event" && m["op"] == "create" && m["path"] == "sub/dir/x.tex").await;
    em.pump(1500).await;
    let proj = {
        let s3 = raw.connect(&pid).await.unwrap();
        let p = s3.project.clone();
        s3.close();
        p
    };
    let find = |f: &Value, name: &str| -> Option<Value> { f["folders"].as_array()?.iter().find(|x| x["name"] == name).cloned() };
    let sub = find(&proj["rootFolder"][0], "sub").expect("sub folder on server");
    let dir = find(&sub, "dir").expect("dir folder on server");
    let xdoc = dir["docs"].as_array().unwrap().iter().find(|d| d["name"] == "x.tex").expect("x.tex on server")["_id"].as_str().unwrap().to_string();
    let mut s3 = raw.connect(&pid).await.unwrap();
    let (xt, _, _) = s3.join_doc(&xdoc, true).await.unwrap();
    s3.close();
    check!(xt == "hello\n", "S7 Emacs-created file on server with its text: {:?}", xt);
    em.send(json!({"t":"file","op":"rename","path":"sub/dir/x.tex","to":"sub/y.tex"})).await;
    em.wait("rename event", 30, |m| m["t"] == "file_event" && m["op"] == "rename" && m["to"] == "sub/y.tex").await;
    check!(mirror.join("sub/y.tex").is_file(), "S7 Emacs rename+move done");
    em.send(json!({"t":"file","op":"delete","path":"sub/y.tex"})).await;
    em.wait("delete event", 30, |m| m["t"] == "file_event" && m["op"] == "delete" && m["path"] == "sub/y.tex").await;
    check!(!mirror.join("sub/y.tex").exists(), "S7 Emacs delete done");
    pump_raw(&mut sock, &mut rawdoc, 500, &mut tree, &mut cursors).await;
    // binary file upload by the browser -> the bridge downloads it
    let png: Vec<u8> = (0..300u32).map(|i| (i * 7 % 256) as u8).collect();
    {
        let part = reqwest::multipart::Part::bytes(png.clone()).file_name("pic.png").mime_str("image/png").unwrap();
        let form = reqwest::multipart::Form::new().text("relativePath", "null").text("name", "pic.png").text("type", "image/png").part("qqfile", part);
        let r = raw.http.post(raw.base.join(&format!("/project/{pid}/upload?folder_id={root_id}")).unwrap()).header("X-Csrf-Token", &raw.csrf).multipart(form).send().await.unwrap();
        check!(r.status().is_success(), "S7 upload of pic.png: HTTP {}", r.status());
    }
    em.wait("file_event pic.png", 20, |m| m["t"] == "file_event" && m["op"] == "create" && m["path"] == "pic.png").await;
    let mut ok = false;
    for _ in 0..40 {
        if std::fs::read(mirror.join("pic.png")).map(|b| b == png).unwrap_or(false) { ok = true; break; }
        em.pump(100).await;
    }
    check!(ok, "S7 binary file downloaded into the mirror byte for byte ({} bytes)", png.len());
    check!(tree.iter().any(|(n, _)| n == "reciveNewDoc") && tree.iter().any(|(n, _)| n == "removeEntity"), "S7 browser saw the tree broadcasts ({} events)", tree.len());

    // ---- S8: reconnect ----
    em.send(json!({"t":"x-test-drop"})).await;
    em.wait("offline", 20, |m| m["t"] == "status" && m["state"] == "offline").await;
    let len = em.docs["main.tex"].text.len();
    em.edit("main.tex", ot::from_change(len, len, 0, "\n% offline edit from emacs\n").unwrap()).await;
    if rawdoc.inflight.is_none() {
        let op = ot::from_change(rawdoc.text.len(), 0, 0, "% browser edit during outage\n").unwrap();
        send_raw(&mut sock, &mut rawdoc, op).await;
    }
    em.wait("connected again", 60, |m| m["t"] == "status" && m["state"] == "connected").await;
    for _ in 0..40 {
        pump_raw(&mut sock, &mut rawdoc, 250, &mut tree, &mut cursors).await;
        em.pump(50).await;
        if rawdoc.inflight.is_none() && em.docs["main.tex"].pending.is_empty() && em.text("main.tex") == rawdoc.text.iter().collect::<String>() {
            break;
        }
    }
    let mut s2 = raw.connect(&pid).await.unwrap();
    let (st3, _, _) = s2.join_doc(&main_id, true).await.unwrap();
    s2.close();
    check!(em.text("main.tex") == st3 && rawdoc.text.iter().collect::<String>() == st3, "S8 converged after reconnect");
    check!(st3.contains("offline edit from emacs") && st3.contains("browser edit during outage"), "S8 both outage edits survived");
    let dups = st3.matches("offline edit from emacs").count();
    check!(dups == 1, "S8 the queued edit was applied exactly once ({dups})");

    // ---- S8b: drop right after edits, so an op may be inflight ----
    for i in 0..3 {
        let len = em.docs["main.tex"].text.len();
        em.edit("main.tex", ot::from_change(len, len, 0, &format!("\n%%MARK{i}%%")).unwrap()).await;
        em.send(json!({"t":"x-test-drop"})).await;
        em.wait("offline", 20, |m| m["t"] == "status" && m["state"] == "offline").await;
        em.wait("connected again", 60, |m| m["t"] == "status" && m["state"] == "connected").await;
    }
    for _ in 0..40 {
        em.pump(250).await;
        if em.docs["main.tex"].pending.is_empty() { break; }
    }
    em.pump(500).await;
    let mut s2 = raw.connect(&pid).await.unwrap();
    let (st4, sv4, _) = s2.join_doc(&main_id, true).await.unwrap();
    s2.close();
    check!(em.text("main.tex") == st4, "S8b emacs == server after 3 drops with edits in flight (server v{sv4})");
    for i in 0..3 {
        let n = st4.matches(&format!("%%MARK{i}%%")).count();
        check!(n == 1, "S8b MARK{i} applied exactly once ({n})");
    }
    let st3 = st4;

    // ---- cookie login (second bridge) ----
    {
        let cookie = raw.jar.cookies(&raw.base).and_then(|h| h.to_str().ok().map(String::from)).unwrap();
        let mut e2 = Emacs::start(&bin, &["--hooks"]).await;
        e2.send(json!({"t":"hello","client":"olb-integration","version":0})).await;
        e2.send(json!({"t":"open","params":{"server":server,"project_id":pid,"mirror_dir":std::env::temp_dir().join(format!("olb-mirror2-{pid}"))},"credentials":{"method":"cookie","cookie":cookie}})).await;
        let r = e2.wait("ready (cookie)", 60, |m| m["t"] == "ready" || m["t"] == "need_auth" || m["t"] == "error").await;
        check!(r["t"] == "ready", "cookie login: {}", r["t"]);
        e2.send(json!({"t":"close"})).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), e2.child.wait()).await;
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join(format!("olb-mirror2-{pid}")));
    }

    // ---- teardown ----
    em.send(json!({"t":"detach","path":"main.tex"})).await;
    em.pump(300).await;
    check!(std::fs::read_to_string(mirror.join("main.tex")).unwrap() == st3, "detach wrote the final text to the mirror");
    em.send(json!({"t":"close"})).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), em.child.wait()).await;
    sock.close();
    match raw.delete(&format!("/project/{pid}")).await {
        Ok(()) => println!("deleted test project"),
        Err(e) => println!("note: could not delete test project: {e}"),
    }
    let _ = std::fs::remove_dir_all(&mirror);
    println!("ALL INTEGRATION CHECKS PASSED in {:.1}s", t0.elapsed().as_secs_f32());
}
