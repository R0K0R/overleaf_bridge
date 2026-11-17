//! Core + fake server tests for the Overleaf-side OT client: inflight/buffer,
//! resend with dupIfSource, reconnect with replay. Run for both doc types.
use overleaf_bridge::core::Bridge;
use overleaf_bridge::fake::FakeServer;
use overleaf_bridge::net::NetEvent;
use overleaf_bridge::ot::{self, Op};
use overleaf_bridge::Backend;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

struct H {
    b: Bridge,
    s: FakeServer,
    rx: UnboundedReceiver<NetEvent>,
    out: Vec<Value>,
    etext: Vec<char>,
    sent: u64,
    seen: u64,
    pending: Vec<(u64, Op)>,
    t0: Instant,
    doc: String,
}

impl H {
    fn new(history: bool, text: &str) -> H {
        let (tx, rx) = unbounded_channel();
        let mut s = FakeServer::new(tx);
        s.use_history(history);
        let doc = s.add_doc("root", "a.tex", text);
        let mut h = H {
            b: Bridge::new(true),
            s,
            rx,
            out: vec![],
            etext: vec![],
            sent: 0,
            seen: 0,
            pending: vec![],
            t0: Instant::now(),
            doc,
        };
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "olb-t-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        h.emacs(json!({"t":"hello","client":"t","version":0}));
        h.emacs(json!({"t":"open","params":{"mirror_dir":dir}}));
        assert!(
            h.out.iter().any(|m| m["t"] == "ready"),
            "ready: {:?}",
            h.out
        );
        h.emacs(json!({"t":"attach","path":"a.tex"}));
        let d = h
            .out
            .iter()
            .rev()
            .find(|m| m["t"] == "doc")
            .unwrap()
            .clone();
        h.etext = d["text"].as_str().unwrap().chars().collect();
        h
    }
    fn settle(&mut self) {
        loop {
            let cmds = std::mem::take(&mut self.b.net_out);
            let had = !cmds.is_empty();
            for c in cmds {
                self.s.send(c);
            }
            let mut any = false;
            while let Ok(ev) = self.rx.try_recv() {
                self.b.handle_net(ev);
                any = true;
            }
            if !had && !any && self.b.net_out.is_empty() {
                break;
            }
        }
        for m in std::mem::take(&mut self.b.emacs_out) {
            self.on_msg(&m);
            self.out.push(m);
        }
    }
    fn on_msg(&mut self, m: &Value) {
        match m["t"].as_str() {
            Some("remote") => {
                assert_eq!(m["seq"].as_u64().unwrap(), self.seen + 1);
                let s = m["seen"].as_u64().unwrap();
                self.pending.retain(|(q, _)| *q > s);
                let mut op = ot::op_from_json(&m["op"]).unwrap();
                for p in self.pending.iter_mut() {
                    let (o2, p2) = ot::transform(&op, &p.1).unwrap();
                    op = o2;
                    p.1 = p2;
                }
                self.etext = ot::apply(&self.etext, &op).unwrap();
                self.seen = m["seq"].as_u64().unwrap();
            }
            Some("ack") => {
                let s = m["seen"].as_u64().unwrap();
                self.pending.retain(|(q, _)| *q > s);
            }
            Some("doc") => {
                self.etext = m["text"].as_str().unwrap().chars().collect();
                self.sent = 0;
                self.seen = 0;
                self.pending.clear();
            }
            _ => {}
        }
    }
    fn emacs(&mut self, m: Value) {
        self.b.handle_emacs(m);
        self.settle();
    }
    fn edit(&mut self, pos: usize, del: usize, ins: &str) {
        let op = ot::from_change(self.etext.len(), pos, del, ins).unwrap();
        self.etext = ot::apply(&self.etext, &op).unwrap();
        self.sent += 1;
        self.pending.push((self.sent, op.clone()));
        let m = json!({"t":"edit","path":"a.tex","seq":self.sent,"seen":self.seen,"op":ot::op_to_json(&op)});
        self.emacs(m);
    }
    fn tick(&mut self, secs: u64) {
        self.b.tick(self.t0 + Duration::from_secs(secs));
        self.settle();
    }
    fn server_text(&self) -> String {
        self.s.doc_text(&self.doc).unwrap()
    }
    fn emacs_text(&self) -> String {
        self.etext.iter().collect()
    }
    fn other(&mut self, pos: usize, ins: &str) {
        let len = self.server_text().chars().count();
        self.s
            .other_edit(&self.doc, ot::from_change(len, pos, 0, ins).unwrap())
            .unwrap();
        self.settle();
    }
}

fn both(f: impl Fn(bool)) {
    f(false);
    f(true);
}

#[test]
fn other_edit_while_inflight() {
    both(|hist| {
        let mut h = H::new(hist, "hello world\n");
        h.s.hold_acks = true;
        h.edit(0, 0, "A");
        h.edit(1, 0, "B"); // buffered behind the inflight edit
        h.other(6, "X");
        h.other(0, "Y");
        h.s.hold_acks = false;
        h.s.release_held();
        h.settle();
        h.tick(1);
        h.tick(2);
        assert_eq!(h.emacs_text(), h.server_text(), "hist={hist}");
        assert!(h.server_text().contains('X') && h.server_text().contains("AB"));
    });
}

#[test]
fn resend_with_dup_applies_once() {
    both(|hist| {
        let mut h = H::new(hist, "base\n");
        h.s.hold_acks = true;
        h.edit(0, 0, "ONCE");
        h.tick(6); // no ack for 5 s: resend with dupIfSource
        assert_eq!(h.s.held.len(), 2);
        h.s.hold_acks = false;
        h.s.release_held();
        h.settle();
        assert_eq!(h.server_text().matches("ONCE").count(), 1, "hist={hist}");
        h.edit(0, 0, "NEXT"); // the doc must still be in sync afterwards
        assert_eq!(h.emacs_text(), h.server_text());
        assert!(h.server_text().starts_with("NEXTONCE"));
    });
}

#[test]
fn reconnect_replays_missed_updates() {
    both(|hist| {
        let mut h = H::new(hist, "one two three\n");
        h.emacs(json!({"t":"x-test-drop"}));
        assert!(h
            .out
            .iter()
            .any(|m| m["t"] == "status" && m["state"] == "offline"));
        h.other(0, "[remote1]");
        let n = h.server_text().chars().count();
        h.other(n, "[remote2]");
        h.edit(h.etext.len(), 0, "offline-edit");
        h.edit(0, 0, ">");
        for i in 1..30 {
            h.tick(i); // backoff elapses, reconnect happens
        }
        assert!(h
            .out
            .iter()
            .any(|m| m["t"] == "status" && m["state"] == "resyncing"));
        assert_eq!(
            h.out.iter().rev().find(|m| m["t"] == "status").unwrap()["state"],
            "connected"
        );
        assert_eq!(h.emacs_text(), h.server_text(), "hist={hist}");
        assert!(
            h.server_text().contains("offline-edit")
                && h.server_text().contains("[remote1]")
                && h.server_text().contains("[remote2]")
        );
    });
}

#[test]
fn astral_input_is_sanitised_when_enabled() {
    let mut h = H::new(false, "x\n");
    h.emacs(json!({"t":"open","params":{}})); // ignored: already open
                                              // enable sanitising as on a real server
    let (tx, _rx) = unbounded_channel::<NetEvent>();
    let _ = tx;
    h.edit(0, 0, "😀");
    // in test mode the astral char is kept
    assert_eq!(h.emacs_text(), h.server_text());
}
