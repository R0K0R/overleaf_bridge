//! Probe: what does the server send for concurrent history-ot updates?
//! OLB_PROJECT=<id of a history-ot test project>  (see integration.rs, OLB_HISTORY)
use overleaf_bridge::rawclient::Raw;
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let creds: Value = serde_json::from_str(&std::fs::read_to_string(".test-credentials").unwrap()).unwrap();
    let raw = Raw::login(creds["server"].as_str().unwrap(), creds["email"].as_str().unwrap(), creds["password"].as_str().unwrap()).await.unwrap();
    let pid = std::env::var("OLB_PROJECT").expect("OLB_PROJECT");
    let mut a = raw.connect(&pid).await.unwrap();
    let mut b = raw.connect(&pid).await.unwrap();
    let doc = a.project["rootFolder"][0]["docs"][0]["_id"].as_str().unwrap().to_string();
    let (ta, va, ty) = a.join_doc(&doc, true).await.unwrap();
    let (_tb, vb, _) = b.join_doc(&doc, true).await.unwrap();
    println!("type {ty} len {} versions {va} {vb} A={} B={}", ta.len(), a.public_id, b.public_id);
    let n = ta.chars().count();
    let (opa, opb) = if ty == "history-ot" {
        (json!([{"textOperation": ["A", n]}]), json!([{"textOperation": ["B", n]}]))
    } else {
        (json!([{"p": 0, "i": "A"}]), json!([{"p": 0, "i": "B"}]))
    };
    let ua = json!({"doc": doc, "op": opa, "v": va});
    let ub = json!({"doc": doc, "op": opb, "v": vb});
    a.call("applyOtUpdate", vec![json!(doc), ua]).await.unwrap();
    b.call("applyOtUpdate", vec![json!(doc), ub]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    for (name, s) in [("A", &mut a), ("B", &mut b)] {
        while let Ok((n, args)) = s.events.try_recv() {
            if n == "otUpdateApplied" {
                println!("{name} got {}", args[0]);
            }
        }
    }
    let (t, v, _) = a.join_doc(&doc, true).await.unwrap();
    println!("final v={v} text starts {:?}", t.chars().take(3).collect::<String>());
}
