use overleaf_bridge::core::Bridge;
use overleaf_bridge::{fake::FakeServer, net::NetEvent, real, Backend};
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let test = std::env::args().any(|a| a == "--test");
    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<NetEvent>();
    let mut backend: Box<dyn Backend + Send> = if test { Box::new(FakeServer::new(ev_tx)) } else { Box::new(real::spawn(ev_tx)) };
    let mut bridge = Bridge::new(test);
    bridge.hooks = std::env::args().any(|a| a == "--hooks");
    let (in_tx, mut in_rx) = tokio::sync::mpsc::unbounded_channel::<Option<String>>();
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(l)) => {
                    let _ = in_tx.send(Some(l));
                }
                _ => {
                    let _ = in_tx.send(None);
                    break;
                }
            }
        }
    });
    let mut stdout = tokio::io::stdout();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            biased;
            ev = ev_rx.recv() => { if let Some(ev) = ev { bridge.handle_net(ev); } }
            line = in_rx.recv() => {
                match line {
                    Some(Some(l)) => {
                        let l = l.trim();
                        if l.is_empty() { continue; }
                        match serde_json::from_str::<Value>(l) {
                            Ok(m) if m.is_object() => bridge.handle_emacs(m),
                            _ => eprintln!("[overleaf_bridge] bad json: {:.100}", l),
                        }
                    }
                    _ => { backend.send(overleaf_bridge::net::NetCmd::Close); break; }
                }
            }
            _ = ticker.tick() => bridge.tick(Instant::now()),
        }
        // settle: drain commands and their synchronous replies
        loop {
            for c in std::mem::take(&mut bridge.net_out) { backend.send(c); }
            let mut any = false;
            while let Ok(ev) = ev_rx.try_recv() { bridge.handle_net(ev); any = true; }
            if !any && bridge.net_out.is_empty() { break; }
        }
        let out = std::mem::take(&mut bridge.emacs_out);
        if !out.is_empty() {
            let mut buf = String::new();
            for m in out { buf.push_str(&m.to_string()); buf.push('\n'); }
            if stdout.write_all(buf.as_bytes()).await.is_err() || stdout.flush().await.is_err() { break; }
        }
        if let Some(code) = bridge.exit {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::process::exit(code);
        }
    }
}
