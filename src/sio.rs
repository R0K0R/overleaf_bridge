//! Socket.IO 0.9 packet codec (Overleaf's real-time service speaks 0.9).
//! Wire format: `type:id[+]:endpoint:data`.
use serde_json::{json, Value};

#[derive(Debug, PartialEq)]
pub enum Packet {
    Disconnect,
    Connect,
    Heartbeat,
    Message(String),
    Event { id: Option<String>, name: String, args: Vec<Value> },
    Ack { id: String, args: Vec<Value> },
    Error(String),
    Noop,
}

pub fn decode(raw: &str) -> Result<Packet, String> {
    let mut it = raw.splitn(4, ':');
    let ty = it.next().unwrap_or("");
    let id = it.next().unwrap_or("");
    let _endpoint = it.next().unwrap_or("");
    let data = it.next().unwrap_or("");
    Ok(match ty {
        "0" => Packet::Disconnect,
        "1" => Packet::Connect,
        "2" => Packet::Heartbeat,
        "3" | "4" => Packet::Message(data.to_string()),
        "5" => {
            let v: Value = serde_json::from_str(data).map_err(|e| format!("bad event json: {e}"))?;
            let name = v.get("name").and_then(|x| x.as_str()).ok_or("event without name")?.to_string();
            let args = v.get("args").and_then(|x| x.as_array()).cloned().unwrap_or_default();
            let id = if id.is_empty() { None } else { Some(id.trim_end_matches('+').to_string()) };
            Packet::Event { id, name, args }
        }
        "6" => {
            // data is "id+[json...]"
            let (id, rest) = match data.find('+') {
                Some(i) => (&data[..i], &data[i + 1..]),
                None => (data, ""),
            };
            let args = if rest.is_empty() { vec![] } else { serde_json::from_str::<Value>(rest).map_err(|e| format!("bad ack json: {e}"))?.as_array().cloned().unwrap_or_default() };
            Packet::Ack { id: id.to_string(), args }
        }
        "7" => Packet::Error(data.to_string()),
        "8" => Packet::Noop,
        _ => return Err(format!("unknown packet {raw:.40}")),
    })
}

pub fn encode_event(id: Option<u64>, name: &str, args: Vec<Value>) -> String {
    let body = json!({"name": name, "args": args}).to_string();
    match id {
        Some(i) => format!("5:{i}+::{body}"),
        None => format!("5:::{body}"),
    }
}

/// Overleaf sends doc lines as UTF-8 bytes packed one per char (Latin-1).
pub fn unpack_latin1_utf8(s: &str) -> String {
    let bytes: Vec<u8> = s.chars().map(|c| if (c as u32) < 256 { c as u8 } else { b'?' }).collect();
    match String::from_utf8(bytes) {
        Ok(t) => t,
        Err(_) => s.to_string(), // already unpacked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn codec() {
        assert_eq!(decode("2::").unwrap(), Packet::Heartbeat);
        assert_eq!(decode("1::").unwrap(), Packet::Connect);
        let e = decode(r#"5:::{"name":"x:y","args":[1,"a:b"]}"#).unwrap();
        assert_eq!(e, Packet::Event { id: None, name: "x:y".into(), args: vec![json!(1), json!("a:b")] });
        let a = decode(r#"6:::12+[null,"l:1",3]"#).unwrap();
        assert_eq!(a, Packet::Ack { id: "12".into(), args: vec![Value::Null, json!("l:1"), json!(3)] });
        assert_eq!(encode_event(Some(3), "joinDoc", vec![json!("x")]), r#"5:3+::{"name":"joinDoc","args":["x"]}"#);
    }
    #[test]
    fn latin1() {
        let packed: String = "é漢".bytes().map(|b| b as char).collect();
        assert_eq!(unpack_latin1_utf8(&packed), "é漢");
        assert_eq!(unpack_latin1_utf8("plain ascii"), "plain ascii");
    }
}
