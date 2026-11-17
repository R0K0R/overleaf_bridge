//! ot.js-style TextOperation over code points (PROTOCOL.md section 3), plus
//! conversions to and from Overleaf's wire formats (UTF-16 units).
//!
//! The transform/compose algorithms follow ot.js (MIT) and the reference
//! implementation `bridge-ref/ot.py` of emacs-collab.
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum C {
    R(usize),
    I(String),
    D(usize),
}
pub type Op = Vec<C>;
pub type R<T> = Result<T, String>;

#[derive(Default)]
struct B {
    ops: Op,
}
impl B {
    fn retain(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(C::R(m)) = self.ops.last_mut() {
            *m += n;
        } else {
            self.ops.push(C::R(n));
        }
    }
    fn insert(&mut self, s: &str) {
        if s.is_empty() {
            return;
        }
        let len = self.ops.len();
        match self.ops.last_mut() {
            Some(C::I(x)) => x.push_str(s),
            Some(C::D(_)) => {
                if len >= 2 {
                    if let C::I(x) = &mut self.ops[len - 2] {
                        x.push_str(s);
                        return;
                    }
                }
                self.ops.insert(len - 1, C::I(s.to_string()));
            }
            _ => self.ops.push(C::I(s.to_string())),
        }
    }
    fn delete(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        if let Some(C::D(m)) = self.ops.last_mut() {
            *m += n;
        } else {
            self.ops.push(C::D(n));
        }
    }
}

pub fn normalize(op: &Op) -> Op {
    let mut b = B::default();
    for c in op {
        match c {
            C::R(n) => b.retain(*n),
            C::I(s) => b.insert(s),
            C::D(n) => b.delete(*n),
        }
    }
    b.ops
}
pub fn base_len(op: &Op) -> usize {
    op.iter()
        .map(|c| match c {
            C::R(n) | C::D(n) => *n,
            C::I(_) => 0,
        })
        .sum()
}
pub fn target_len(op: &Op) -> usize {
    op.iter()
        .map(|c| match c {
            C::R(n) => *n,
            C::I(s) => s.chars().count(),
            C::D(_) => 0,
        })
        .sum()
}
pub fn is_noop(op: &Op) -> bool {
    let n = normalize(op);
    n.is_empty() || (n.len() == 1 && matches!(n[0], C::R(_)))
}
pub fn identity(len: usize) -> Op {
    if len == 0 {
        vec![]
    } else {
        vec![C::R(len)]
    }
}
pub fn from_change(doc_len: usize, pos: usize, del: usize, ins: &str) -> R<Op> {
    if pos + del > doc_len {
        return Err("change out of range".into());
    }
    let mut b = B::default();
    b.retain(pos);
    b.delete(del);
    b.insert(ins);
    b.retain(doc_len - pos - del);
    Ok(b.ops)
}

pub fn apply(doc: &[char], op: &Op) -> R<Vec<char>> {
    if base_len(op) != doc.len() {
        return Err(format!(
            "base length {} does not match document length {}",
            base_len(op),
            doc.len()
        ));
    }
    let mut out = Vec::with_capacity(doc.len());
    let mut i = 0;
    for c in op {
        match c {
            C::I(s) => out.extend(s.chars()),
            C::R(n) => {
                out.extend_from_slice(&doc[i..i + n]);
                i += n;
            }
            C::D(n) => i += n,
        }
    }
    Ok(out)
}

/// Split a component after `m` units (retain/delete: count; insert: chars).
fn rest(c: &C, m: usize) -> Option<C> {
    match c {
        C::R(n) => (*n > m).then(|| C::R(n - m)),
        C::D(n) => (*n > m).then(|| C::D(n - m)),
        C::I(s) => {
            let t: String = s.chars().skip(m).collect();
            (!t.is_empty()).then_some(C::I(t))
        }
    }
}
fn clen(c: &C) -> usize {
    match c {
        C::R(n) | C::D(n) => *n,
        C::I(s) => s.chars().count(),
    }
}

pub fn compose(a: &Op, b: &Op) -> R<Op> {
    let a = normalize(a);
    let b = normalize(b);
    if target_len(&a) != base_len(&b) {
        return Err("compose: target length of a != base length of b".into());
    }
    let mut res = B::default();
    let (mut ia, mut ib) = (a.into_iter(), b.into_iter());
    let (mut o1, mut o2) = (ia.next(), ib.next());
    while o1.is_some() || o2.is_some() {
        if let Some(C::D(n)) = o1 {
            res.delete(n);
            o1 = ia.next();
            continue;
        }
        if let Some(C::I(s)) = &o2 {
            res.insert(s);
            o2 = ib.next();
            continue;
        }
        let (x, y) = match (&o1, &o2) {
            (Some(x), Some(y)) => (x.clone(), y.clone()),
            _ => return Err("compose: operations too short".into()),
        };
        let m = clen(&x).min(clen(&y));
        match (&x, &y) {
            (C::R(_), C::R(_)) => res.retain(m),
            (C::I(s), C::R(_)) => res.insert(&s.chars().take(m).collect::<String>()),
            (C::I(_), C::D(_)) => {}
            (C::R(_), C::D(_)) => res.delete(m),
            _ => unreachable!(),
        }
        o1 = rest(&x, m).or_else(|| ia.next());
        o2 = rest(&y, m).or_else(|| ib.next());
    }
    Ok(res.ops)
}

/// Returns (a', b'). When both insert at the same position, a's comes first.
pub fn transform(a: &Op, b: &Op) -> R<(Op, Op)> {
    let a = normalize(a);
    let b = normalize(b);
    if base_len(&a) != base_len(&b) {
        return Err("transform: base lengths differ".into());
    }
    let (mut ap, mut bp) = (B::default(), B::default());
    let (mut ia, mut ib) = (a.into_iter(), b.into_iter());
    let (mut o1, mut o2) = (ia.next(), ib.next());
    while o1.is_some() || o2.is_some() {
        if let Some(C::I(s)) = &o1 {
            ap.insert(s);
            bp.retain(s.chars().count());
            o1 = ia.next();
            continue;
        }
        if let Some(C::I(s)) = &o2 {
            ap.retain(s.chars().count());
            bp.insert(s);
            o2 = ib.next();
            continue;
        }
        let (x, y) = match (&o1, &o2) {
            (Some(x), Some(y)) => (x.clone(), y.clone()),
            _ => return Err("transform: operations too short".into()),
        };
        let m = clen(&x).min(clen(&y));
        match (&x, &y) {
            (C::R(_), C::R(_)) => {
                ap.retain(m);
                bp.retain(m);
            }
            (C::D(_), C::D(_)) => {}
            (C::D(_), C::R(_)) => ap.delete(m),
            (C::R(_), C::D(_)) => bp.delete(m),
            _ => unreachable!(),
        }
        o1 = rest(&x, m).or_else(|| ia.next());
        o2 = rest(&y, m).or_else(|| ib.next());
    }
    Ok((ap.ops, bp.ops))
}

// ---- JSON (emacs-collab wire form) ---------------------------------------
pub fn op_to_json(op: &Op) -> Value {
    Value::Array(
        op.iter()
            .map(|c| match c {
                C::R(n) => json!(*n as i64),
                C::D(n) => json!(-(*n as i64)),
                C::I(s) => json!(s),
            })
            .collect(),
    )
}
pub fn op_from_json(v: &Value) -> R<Op> {
    let arr = v.as_array().ok_or("operation must be a list")?;
    let mut out = Vec::new();
    for x in arr {
        if let Some(s) = x.as_str() {
            out.push(C::I(s.to_string()));
        } else if let Some(n) = x.as_i64() {
            if n > 0 {
                out.push(C::R(n as usize))
            } else if n < 0 {
                out.push(C::D((-n) as usize))
            }
        } else {
            return Err(format!("bad component {x}"));
        }
    }
    Ok(normalize(&out))
}

// ---- code points <-> UTF-16 units ----------------------------------------
fn u16len(c: char) -> usize {
    c.len_utf16()
}
/// Convert an op counted in code points to one counted in UTF-16 units.
pub fn to_units(op: &Op, text: &[char]) -> R<Op> {
    let mut i = 0;
    let mut out = Vec::new();
    for c in op {
        match c {
            C::I(s) => out.push(C::I(s.clone())),
            C::R(n) | C::D(n) => {
                if i + n > text.len() {
                    return Err("op longer than text".into());
                }
                let u: usize = text[i..i + n].iter().map(|c| u16len(*c)).sum();
                i += n;
                out.push(if matches!(c, C::R(_)) { C::R(u) } else { C::D(u) });
            }
        }
    }
    Ok(out)
}
/// Inverse of [`to_units`].
pub fn from_units(op: &Op, text: &[char]) -> R<Op> {
    let mut i = 0;
    let mut out = Vec::new();
    for c in op {
        match c {
            C::I(s) => out.push(C::I(s.clone())),
            C::R(n) | C::D(n) => {
                let mut u = 0;
                let mut k = 0;
                while u < *n {
                    let ch = *text.get(i + k).ok_or("op longer than text")?;
                    u += u16len(ch);
                    k += 1;
                }
                if u != *n {
                    return Err("op splits a surrogate pair".into());
                }
                i += k;
                out.push(if matches!(c, C::R(_)) { C::R(k) } else { C::D(k) });
            }
        }
    }
    Ok(out)
}
pub fn units_of(s: &[char]) -> usize {
    s.iter().map(|c| u16len(*c)).sum()
}

// ---- sharejs-text-ot ------------------------------------------------------
/// Convert a cp op (over `text`) to sharejs components `[{p,i}|{p,d}]`.
pub fn to_sharejs(op: &Op, text: &[char]) -> R<Vec<Value>> {
    let mut out = Vec::new();
    let (mut i, mut p) = (0usize, 0usize);
    for c in op {
        match c {
            C::R(n) => {
                if i + n > text.len() {
                    return Err("op longer than text".into());
                }
                p += units_of(&text[i..i + n]);
                i += n;
            }
            C::I(s) => {
                out.push(json!({"p": p, "i": s}));
                p += s.encode_utf16().count();
            }
            C::D(n) => {
                if i + n > text.len() {
                    return Err("op longer than text".into());
                }
                let d: String = text[i..i + n].iter().collect();
                i += n;
                out.push(json!({"p": p, "d": d}));
            }
        }
    }
    Ok(out)
}
/// Convert sharejs components (applied sequentially) to a cp op over `text`.
pub fn from_sharejs(comps: &[Value], text: &[char]) -> R<Op> {
    let mut cur: Vec<char> = text.to_vec();
    let mut acc = identity(text.len());
    for c in comps {
        let p = c.get("p").and_then(|x| x.as_u64()).ok_or("component without p")? as usize;
        // unit offset -> char index
        let (mut u, mut k) = (0usize, 0usize);
        while u < p {
            let ch = *cur.get(k).ok_or("position beyond document")?;
            u += u16len(ch);
            k += 1;
        }
        if u != p {
            return Err("position splits a surrogate pair".into());
        }
        let single = if let Some(s) = c.get("i").and_then(|x| x.as_str()) {
            from_change(cur.len(), k, 0, s)?
        } else if let Some(d) = c.get("d").and_then(|x| x.as_str()) {
            let n = d.chars().count();
            if k + n > cur.len() {
                return Err("delete beyond document".into());
            }
            from_change(cur.len(), k, n, "")?
        } else {
            continue; // comment components do not change the text
        };
        cur = apply(&cur, &single)?;
        acc = compose(&acc, &single)?;
    }
    Ok(acc)
}

// ---- history-ot -----------------------------------------------------------
pub fn to_history(op: &Op, text: &[char]) -> R<Value> {
    let u = to_units(op, text)?;
    let comps: Vec<Value> = u
        .iter()
        .map(|c| match c {
            C::R(n) => json!(*n as i64),
            C::D(n) => json!(-(*n as i64)),
            C::I(s) => json!(s),
        })
        .collect();
    Ok(json!([{"textOperation": comps}]))
}
pub fn from_history(ops: &Value, text: &[char]) -> R<Op> {
    let arr = ops.as_array().ok_or("history op must be a list")?;
    let mut cur = text.to_vec();
    let mut acc = identity(text.len());
    for o in arr {
        let Some(t) = o.get("textOperation").and_then(|x| x.as_array()) else {
            continue; // comment operations do not change the text
        };
        let mut op = Vec::new();
        for c in t {
            if let Some(s) = c.as_str() {
                op.push(C::I(s.to_string()));
            } else if let Some(n) = c.as_i64() {
                op.push(if n >= 0 { C::R(n as usize) } else { C::D((-n) as usize) });
            } else if let Some(n) = c.get("r").and_then(|x| x.as_u64()) {
                op.push(C::R(n as usize));
            } else if let Some(s) = c.get("i").and_then(|x| x.as_str()) {
                op.push(C::I(s.to_string()));
            } else {
                return Err(format!("unknown history component {c}"));
            }
        }
        let cp = from_units(&normalize(&op), &cur)?;
        cur = apply(&cur, &cp)?;
        acc = compose(&acc, &cp)?;
    }
    Ok(acc)
}

/// Overleaf stores no astral characters in inserted text (surrogates become
/// U+FFFD). Returns an op (over `text`) that replaces each astral character
/// by two U+FFFD, or None when there is none.
pub fn astral_fix(text: &[char]) -> Option<Op> {
    if !text.iter().any(|c| (*c as u32) > 0xFFFF) {
        return None;
    }
    let mut b = B::default();
    for c in text {
        if (*c as u32) > 0xFFFF {
            b.delete(1);
            b.insert("\u{FFFD}\u{FFFD}");
        } else {
            b.retain(1);
        }
    }
    Some(b.ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cs(s: &str) -> Vec<char> {
        s.chars().collect()
    }
    #[test]
    fn basic() {
        let op = vec![C::R(1), C::I("a".into()), C::D(2), C::R(2)];
        assert_eq!(apply(&cs("hello"), &op).unwrap(), cs("halo"));
    }
    #[test]
    fn transform_converges() {
        let d = cs("abcdef");
        let a = from_change(6, 2, 1, "XY").unwrap();
        let b = from_change(6, 2, 3, "Z").unwrap();
        let (a2, b2) = transform(&a, &b).unwrap();
        assert_eq!(
            apply(&apply(&d, &a).unwrap(), &b2).unwrap(),
            apply(&apply(&d, &b).unwrap(), &a2).unwrap()
        );
    }
    #[test]
    fn tie_a_first() {
        let a = from_change(2, 1, 0, "A").unwrap();
        let b = from_change(2, 1, 0, "B").unwrap();
        let (_, b2) = transform(&a, &b).unwrap();
        assert_eq!(apply(&apply(&cs("xy"), &a).unwrap(), &b2).unwrap(), cs("xABy"));
    }
    #[test]
    fn units_roundtrip() {
        let t = cs("a😀b");
        let op = from_change(3, 2, 1, "Z").unwrap();
        let u = to_units(&op, &t).unwrap();
        assert_eq!(u, vec![C::R(3), C::I("Z".into()), C::D(1)]);
        assert_eq!(from_units(&u, &t).unwrap(), op);
    }
    #[test]
    fn sharejs_roundtrip() {
        let t = cs("a😀bc");
        let op = vec![C::R(1), C::D(1), C::I("QQ".into()), C::R(1), C::D(1)];
        let sj = to_sharejs(&op, &t).unwrap();
        let back = from_sharejs(&sj, &t).unwrap();
        assert_eq!(apply(&t, &back).unwrap(), apply(&t, &op).unwrap());
    }
    #[test]
    fn history_roundtrip() {
        let t = cs("a😀bc");
        let op = vec![C::R(1), C::D(1), C::I("QQ".into()), C::R(2)];
        let h = to_history(&op, &t).unwrap();
        assert_eq!(h, json!([{"textOperation":[1,-2,"QQ",2]}]));
        assert_eq!(from_history(&h, &t).unwrap(), op);
    }
}
