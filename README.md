# overleaf_bridge

A bridge between Emacs and Overleaf. Emacs speaks the emacs-collab protocol
(`emacs-collab.el/PROTOCOL.md`, version 0) to this program over stdio; the
program speaks Overleaf's real-time protocol (socket.io 0.9 plus REST) to the
server. GPL-3.0-or-later; see NOTICE for the MIT-licensed prior art.

    overleaf_bridge            # real Overleaf
    overleaf_bridge --test     # in-process fake Overleaf, for the conformance suite
    overleaf_bridge --hooks    # real Overleaf, plus x-test-text/-pending/-drop (integration tests)

`open.params`: `{server, project_id | project_name, mirror_dir?}`.
`open.credentials`: `{method:"password", email, password}` or
`{method:"cookie", cookie_name?, cookie}`. Nothing is read from disk. Missing or
rejected credentials give `need_auth`. Overleaf.com (cookie login) is
implemented but untested.

## Layout

- `src/core.rs`: the synchronous state machine (emacs-collab sync, per-doc
  Overleaf OT client, mirror, file tree, reconnect). Talks to the network only
  through `NetCmd`/`NetEvent` (`src/net.rs`).
- `src/real.rs`, `src/sio.rs`: login, REST, socket.io 0.9 codec and client.
- `src/fake.rs`: the fake Overleaf behind the same trait (sharejs-text-ot or
  history-ot; `params.fake_history`).
- `src/ot.rs`: TextOperation (code points), UTF-16/ShareJS/history-ot conversions.
- `examples/integration.rs`: end-to-end test against a real CE
  (`OLB_INTEGRATION=1 cargo run --example integration`, optional `OLB_HISTORY=1`).
  Needs `.test-credentials` (JSON `{server,email,password}`, git-ignored).

## Tests

    cargo test
    python3 ../emacs-collab.el/conformance/run.py --bridge "$PWD/target/debug/overleaf_bridge"
