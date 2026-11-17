//! The boundary between the bridge core and the network: the core issues
//! `NetCmd`s and receives `NetEvent`s. `real.rs` talks to Overleaf; `fake.rs`
//! is an in-process Overleaf for `--test` and unit tests.
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum DocType {
    Share,
    History,
}

#[derive(Clone, Debug, Default)]
pub struct Folder {
    pub id: String,
    pub name: String,
    pub docs: Vec<(String, String)>,
    pub files: Vec<(String, String)>,
    pub folders: Vec<Folder>,
}

#[derive(Clone, Debug)]
pub struct ProjectInfo {
    pub id: String,
    pub name: String,
    pub root: Folder,
}

#[derive(Clone, Debug)]
pub struct Credentials {
    pub method: String,
    pub email: Option<String>,
    pub password: Option<String>,
    pub cookie_name: Option<String>,
    pub cookie: Option<String>,
}

#[derive(Clone, Debug)]
pub enum RestReq {
    CreateDoc {
        parent: String,
        name: String,
    },
    CreateFolder {
        parent: String,
        name: String,
    },
    Rename {
        kind: String,
        id: String,
        name: String,
    },
    Move {
        kind: String,
        id: String,
        folder: String,
    },
    Delete {
        kind: String,
        id: String,
    },
}

#[derive(Clone, Debug)]
pub enum NetCmd {
    /// Log in, find the project and (for the first time) connect.
    Open {
        params: Value,
        creds: Option<Credentials>,
    },
    /// Re-establish the socket after an interruption.
    Reconnect,
    JoinDoc {
        doc_id: String,
        from_version: Option<i64>,
    },
    Update {
        doc_id: String,
        v: i64,
        op: Value,
        dup_if_source: Vec<String>,
    },
    Cursor {
        doc_id: String,
        row: usize,
        column: usize,
    },
    FetchFile {
        file_id: String,
    },
    Rest {
        tag: u64,
        req: RestReq,
    },
    /// Test hook: an edit by another client, over the current text.
    TestRemote {
        doc_id: String,
        op: crate::ot::Op,
    },
    /// Test hook: drop the connection as if the network failed.
    DropConnection,
    Close,
}

#[derive(Clone, Debug)]
pub struct DocSnapshot {
    pub text: String,
    pub version: i64,
    pub kind: DocType,
    /// Updates since `from_version`, in order: (v, op, source).
    pub updates: Vec<WireUpdate>,
}

#[derive(Clone, Debug)]
pub struct WireUpdate {
    pub v: i64,
    pub op: Option<Value>,
    pub source: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub doc_id: Option<String>,
    pub row: Option<usize>,
    pub column: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum TreeEvent {
    NewDoc {
        parent: String,
        id: String,
        name: String,
    },
    NewFile {
        parent: String,
        id: String,
        name: String,
    },
    NewFolder {
        parent: String,
        id: String,
        name: String,
    },
    Rename {
        id: String,
        name: String,
    },
    Move {
        id: String,
        folder: String,
    },
    Remove {
        id: String,
    },
}

#[derive(Clone, Debug)]
pub enum NetEvent {
    NeedAuth {
        reason: String,
    },
    Fatal {
        message: String,
    },
    /// Joined the project (first connection or a reconnection).
    Connected {
        public_id: String,
        project: ProjectInfo,
        user_name: String,
        user_id: String,
    },
    /// The connection is gone; `retry` says whether reconnecting makes sense.
    Disconnected {
        reason: String,
        retry: bool,
    },
    DocJoined {
        doc_id: String,
        result: Result<DocSnapshot, String>,
    },
    Update {
        doc_id: String,
        update: WireUpdate,
    },
    UpdateError {
        doc_id: String,
        message: String,
    },
    Peer(Peer),
    PeerLeft {
        id: String,
    },
    Tree(TreeEvent),
    FileData {
        file_id: String,
        result: Result<Vec<u8>, String>,
    },
    RestDone {
        tag: u64,
        result: Result<Value, String>,
    },
    Log(String),
}
