pub mod core;
pub mod fake;
pub mod net;
pub mod ot;
pub mod real;
pub mod sio;

/// The network side of the bridge: the core sends commands, and the backend
/// pushes `NetEvent`s onto the channel it was built with.
pub trait Backend {
    fn send(&mut self, cmd: net::NetCmd);
}
