use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use catplay_iap2_client::CsmClientHandleRef;

/// Shared output channel for route-guidance injection into the live TX CSM
/// session (the nav self-test; later the live rewriter).
///
/// The TX CSM session publishes its live [`CsmClientHandleRef`] here on
/// `start()` (once per iAP2 pipe); the client gadget invalidates the box on
/// every session teardown/replacement.
///
/// Staleness is tracked with a generation counter because
/// `AsyncHandle::is_closed()`/`is_writable()` are stubs. Consumers must
/// snapshot `(handle, generation)` and re-check the generation before every
/// send, aborting if it changed — the captured handle may belong to a dead
/// iAP2 pipe.
///
/// `AtomicU32` (not `u64`): the RISC-V 32-bit target has no `AtomicU64`.
pub struct NavOutbox {
    handle: Mutex<Option<CsmClientHandleRef>>,
    generation: AtomicU32,
}

impl NavOutbox {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(None),
            generation: AtomicU32::new(0),
        })
    }

    /// Publish the handle of a fresh session, bumping the generation.
    /// Returns the new generation.
    pub fn publish(&self, handle: CsmClientHandleRef) -> u32 {
        *self.handle.lock().unwrap() = Some(handle);
        self.bump()
    }

    /// Invalidate (session teardown/replacement): drop the handle and bump
    /// the generation so any in-flight consumer stops without sending.
    pub fn invalidate(&self) {
        *self.handle.lock().unwrap() = None;
        self.bump();
    }

    /// Snapshot of `(handle, generation)` for staleness checks.
    pub fn snapshot(&self) -> (Option<CsmClientHandleRef>, u32) {
        let handle = self.handle.lock().unwrap().clone();
        (handle, self.generation.load(Ordering::Acquire))
    }

    fn bump(&self) -> u32 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }
}
