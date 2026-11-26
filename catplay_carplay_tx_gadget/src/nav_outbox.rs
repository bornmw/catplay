use std::sync::{Arc, Mutex};

use catplay_iap2_client::CsmClientHandleRef;
use catplay_csm::decoder::AsCsmPacket;

/// One logical state: handle + generation + subscription flag, protected
/// by a single mutex so every snapshot is a consistent pair.
///
/// (The previous design split these into `Mutex<Option<Handle>>` +
/// `AtomicU32`; the two variables were updated independently, so readers
/// could observe (new handle, old generation) or (old handle, new
/// generation) in the window between the two writes.)
struct NavOutboxState {
    handle: Option<CsmClientHandleRef>,
    generation: u32,
    /// The car sent StartRouteGuidanceUpdates (0x5200) on this session.
    route_subscribed: bool,
}

/// Shared output channel for route-guidance injection into the live TX CSM
/// session (the nav self-test; later the live rewriter).
///
/// Lifecycle is session-scoped: the CSM session publishes its live
/// [`CsmClientHandleRef`] in `start()` (once per iAP2 pipe) and invalidates
/// its OWN generation in `Drop` — an old session can therefore never clear
/// a handle published by a newer one.
///
/// `handle.send()` is synchronous and non-blocking (registry encode +
/// `VecDeque::push_back`), so [`send_if_current`] may hold the state lock
/// across the enqueue: validation and enqueue are one atomic operation.
pub struct NavOutbox {
    state: Mutex<NavOutboxState>,
}

impl NavOutbox {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(NavOutboxState {
                handle: None,
                generation: 0,
                route_subscribed: false,
            }),
        })
    }

    /// Publish the handle of a fresh session, bumping the generation and
    /// resetting the subscription flag. Returns the new generation.
    pub fn publish(&self, handle: CsmClientHandleRef) -> u32 {
        let mut s = self.state.lock().unwrap();
        s.generation = s.generation.wrapping_add(1);
        s.handle = Some(handle);
        s.route_subscribed = false;
        s.generation
    }

    /// Invalidate only if the state is still the one `expected` refers to
    /// (session-scoped teardown). Returns false when a newer generation
    /// has already superseded it — in which case nothing is cleared.
    pub fn invalidate_if_generation(&self, expected: u32) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.generation != expected {
            return false;
        }
        s.generation = s.generation.wrapping_add(1);
        s.handle = None;
        s.route_subscribed = false;
        true
    }

    /// Record that the car sent 0x5200 on the session of `generation`.
    /// No-op if the session has already been superseded.
    pub fn mark_route_subscribed(&self, generation: u32) {
        let mut s = self.state.lock().unwrap();
        if s.generation == generation {
            s.route_subscribed = true;
        }
    }

    /// Record that the car stopped the route-guidance subscription
    /// (A6-originated 0x5203 — it lists 0x5203 in its sent ids). The
    /// bridge closes its TX gate until the car re-subscribes with 0x5200.
    /// No-op if the session has already been superseded.
    pub fn mark_route_unsubscribed(&self, generation: u32) {
        let mut s = self.state.lock().unwrap();
        if s.generation == generation {
            s.route_subscribed = false;
        }
    }

    /// Consistent snapshot of `(handle, generation, route_subscribed)`.
    pub fn snapshot(&self) -> (Option<CsmClientHandleRef>, u32, bool) {
        let s = self.state.lock().unwrap();
        (s.handle.clone(), s.generation, s.route_subscribed)
    }

    /// Generation-checked enqueue, atomic with the validation: the handle
    /// used is exactly the one stored in the state that matches
    /// `generation`. Fails (without sending) if the session went away.
    pub fn send_if_current(&self, generation: u32, packet: &dyn AsCsmPacket) -> Result<(), String> {
        let s = self.state.lock().unwrap();
        if s.generation != generation {
            return Err(format!("generation changed before send (current {})", s.generation));
        }
        let handle = s
            .handle
            .as_ref()
            .ok_or_else(|| "no handle for current generation".to_string())?;
        handle
            .send(packet)
            .map_err(|e| format!("send failed: {e:?}"))
    }
}
