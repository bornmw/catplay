//! Pipeline progress counters for hang diagnosis (DIAGNOSTIC, fix10).
//!
//! One in-car session must reveal which stage of the iPhone -> dongle -> car
//! pipeline stalls when CarPlay freezes. The counters are lock-free
//! (`AtomicU32` — the riscv32 target has no `AtomicU64`); timestamps are
//! milliseconds since first use in `u32` (wraps after ~49 days, irrelevant).
//!
//! A compact `HEALTH` line every 2 s and a `HANG_DETECTED` warning are
//! emitted by the monitor task in `catplay_c2a::diag` (needs tokio, so it
//! lives there, not here).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

static DIAG: OnceLock<Diag> = OnceLock::new();

pub fn diag() -> &'static Diag {
    DIAG.get_or_init(Diag::new)
}

pub struct Diag {
    start: Instant,
    rx_video_bytes: AtomicU32,
    rx_video_frames: AtomicU32,
    rx_video_last_ms: AtomicU32,
    tx_video_frames: AtomicU32,
    tx_video_bytes: AtomicU32,
    tx_video_last_ms: AtomicU32,
    tx_keepalives: AtomicU32,
    tx_keepalive_last_ms: AtomicU32,
    rtsp_events: AtomicU32,
    rtsp_last_ms: AtomicU32,
    session_active: AtomicBool,
}

impl Diag {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            rx_video_bytes: AtomicU32::new(0),
            rx_video_frames: AtomicU32::new(0),
            rx_video_last_ms: AtomicU32::new(0),
            tx_video_frames: AtomicU32::new(0),
            tx_video_bytes: AtomicU32::new(0),
            tx_video_last_ms: AtomicU32::new(0),
            tx_keepalives: AtomicU32::new(0),
            tx_keepalive_last_ms: AtomicU32::new(0),
            rtsp_events: AtomicU32::new(0),
            rtsp_last_ms: AtomicU32::new(0),
            session_active: AtomicBool::new(false),
        }
    }

    pub fn now_ms(&self) -> u32 {
        self.start.elapsed().as_millis() as u32
    }

    /// A video frame was received from the iPhone (RX side, before any
    /// dropping/forwarding decisions).
    pub fn rx_video_add(&self, frames: u32, bytes: u32) {
        self.rx_video_frames.fetch_add(frames, Ordering::Relaxed);
        self.rx_video_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.rx_video_last_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    /// A video frame was handed to the car socket (TX side).
    pub fn tx_video_add(&self, frames: u32, bytes: u32) {
        self.tx_video_frames.fetch_add(frames, Ordering::Relaxed);
        self.tx_video_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.tx_video_last_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    /// A screen keepalive was handed to the car socket (TX side).
    pub fn tx_keepalive(&self) {
        self.tx_keepalives.fetch_add(1, Ordering::Relaxed);
        self.tx_keepalive_last_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    /// An RTSP response was fulfilled (control-plane progress).
    pub fn rtsp_add(&self) {
        self.rtsp_events.fetch_add(1, Ordering::Relaxed);
        self.rtsp_last_ms.store(self.now_ms(), Ordering::Relaxed);
    }

    pub fn set_session_active(&self, active: bool) {
        self.session_active.store(active, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DiagSnapshot {
        let rx_last = self.rx_video_last_ms.load(Ordering::Relaxed);
        let tx_last = self.tx_video_last_ms.load(Ordering::Relaxed);
        let ka_last = self.tx_keepalive_last_ms.load(Ordering::Relaxed);
        let rtsp_last = self.rtsp_last_ms.load(Ordering::Relaxed);
        let last_any = [rx_last, tx_last, ka_last, rtsp_last].into_iter().max().unwrap_or(0);
        DiagSnapshot {
            t_ms: self.now_ms(),
            rx_video_bytes: self.rx_video_bytes.load(Ordering::Relaxed),
            rx_video_frames: self.rx_video_frames.load(Ordering::Relaxed),
            tx_video_frames: self.tx_video_frames.load(Ordering::Relaxed),
            tx_video_bytes: self.tx_video_bytes.load(Ordering::Relaxed),
            tx_keepalives: self.tx_keepalives.load(Ordering::Relaxed),
            rtsp_events: self.rtsp_events.load(Ordering::Relaxed),
            session_active: self.session_active.load(Ordering::Relaxed),
            rx_last_ms: rx_last,
            tx_last_ms: tx_last,
            keepalive_last_ms: ka_last,
            rtsp_last_ms: rtsp_last,
            last_any_ms: last_any,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DiagSnapshot {
    pub t_ms: u32,
    pub rx_video_bytes: u32,
    pub rx_video_frames: u32,
    pub tx_video_frames: u32,
    pub tx_video_bytes: u32,
    pub tx_keepalives: u32,
    pub rtsp_events: u32,
    pub session_active: bool,
    pub rx_last_ms: u32,
    pub tx_last_ms: u32,
    pub keepalive_last_ms: u32,
    pub rtsp_last_ms: u32,
    pub last_any_ms: u32,
}

impl DiagSnapshot {
    /// Milliseconds since any pipeline stage made progress (0 = no progress
    /// ever).
    pub fn idle_ms(&self) -> u32 {
        if self.last_any_ms == 0 {
            0
        } else {
            self.t_ms.saturating_sub(self.last_any_ms)
        }
    }
}
