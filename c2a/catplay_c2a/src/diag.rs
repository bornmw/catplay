//! HEALTH line + hang detection for the pipeline progress counters
//! (DIAGNOSTIC, fix10 — see `catplay_util::diag`).
//!
//! Runs on the app's own `current_thread` tokio runtime: if the runtime
//! starves (deadlock / blocked task), this task stops too — the absence of
//! `HEALTH` lines IS the signal (the independent kernel sampler keeps
//! running in a separate process).

use std::time::Duration;

use catplay_util::diag::{diag, DiagSnapshot};
use log::{info, warn};

/// No pipeline progress for this long while a session is active => HANG.
/// Must exceed the iPhone's VFR quiet periods (static screen: ~10s between
/// trickle frames) — the car-side keepalives (1/s) keep `last_any` fresh as
/// long as the TX session is alive, so this mainly catches total stalls.
const HANG_IDLE_MS: u32 = 15_000;
/// Re-report a continuing hang at most this often.
const HANG_REARM_MS: u32 = 15_000;

pub fn spawn_health_monitor() {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        let mut last_hang_ms: u32 = 0;
        loop {
            ticker.tick().await;
            let snap: DiagSnapshot = diag().snapshot();
            info!(
                "HEALTH t={}s active={} rx_b={} rx_f={} tx_f={} tx_b={} ka={} rtsp={} idle_ms={}",
                snap.t_ms / 1000,
                snap.session_active,
                snap.rx_video_bytes,
                snap.rx_video_frames,
                snap.tx_video_frames,
                snap.tx_video_bytes,
                snap.tx_keepalives,
                snap.rtsp_events,
                snap.idle_ms()
            );
            if snap.session_active
                && snap.idle_ms() >= HANG_IDLE_MS
                && snap.t_ms.saturating_sub(last_hang_ms) >= HANG_REARM_MS
            {
                warn!(
                    "HANG_DETECTED no pipeline progress for {}ms while session active — rx_b={} rx_f={} tx_f={} tx_b={} ka={} rtsp={} rx_age={}ms tx_age={}ms ka_age={}ms rtsp_age={}ms",
                    snap.idle_ms(),
                    snap.rx_video_bytes,
                    snap.rx_video_frames,
                    snap.tx_video_frames,
                    snap.tx_video_bytes,
                    snap.tx_keepalives,
                    snap.rtsp_events,
                    snap.t_ms.saturating_sub(snap.rx_last_ms),
                    snap.t_ms.saturating_sub(snap.tx_last_ms),
                    snap.t_ms.saturating_sub(snap.keepalive_last_ms),
                    snap.t_ms.saturating_sub(snap.rtsp_last_ms)
                );
                last_hang_ms = snap.t_ms;
            }
        }
    });
}
