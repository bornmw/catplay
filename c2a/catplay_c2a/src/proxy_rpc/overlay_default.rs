use crate::{
    proxy_rpc::{OverlayPeerState, OverlayPolicy},
    ui::UiState,
};
use catplay_bt::BluezManager;
use catplay_util::{EventReconciler, EventSleeper, EventToken, LazyAsync};
use log::{debug, info, warn};
use std::time::{Duration, Instant};

pub struct OverlayPolicyDefault {
    bluez: LazyAsync<BluezManager>,
    peer: OverlayPeerState,
    overlay: Option<UiState>,
    /// One-shot guard so the pairing overlay appears only once in the log.
    pairing_shown: bool,
    /// Last time we logged a BT_PAIR_STATE summary (once per second while pending).
    last_state_log: Option<Instant>,
}

impl OverlayPolicyDefault {
    pub fn new() -> Self {
        Self {
            bluez: LazyAsync::new(Self::start_pin_agent),
            peer: OverlayPeerState::Unconnected,
            overlay: None,
            pairing_shown: false,
            last_state_log: None,
        }
    }
    async fn start_pin_agent() -> BluezManager {
        loop {
            let mut mgr = BluezManager::new();
            match mgr.register_pin_agent().await {
                Ok(()) => {
                    if let Err(err) = mgr.set_discoverable("hci0", true).await {
                        warn!("Failed to set BlueZ adapter discoverable: {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    if let Err(err) = mgr.set_pairable("hci0", true).await {
                        warn!("Failed to set BlueZ adapter pairable: {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    return mgr;
                }
                Err(err) => {
                    debug!("BlueZ agent not available: {err}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

impl OverlayPolicy for OverlayPolicyDefault {
    fn overlay(&mut self) -> Option<UiState> {
        if self.peer != OverlayPeerState::Unconnected {
            return None;
        }

        if let Some(req) = self
            .bluez
            .as_ref()
            .as_ref()
            .and_then(|bluez| bluez.get_pairing_request())
        {
            if !self.pairing_shown {
                self.pairing_shown = true;
                info!(
                    "BT_PAIR_OVERLAY_SHOWN remote_mac={} remote_name={:?} passkey={:06} — pairing overlay (cat+code screen) now visible on the car display",
                    req.remote_mac, req.remote_name, req.passkey
                );
            }

            // Periodic summary while the request is pending (once per second).
            // debug!: pairing is proven working (fix9); kept for regression
            // diagnosis, invisible at the release info ceiling.
            let now = Instant::now();
            if self
                .last_state_log
                .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1))
            {
                self.last_state_log = Some(now);
                debug!(
                    "BT_PAIR_STATE pending=true remote_mac={} passkey={:06} age_ms={} overlay_visible=true — pairing request still pending (car touch + iPhone 'Pair' both required)",
                    req.remote_mac,
                    req.passkey,
                    req.created.elapsed().as_millis()
                );
            }

            return Some(UiState::Pairing {
                device: req.remote_name.unwrap_or("unknown".into()),
                pin: format!("{:06}", req.passkey),
            });
        }

        self.pairing_shown = false;
        self.last_state_log = None;

        // return Some(UiState::Connecting {
        //     device: "iPhone".into(),
        //     ticks: 10,
        // });

        Some(UiState::WaitingForConnection)
    }

    fn on_hid_interact(&mut self) -> bool {
        if let Some(req) = self
            .bluez
            .as_ref()
            .as_ref()
            .and_then(|bluez| bluez.get_pairing_request())
        {
            info!(
                "PAIRING_OVERLAY_INTERACT remote_mac={} passkey={:06} age_ms={} — car HID interact: accepting the pending pairing request (dongle side; iPhone must also confirm)",
                req.remote_mac,
                req.passkey,
                req.created.elapsed().as_millis()
            );
            req.accept();
            return false;
        }

        info!("PAIRING_OVERLAY_INTERACT — car HID interact with no pending pairing request");
        true
    }

    fn set_peer_state(&mut self, state: OverlayPeerState) {
        self.peer = state;
    }
}

impl EventReconciler for OverlayPolicyDefault {
    type Error = ();

    async fn reconcile(&mut self) -> Result<(), Self::Error> {
        self.overlay = self.overlay();
        Ok(())
    }
}

impl EventSleeper for OverlayPolicyDefault {
    async fn sleep(&mut self) -> Option<EventToken> {
        self.bluez.sleep().await
    }
}
