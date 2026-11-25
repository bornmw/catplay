//! Built-in route-guidance self-test (M0).
//!
//! SSH-triggered via the command file `/run/catplay/navtest.cmd`
//! (`"start"` / `"stop"`, polled every 250 ms — no listener, no CLI):
//!
//! ```sh
//! ssh dongle 'printf start > /run/catplay/navtest.cmd'
//! ssh dongle 'printf stop  > /run/catplay/navtest.cmd'
//! ```
//!
//! Runs a 60 s staged synthetic route through the live TX CSM session
//! (typed packets through the normal serializer — never raw bytes):
//!
//! | t (s) | stage    | what is sent |
//! |-------|----------|--------------|
//! | 0–5   | validate | active TX session + stable generation required |
//! | 5–20  | A        | one 0x5202 (left turn, "Elm Street") + active 0x5201 @ 1 Hz, 500 m held |
//! | 20–45 | B        | 0x5201 @ 1 Hz, distances decreasing at 5 m/s |
//! | 45–60 | C        | 0x5202 with exit angle −90 + 0x5204 three-lane block, 0x5201 continues |
//! | 60    | stop     | inactive 0x5201 (`NoRouteSet`) — never 0x5203 (accessory-originated) |
//!
//! Safety: parked-by-operator discipline (no trustworthy parked signal
//! exists to enforce it); generation-bound — the test aborts *without
//! sending* when the session goes away; every send re-checks the
//! generation.
//!
//! Component ID is 1 — the A6 MIB3's declared
//! `RouteGuidanceDisplayComponent` id (its 0x5200 targets it). Our TX
//! identification declares no components this iteration; if Stage A shows
//! nothing, advertising the ID is the fallback.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use catplay_carplay_tx_gadget::nav_outbox::NavOutbox;
use catplay_csm::decoder::{AsCsmPacket, CsmPacked};
use catplay_csm::msg::{
    DrivingSide, DistanceDisplayUnits, JunctionType, LaneGuidanceInformation, LaneInformation,
    LaneInformationStatus, ManeuverState, ManeuverType, RouteGuidanceManeuverInformation,
    RouteGuidanceState, RouteGuidanceUpdate,
};
use catplay_util::spawn_blocking;
use log::{info, warn};

const CMD_PATH: &str = "/run/catplay/navtest.cmd";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const TICK: Duration = Duration::from_secs(1);
/// Total timeline length.
const RUN_SECS: u32 = 60;
/// A6 MIB3 declared RouteGuidanceDisplayComponent id.
const COMPONENT_ID: u16 = 1;
const MANEUVER_INDEX: u16 = 0;

/// Spawn the command-file poller. Lives for the process lifetime.
pub fn spawn(outbox: Arc<NavOutbox>) {
    tokio::spawn(async move {
        let stop = Arc::new(AtomicBool::new(false));
        let mut running: Option<tokio::task::JoinHandle<()>> = None;
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            // Reap a finished run.
            if let Some(handle) = running.take() {
                if handle.is_finished() {
                    let _ = handle.await;
                    info!("RG_TEST_IDLE: self-test finished, waiting for next command");
                } else {
                    running.replace(handle);
                }
            }

            let Some(cmd) = read_command() else {
                continue;
            };
            match cmd.as_str() {
                "start" => {
                    if running.is_some() {
                        info!("RG_TEST_CMD: start ignored — self-test already running");
                    } else {
                        stop.store(false, Ordering::Relaxed);
                        info!("RG_TEST_CMD: start");
                        let outbox = outbox.clone();
                        let stop = stop.clone();
                        running.replace(tokio::spawn(async move {
                            if let Err(err) = run_selftest(outbox, stop).await {
                                warn!("RG_TEST_FAIL: {err}");
                            }
                        }));
                    }
                }
                "stop" => {
                    info!("RG_TEST_CMD: stop");
                    stop.store(true, Ordering::Relaxed);
                }
                other => warn!("RG_TEST_CMD: unrecognized command file content {other:?}"),
            }
            remove_command().await;
        }
    });
}

fn read_command() -> Option<String> {
    std::fs::read_to_string(CMD_PATH)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

async fn remove_command() {
    let _ = spawn_blocking(|| std::fs::remove_file(CMD_PATH)).await;
}

/// The 60 s staged timeline. Returns an error string when the run is
/// aborted for an operator-visible reason (logged by the caller).
async fn run_selftest(outbox: Arc<NavOutbox>, stop: Arc<AtomicBool>) -> Result<(), String> {
    // --- validate (0–5 s): wait for an active TX session to publish its
    // handle (the CSM session's start() does that once per iAP2 pipe) ---
    let deadline = Instant::now() + Duration::from_secs(5);
    let (handle, generation) = loop {
        let (h, g) = outbox.snapshot();
        if let Some(h) = h {
            break (h, g);
        }
        if Instant::now() >= deadline {
            return Err("no active TX session within 5 s (CarPlay not connected?)".to_string());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    info!("RG_TEST_START: gen={generation} — stage A in 5 s");

    let t0 = Instant::now();
    let mut stage_a_sent = false;
    let mut stage_c_sent = false;
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let elapsed = t0.elapsed().as_secs() as u32;

        if elapsed >= RUN_SECS || stop.load(Ordering::Relaxed) {
            send(&outbox, generation, &inactive_update()).await?;
            info!(
                "RG_TEST_STOP: {} — inactive route state sent, clean stop",
                if elapsed >= RUN_SECS { "timeline complete" } else { "stop requested" }
            );
            return Ok(());
        }

        // Generation changed ⇒ the session we captured is gone. Do NOT
        // send on a dead handle — just stop.
        if outbox.snapshot().1 != generation {
            info!("RG_TEST_ABORT: session generation changed (gen {generation}) — stopping without sending");
            return Ok(());
        }

        let stage = elapsed / 15; // 0: validate, 1: A, 2: B, 3: C
        match stage {
            1 => {
                if !stage_a_sent {
                    send(&outbox, generation, &maneuver(false)).await?;
                    stage_a_sent = true;
                    info!("RG_TEST_STAGE_A: 0x5202 sent (left turn, 'Elm Street')");
                }
                send(&outbox, generation, &active_update(500, 8000, 900, false)).await?;
            }
            2 => {
                let e = elapsed.saturating_sub(20);
                // 5 m/s: at e=24 (last B tick) next=380, dest=7680, time=876
                let next_m = 500u32.saturating_sub(e * 5);
                let dest_m = 8000u32.saturating_sub(e * 5);
                let time_s = 900u64.saturating_sub(e as u64);
                send(&outbox, generation, &active_update(next_m, dest_m, time_s, false)).await?;
            }
            3 => {
                if !stage_c_sent {
                    send(&outbox, generation, &maneuver(true)).await?;
                    send(&outbox, generation, &lanes()).await?;
                    stage_c_sent = true;
                    info!("RG_TEST_STAGE_C: 0x5202 exit angle -90 + 0x5204 sent");
                }
                let e = elapsed.saturating_sub(45);
                // Continues from B's last tick (next=380).
                let next_m = 380u32.saturating_sub(e * 5);
                let dest_m = 7680u32.saturating_sub(e * 5);
                let time_s = 876u64.saturating_sub(e as u64);
                send(&outbox, generation, &active_update(next_m, dest_m, time_s, true)).await?;
            }
            _ => {} // 0–5 s: validate, quiet
        }
    }
}

/// Generation-checked send. Fails the run if the session went away since
/// the snapshot was taken — never sends on a handle from a dead session.
async fn send(outbox: &NavOutbox, generation: u32, packet: &dyn AsCsmPacket) -> Result<(), String> {
    let (h, cur) = outbox.snapshot();
    if cur != generation {
        return Err(format!("generation changed ({generation} -> {cur}) before send"));
    }
    let h = h.ok_or_else(|| "handle disappeared before send".to_string())?;
    h.send(packet)
        .map_err(|e| format!("send failed: {e:?}"))
}

// --- packet builders -----------------------------------------------------

/// Active route, 0x5201. Km units (A6 is de-locale).
fn active_update(next_m: u32, dest_m: u32, time_s: u64, lanes: bool) -> RouteGuidanceUpdate {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    RouteGuidanceUpdate {
        route_guidance_display_component_id: vec![COMPONENT_ID],
        route_guidance_state: Some(RouteGuidanceState::RouteSetARouteIsSet),
        maneuver_state: Some(ManeuverState::ContinueContinueAlongThisRouteUntilNextManeuver),
        current_road_name: Some("Main St".into()),
        destination_name: Some("Test Dest".into()),
        estimated_time_of_arrival: Some(now.saturating_add(time_s)),
        time_remaining_to_destination: Some(time_s),
        distance_remaining: Some(dest_m),
        distance_remaining_display_string: Some(km_string(dest_m)),
        distance_remaining_display_units: Some(DistanceDisplayUnits::Km),
        distance_remaining_to_next_maneuver: Some(next_m),
        distance_remaining_to_next_maneuver_display_string: Some(next_string(next_m)),
        distance_remaining_to_next_maneuver_display_units: Some(DistanceDisplayUnits::Km),
        route_guidance_maneuver_current_list: Some(CsmPacked::from(vec![MANEUVER_INDEX])),
        route_guidance_maneuver_total_count: Some(1),
        route_guidance_visible_in_app: Some(true),
        present_lane_guidance: Some(lanes),
        lane_guidance_current_index: lanes.then_some(0),
        lane_guidance_total_count: lanes.then_some(1),
        source_name: Some("CatPlay Test".into()),
        source_supports_route_guidance: Some(true),
        ..Default::default()
    }
}

/// Inactive route, 0x5201 — the clean stop.
fn inactive_update() -> RouteGuidanceUpdate {
    RouteGuidanceUpdate {
        route_guidance_display_component_id: vec![COMPONENT_ID],
        route_guidance_state: Some(RouteGuidanceState::NoRouteSetARouteIsNotSet),
        ..Default::default()
    }
}

/// Maneuver, 0x5202. Stage C adds the exit angle (−90 = left).
fn maneuver(exit_angle: bool) -> RouteGuidanceManeuverInformation {
    RouteGuidanceManeuverInformation {
        route_guidance_display_component_id: COMPONENT_ID,
        index: MANEUVER_INDEX,
        maneuver_type: Some(ManeuverType::LeftTurnAngleIsBetween45And135Degrees),
        after_maneuver_road_name: Some("Elm Street".into()),
        distance_between_maneuver: Some(500),
        distance_between_maneuver_display_string: Some("0.5".into()),
        distance_between_maneuver_display_units: Some(DistanceDisplayUnits::Km),
        driving_side: Some(DrivingSide::RightOrAntiClockwiseForRoundabouts),
        junction_type: Some(JunctionType::Value0),
        junction_element_exit_angle: exit_angle.then_some(-90),
        ..Default::default()
    }
}

/// Three lanes, leftmost preferred, 0x5204.
fn lanes() -> LaneGuidanceInformation {
    LaneGuidanceInformation {
        route_guidance_display_component_id: COMPONENT_ID,
        lane_guidance_index: 0,
        lane_information: vec![
            LaneInformation {
                index: 0,
                lane_status: LaneInformationStatus::Value2,
                lane_angle: vec![],
                lane_angle_highlight: None,
            },
            LaneInformation {
                index: 1,
                lane_status: LaneInformationStatus::NotGoodTheVehicleShouldNotTakeThisLane,
                lane_angle: vec![],
                lane_angle_highlight: None,
            },
            LaneInformation {
                index: 2,
                lane_status: LaneInformationStatus::NotGoodTheVehicleShouldNotTakeThisLane,
                lane_angle: vec![],
                lane_angle_highlight: None,
            },
        ],
        ..Default::default()
    }
}

// --- display strings ------------------------------------------------------

/// Sub-kilometer distances get one decimal ("0.5"); whole kilometers after.
fn next_string(m: u32) -> String {
    if m < 1000 {
        format!("{:.1}", m as f32 / 1000.0)
    } else {
        format!("{}", m / 1000)
    }
}

fn km_string(m: u32) -> String {
    format!("{}", m / 1000)
}
