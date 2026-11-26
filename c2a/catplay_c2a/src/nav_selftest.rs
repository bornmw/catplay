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
//! | 5–20  | A        | 0x5202 highway exit ("I-87 North", exit_info "Exit 30", 3200 m) + active 0x5201 @ 1 Hz; HUD combo (`nav_combo`) re-sent as 0x5202 at 5 s cadence with decreasing distance ("Ext 30  3.0 km" → "Ext 30  2.7 km") |
//! | 20–45 | B        | 0x5202 urban ("Elm Street", NO exit_info) — street name must stay; 0x5201 @ 1 Hz, distances decreasing at 5 m/s |
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
/// DATABASE-PROBE (temporary): a FRESH maneuver index so the MIB3 cannot
/// be showing a cached "I-87 North" from a prior run. Pair with
/// `maneuver_highway()`'s non-road string "Exit 30". Revert after probe.
const DB_PROBE_INDEX: u16 = 7;

/// Spawn the command-file poller. Lives for the process lifetime.
pub fn spawn(outbox: Arc<NavOutbox>) {
    tokio::spawn(async move {
        info!("RG_POLLER_READY: path={CMD_PATH} poll_ms={}", POLL_INTERVAL.as_millis());
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
                        warn!("RG_TEST_BUSY: start ignored — self-test already running");
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
    // M1: refuse to feed the car packets that violate its advertised
    // display caps (the caller logs the reason as RG_TEST_FAIL).
    validate_caps()?;

    // --- validate: wait for an active TX session to publish its handle
    // (the CSM session's start() does that once per iAP2 pipe), then for
    // the car's route-guidance subscription (0x5200, arrives after
    // identification). 10 s total, distinct failure reasons.
    let deadline = Instant::now() + Duration::from_secs(10);
    let generation = loop {
        let (h, g, subscribed) = outbox.snapshot();
        if h.is_some() && subscribed {
            break g;
        }
        if Instant::now() >= deadline {
            return if h.is_none() {
                Err("no active TX session within 10 s (CarPlay not connected?)".to_string())
            } else {
                Err("car did not subscribe to route guidance (no 0x5200) within 10 s".to_string())
            };
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    info!("RG_TEST_START: gen={generation} subscribed=true — stage A in 5 s");

    let t0 = Instant::now();
    let mut stage_a = false;
    let mut stage_b = false;
    let mut stage_c = false;
    let mut enqueued = 0u32;
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let elapsed = t0.elapsed().as_secs() as u32;

        if elapsed >= RUN_SECS || stop.load(Ordering::Relaxed) {
            // Clean stop: inactive 0x5201, but only to the same live
            // session — never across a session boundary.
            let reason = if elapsed >= RUN_SECS { "timeline complete" } else { "stop requested" };
            match outbox.send_if_current(generation, &inactive_update()) {
                Ok(()) => {
                    enqueued += 1;
                    info!("RG_TEST_STOP: {reason} — inactive route state enqueued, clean stop");
                }
                Err(_) => info!("RG_TEST_STOP: {reason} — session gone, cleanup skipped"),
            }
            info!(
                "RG_TEST_DONE: gen={generation} stage_a={stage_a} stage_b={stage_b} stage_c={stage_c} packets_enqueued={enqueued}"
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
                // Highway exit stage: the HUD combo (production
                // `nav_combo` format) replaces the street name and
                // refreshes on a 5 s cadence with decreasing distance.
                let e = elapsed.saturating_sub(5);
                if !stage_a {
                    enqueue(&outbox, generation, &maneuver_highway(), &mut enqueued)?;
                    stage_a = true;
                    info!("RG_TEST_STAGE_A: 0x5202 highway exit ('I-87 North', exit_info 'Exit 30', 3200 m)");
                }
                // 50 m/s synthetic approach; combo ticks at e=5 and e=10.
                let next_m = 3200u32.saturating_sub(e * 50);
                if e > 0 && e % 5 == 0 {
                    match crate::nav_combo::format_combo("Exit 30", next_m) {
                        Some(combo) => {
                            enqueue(&outbox, generation, &maneuver_combo(combo.clone()), &mut enqueued)?;
                            info!("RG_TEST_COMBO: '{combo}' (meters={next_m})");
                        }
                        None => warn!("RG_TEST_COMBO: combo does not fit at {next_m} m — skipped"),
                    }
                }
                let dest_m = 8000u32.saturating_sub(e * 50);
                let time_s = 900u64.saturating_sub(e as u64);
                enqueue(
                    &outbox,
                    generation,
                    &active_update(next_m, dest_m, time_s, false, DB_PROBE_INDEX),
                    &mut enqueued,
                )?;
            }
            2 => {
                // Urban stage: NO exit_info — the street name must stay.
                let e = elapsed.saturating_sub(20);
                if !stage_b {
                    enqueue(&outbox, generation, &maneuver(false), &mut enqueued)?;
                    stage_b = true;
                    info!("RG_TEST_STAGE_B: 0x5202 urban ('Elm Street', no exit_info)");
                }
                // 5 m/s: at e=14 (last B tick) next=380, dest=7930, time=886
                let next_m = 500u32.saturating_sub(e * 5);
                let dest_m = 8000u32.saturating_sub(e * 5);
                let time_s = 900u64.saturating_sub(e as u64);
                enqueue(
                    &outbox,
                    generation,
                    &active_update(next_m, dest_m, time_s, false, MANEUVER_INDEX),
                    &mut enqueued,
                )?;
            }
            3 => {
                if !stage_c {
                    enqueue(&outbox, generation, &maneuver(true), &mut enqueued)?;
                    enqueue(&outbox, generation, &lanes(), &mut enqueued)?;
                    stage_c = true;
                    info!("RG_TEST_STAGE_C: 0x5202 exit angle -90 + 0x5204 enqueued");
                }
                let e = elapsed.saturating_sub(45);
                // Continues from B's last tick (next=380).
                let next_m = 380u32.saturating_sub(e * 5);
                let dest_m = 7930u32.saturating_sub(e * 5);
                let time_s = 886u64.saturating_sub(e as u64);
                enqueue(
                    &outbox,
                    generation,
                    &active_update(next_m, dest_m, time_s, true, MANEUVER_INDEX),
                    &mut enqueued,
                )?;
            }
            _ => {} // 0–5 s: validate, quiet
        }
    }
}

/// Generation-checked enqueue (atomic with the validation inside the
/// outbox). "Enqueued" is deliberate: this proves acceptance into the
/// session's outbound queue, not delivery on the wire.
fn enqueue(outbox: &NavOutbox, generation: u32, packet: &dyn AsCsmPacket, enqueued: &mut u32) -> Result<(), String> {
    outbox
        .send_if_current(generation, packet)
        .map(|()| *enqueued += 1)
}

// --- cap validation (M1) --------------------------------------------------

/// A6 C8 (MIB3P) advertised display caps — a contract, not a presentation
/// hint (see RG_CAPABILITY_AUDIT in the logs). A native renderer may
/// silently discard an update that exceeds them.
const A6_MAX_CURRENT_ROAD: usize = 6;
const A6_MAX_DESTINATION: usize = 6;
const A6_MAX_AFTER_MANEUVER_ROAD: usize = 20;

/// Pre-send validator: every synthetic packet variant used by the test
/// must satisfy the A6's advertised caps and the internal index
/// contracts (0x5201 current-list vs 0x5202 index; 0x5201 lane fields vs
/// the 0x5204 block). Fails the run (RG_TEST_FAIL) with the offending
/// field.
fn validate_caps() -> Result<(), String> {
    // 0x5201 active — both lane modes (Stage C uses lanes=true).
    for lanes in [false, true] {
        let u = active_update(500, 8000, 900, lanes, MANEUVER_INDEX);
        if let Some(s) = &u.current_road_name {
            if s.chars().count() > A6_MAX_CURRENT_ROAD {
                return Err(format!("current_road_name '{s}' exceeds A6 cap {A6_MAX_CURRENT_ROAD}"));
            }
        }
        if let Some(s) = &u.destination_name {
            if s.chars().count() > A6_MAX_DESTINATION {
                return Err(format!("destination_name '{s}' exceeds A6 cap {A6_MAX_DESTINATION}"));
            }
        }
        // The 0x5201 current-maneuver reference must match the 0x5202 index.
        match &u.route_guidance_maneuver_current_list {
            Some(list) if list.0 == [MANEUVER_INDEX] => {}
            _ => return Err("0x5201 current_list must reference MANEUVER_INDEX".into()),
        }
        if lanes {
            match (&u.lane_guidance_current_index, &u.lane_guidance_total_count) {
                (Some(0), Some(1)) => {}
                _ => return Err("0x5201 lane index/count inconsistent with the 0x5204 block".into()),
            }
        }
    }
    // 0x5201 inactive (clean stop) — no names, trivially within caps.
    let inactive = inactive_update();
    if inactive.current_road_name.is_some() || inactive.destination_name.is_some() {
        return Err("inactive 0x5201 must carry no road/destination names".into());
    }
    // 0x5202 — all stage variants (highway raw, combo refreshes, urban).
    for exit_angle in [false, true] {
        let m = maneuver(exit_angle);
        check_maneuver_caps(&m)?;
        // A6 caps maneuver_description at 0: the field must be absent.
        if m.maneuver_description.is_some() {
            return Err("maneuver_description set although A6 cap is 0".into());
        }
        if m.index != MANEUVER_INDEX {
            return Err(format!("0x5202 index {} != MANEUVER_INDEX", m.index));
        }
    }
    let highway = maneuver_highway();
    check_maneuver_caps(&highway)?;
    if highway.index != DB_PROBE_INDEX {
        return Err(format!("0x5202 index {} != DB_PROBE_INDEX", highway.index));
    }
    // The production combo strings for the exact Stage A timeline values
    // must exist and fit the A6 cap (nav_combo guarantees the fit, but
    // verify the real numbers end-to-end).
    for e in [5u32, 10] {
        let meters = 3200u32.saturating_sub(e * 50);
        match crate::nav_combo::format_combo("Exit 30", meters) {
            Some(combo) if combo.chars().count() <= A6_MAX_AFTER_MANEUVER_ROAD => {}
            Some(combo) => return Err(format!("combo '{combo}' ({meters} m) exceeds A6 cap {A6_MAX_AFTER_MANEUVER_ROAD}")),
            None => return Err(format!("combo does not fit at {meters} m — Stage A would lose the exit")),
        }
    }
    // 0x5204 — A6 caps lane description at 0: the field must be absent.
    let l = lanes();
    if l.lane_guidance_description.is_some() {
        return Err("lane_guidance_description set although A6 cap is 0".into());
    }
    if l.lane_guidance_index != 0 {
        return Err(format!("0x5204 index {} != 0 (0x5201 references 0)", l.lane_guidance_index));
    }
    Ok(())
}

/// Shared 0x5202 cap check (after_maneuver_road_name <= 20).
fn check_maneuver_caps(m: &RouteGuidanceManeuverInformation) -> Result<(), String> {
    if let Some(s) = &m.after_maneuver_road_name {
        if s.chars().count() > A6_MAX_AFTER_MANEUVER_ROAD {
            return Err(format!("after_maneuver_road_name '{s}' exceeds A6 cap {A6_MAX_AFTER_MANEUVER_ROAD}"));
        }
    }
    Ok(())
}

// --- packet builders -----------------------------------------------------

/// Active route, 0x5201. Km units (A6 is de-locale). `index` is the
/// current-maneuver reference (must match the 0x5202 index being sent).
fn active_update(next_m: u32, dest_m: u32, time_s: u64, lanes: bool, index: u16) -> RouteGuidanceUpdate {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    RouteGuidanceUpdate {
        route_guidance_display_component_id: vec![COMPONENT_ID],
        route_guidance_state: Some(RouteGuidanceState::RouteSetARouteIsSet),
        maneuver_state: Some(ManeuverState::ContinueContinueAlongThisRouteUntilNextManeuver),
        // Within the A6's advertised caps (6/6 chars): a strict native
        // renderer may silently drop updates that exceed them (M1).
        current_road_name: Some("Main".into()),
        destination_name: Some("Test".into()),
        estimated_time_of_arrival: Some(now.saturating_add(time_s)),
        time_remaining_to_destination: Some(time_s),
        distance_remaining: Some(dest_m),
        distance_remaining_display_string: Some(km_string(dest_m)),
        distance_remaining_display_units: Some(DistanceDisplayUnits::Km),
        distance_remaining_to_next_maneuver: Some(next_m),
        distance_remaining_to_next_maneuver_display_string: Some(next_string(next_m)),
        distance_remaining_to_next_maneuver_display_units: Some(DistanceDisplayUnits::Km),
        route_guidance_maneuver_current_list: Some(CsmPacked::from(vec![index])),
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

/// Urban maneuver, 0x5202 (Stages B/C): street name, NO exit_info —
/// the HUD must keep showing the street name. Stage C adds the exit
/// angle (−90 = left).
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

/// Highway maneuver, 0x5202 (Stage A start): what a real iPhone sends —
/// exit_info as an opaque pre-formatted label, street name intact. The
/// combo replaces the street name on the follow-up refreshes.
///
/// DATABASE-PROBE (temporary): after_maneuver_road_name is "Exit 30"
/// (NOT a road name) on a FRESH index, to test whether the MIB3 only
/// renders road names from its own map database. If "Exit 30" renders,
/// the database hypothesis is disproven and the blocker is the
/// same-index no-repaint; if it does not, the MIB3 filters non-database
/// strings. Revert to "I-87 North" + MANEUVER_INDEX after the probe.
fn maneuver_highway() -> RouteGuidanceManeuverInformation {
    RouteGuidanceManeuverInformation {
        route_guidance_display_component_id: COMPONENT_ID,
        index: DB_PROBE_INDEX,
        maneuver_type: Some(ManeuverType::LeftTurnAngleIsBetween45And135Degrees),
        after_maneuver_road_name: Some("Exit 30".into()),
        distance_between_maneuver: Some(3200),
        distance_between_maneuver_display_string: Some("3.2".into()),
        distance_between_maneuver_display_units: Some(DistanceDisplayUnits::Km),
        driving_side: Some(DrivingSide::RightOrAntiClockwiseForRoundabouts),
        junction_type: Some(JunctionType::Value0),
        exit_info: Some("Exit 30".into()),
        ..Default::default()
    }
}

/// Highway maneuver with the HUD combo in after_maneuver_road_name
/// (Stage A 5 s refreshes) — the production `nav_combo` format.
fn maneuver_combo(combo: String) -> RouteGuidanceManeuverInformation {
    RouteGuidanceManeuverInformation {
        after_maneuver_road_name: Some(combo),
        ..maneuver_highway()
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
