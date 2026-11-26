//! Live route-guidance bridge (Phase 1): iPhone 0x52xx (RX side,
//! dongle-as-car) -> A6 (TX side, dongle-as-iPhone).
//!
//! Single consumer task; owns all route state (single-threaded runtime,
//! no locking beyond the outbox's own state mutex).
//!
//! HUD combo (the project goal): when the current maneuver carries
//! `exit_info`, `after_maneuver_road_name` is replaced with
//! "Ext 30  3.2 km" (see [`crate::nav_combo`]) — the one free-form text
//! field the A6 HUD renders. The distance refreshes on a 5 s cadence
//! (and immediately when a new 0x5202 arrives, per the protocol review).
//!
//! Invariants (per the ChatGPT-approved plan + pre-flash review fixes):
//! - Transform 0x5202 at forward time, not only on the 5 s tick.
//! - Order-2 safe: a 0x5202 that arrives BEFORE the 0x5201 making its
//!   index current is cached and emitted immediately when that 0x5201
//!   arrives (`last_sent_index` tracking).
//! - A combo is only attached when the distance sample MATCHES the
//!   maneuver (same index, same route epoch, < 3 s old). A redefinition
//!   of the current maneuver (same-index reroute) invalidates the
//!   sample until the next 0x5201 reconfirms it.
//! - Only the CURRENT maneuver index is forwarded to the A6 (its
//!   maneuver storage is 1); upcoming indexes are cached.
//! - 0x5203 is NEVER forwarded toward the A6 (accessory-originated);
//!   route end => clean-stop inactive 0x5201 (proven in the M1 self-test).
//! - RX and TX component ids are mapped by explicit constants, not by
//!   assuming equality.
//! - Every forwarded packet is enforced against the A6's advertised
//!   caps (current_road 6, destination 6, after_maneuver 20,
//!   lane_desc 0 -> omit).
//! - A TX session boundary (outbox generation change) drops only the
//!   TX-side send markers; the RX route state survives and is replayed
//!   by the readiness-gate flush when the A6 re-subscribes.
//!
//! TX readiness gate: nothing is forwarded until the outbox reports a
//! live handle AND the A6's 0x5200 subscription; the latest bounded
//! state is held (one 0x5201 + maneuver cache + one distance sample)
//! and flushed when the gate opens.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use catplay_carplay_rx_gadget::RouteGuidanceEvent;
use catplay_carplay_tx_gadget::nav_outbox::NavOutbox;
use catplay_csm::msg::{
    LaneGuidanceInformation, RouteGuidanceManeuverInformation, RouteGuidanceState, RouteGuidanceUpdate,
};
use catplay_util::mpsc;
use futures::StreamExt;
use log::{debug, info};

use crate::nav_combo::format_combo;

/// Component id we declare to the iPhone (RX side, dongle-as-car).
const RX_COMPONENT_ID: u16 = 1;
/// Component id the A6 declared (TX side target, dongle-as-iPhone).
const TX_COMPONENT_ID: u16 = 1;

/// A6 C8 advertised display caps (RG_CAPABILITY_AUDIT) — a contract, not
/// a hint: a native renderer may silently drop updates that exceed them.
const A6_MAX_CURRENT_ROAD: usize = 6;
const A6_MAX_DESTINATION: usize = 6;
const A6_MAX_AFTER_MANEUVER: usize = 20;

/// HUD combo refresh cadence (the user's "distance changes every 5 s").
const COMBO_REFRESH: Duration = Duration::from_secs(5);
/// A distance sample older than this is stale: no combo, keep street name.
const DISTANCE_FRESH: Duration = Duration::from_secs(3);

/// Live distance sample from the latest 0x5201.
struct DistanceSample {
    meters: u32,
    /// First index of the 0x5201 current-maneuver list when sampled —
    /// the combo matching key (a sample is only valid for ITS maneuver).
    maneuver_index: u16,
    epoch: u32,
    at: Instant,
}

/// All route state. Bounded by design: one 0x5201, one distance sample,
/// one 0x5202 per maneuver index (evicted on route end / session change).
struct Guidance {
    /// Bumped on every inactive->active transition and every TX session
    /// boundary; distance samples from an older epoch never match.
    epoch: u32,
    active: bool,
    last_update: Option<RouteGuidanceUpdate>,
    distance: Option<DistanceSample>,
    maneuvers: BTreeMap<u16, RouteGuidanceManeuverInformation>,
    /// Last combo text sent to the A6, scoped to the route epoch that
    /// produced it (change detection: an unchanged combo is not
    /// re-sent — repeated 0x5202 repaints are the flicker risk we are
    /// not allowed to take). A new epoch (new route) always re-sends.
    last_combo: Option<(u32, String)>,
    /// Last maneuver index whose 0x5202 the A6 actually received.
    /// Drives the Order-2 re-emit: when a 0x5201 makes an index current
    /// that the A6 has not received yet, the cached 0x5202 is emitted
    /// immediately (maneuver-before-distance arrival order).
    last_sent_index: Option<u16>,

    last_tx_gen: u32,
    tx_ready: bool,
}

impl Guidance {
    fn new() -> Self {
        Self {
            epoch: 0,
            active: false,
            last_update: None,
            distance: None,
            maneuvers: BTreeMap::new(),
            last_combo: None,
            last_sent_index: None,
            last_tx_gen: 0,
            tx_ready: false,
        }
    }

    /// First index of the latest 0x5201's current-maneuver list.
    fn current_index(&self) -> Option<u16> {
        self
            .last_update
            .as_ref()
            .and_then(|u| u.route_guidance_maneuver_current_list.as_ref())
            .and_then(|l| l.0.first().copied())
    }

    /// A distance sample is usable for `index` only when it is from the
    /// current epoch, sampled for the same maneuver, and fresh.
    fn fresh_distance(&self, index: u16) -> Option<&DistanceSample> {
        self
            .distance
            .as_ref()
            .filter(|d| d.epoch == self.epoch && d.maneuver_index == index && d.at.elapsed() < DISTANCE_FRESH)
    }

    fn clear(&mut self) {
        self.active = false;
        self.last_update = None;
        self.distance = None;
        self.maneuvers.clear();
        self.last_combo = None;
        self.last_sent_index = None;
    }

    /// Outbox generation tracking + TX readiness gate.
    ///
    /// A generation change is a TX session boundary (the A6's iAP2 pipe
    /// was replaced) — but the iPhone stream (RX side) is INDEPENDENT
    /// and still alive: the RX route state (maneuver cache, distance
    /// sample, active flag, epoch) is PRESERVED. Only the TX-side send
    /// markers are dropped; when the new A6 session re-subscribes,
    /// `flush_held` replays the current state (per pre-flash review:
    /// clearing the RX cache here left a reconnected A6 with distances
    /// but no HUD text until a brand-new 0x5202 arrived).
    fn sync_tx(&mut self, outbox: &NavOutbox) {
        let (handle, generation, subscribed) = outbox.snapshot();
        if generation != self.last_tx_gen {
            self.last_tx_gen = generation;
            self.last_combo = None;
            self.last_sent_index = None;
            self.tx_ready = false;
            return;
        }
        let ready = handle.is_some() && subscribed;
        if ready && !self.tx_ready {
            info!("RG_BRIDGE_READY: gen={generation} — A6 subscribed, flushing held state");
            self.flush_held(outbox);
        }
        self.tx_ready = ready;
    }

    /// Gate just opened: replay the latest held 0x5201, then the current
    /// maneuver (combo-transformed when a matching fresh distance exists).
    fn flush_held(&mut self, outbox: &NavOutbox) {
        if let Some(u) = self.last_update.clone() {
            let out = enforce_update_caps(&u);
            let _ = outbox.send_if_current(self.last_tx_gen, &out);
        }
        if let Some(index) = self.current_index() {
            if let Some(m) = self.maneuvers.get(&index).cloned() {
                self.emit_maneuver(&m, outbox);
            }
        }
    }

    /// Forward a (current) 0x5202 to the A6: combo-transformed when a
    /// matching fresh distance sample exists, plain otherwise. Updates
    /// the TX-side markers (`last_sent_index`, epoch-scoped
    /// `last_combo`) only on a generation-validated enqueue.
    fn emit_maneuver(&mut self, m: &RouteGuidanceManeuverInformation, outbox: &NavOutbox) {
        self.maneuvers.insert(m.index, m.clone());

        let out = match (m.exit_info.as_deref(), self.fresh_distance(m.index)) {
            (Some(_), Some(d)) => transform_maneuver(m, d.meters),
            _ => plain_maneuver(m),
        };
        match outbox.send_if_current(self.last_tx_gen, &out) {
            Ok(()) => {
                self.last_sent_index = Some(m.index);
                debug!("RG_FWD_5202: index={} after={:?}", m.index, out.after_maneuver_road_name);
                if out.after_maneuver_road_name != m.after_maneuver_road_name {
                    if let Some(text) = &out.after_maneuver_road_name {
                        self.last_combo = Some((self.epoch, text.clone()));
                        info!("RG_COMBO_SENT: '{text}' (transform at forward) index={}", m.index);
                    }
                }
            }
            Err(e) => debug!("RG_FWD_5202 failed: {e}"),
        }
    }

    fn on_update(&mut self, u: RouteGuidanceUpdate, outbox: &NavOutbox) {
        self.sync_tx(outbox);
        if let Some(first) = u.route_guidance_display_component_id.first() {
            check_rx_component_id("0x5201", *first);
        }

        match u.route_guidance_state {
            Some(RouteGuidanceState::NoRouteSetARouteIsNotSet)
            | Some(RouteGuidanceState::ArrivedArrivedAtTheDestination) => {
                if self.active {
                    info!("RG_ROUTE_END: route inactive/arrived — state cleared");
                }
                self.clear();
                if self.tx_ready {
                    // The proven clean stop: inactive 0x5201 (never 0x5203).
                    let stop = inactive_update();
                    match outbox.send_if_current(self.last_tx_gen, &stop) {
                        Ok(()) => debug!("RG_ROUTE_END: clean-stop 0x5201 sent"),
                        Err(e) => debug!("RG_ROUTE_END: clean-stop failed: {e}"),
                    }
                }
                return;
            }
            Some(RouteGuidanceState::RouteSetARouteIsSet) => {
                if !self.active {
                    self.active = true;
                    self.epoch += 1;
                    info!("RG_ACTIVE_ROUTE: iPhone route active (epoch {})", self.epoch);
                }
            }
            // Loading/Locating/Rerouting: keep state; the distance sample
            // ages out on its own (no stale combos).
            _ => {}
        }

        if let Some(meters) = u.distance_remaining_to_next_maneuver {
            let idx = current_index_of(&u).unwrap_or(0);
            self.distance = Some(DistanceSample {
                meters,
                maneuver_index: idx,
                epoch: self.epoch,
                at: Instant::now(),
            });
        }

        self.last_update = Some(u.clone());
        if !self.tx_ready {
            return; // held until the gate opens
        }
        let out = enforce_update_caps(&u);
        match outbox.send_if_current(self.last_tx_gen, &out) {
            Ok(()) => {
                // The full current list is logged (first element is the
                // HUD's next maneuver for a storage-1 device — verified
                // against the A6, not assumed).
                debug!(
                    "RG_FWD_5201: state={:?} current_list={:?} picked={:?} next_m={:?}",
                    u.route_guidance_state,
                    u.route_guidance_maneuver_current_list,
                    self.current_index(),
                    out.distance_remaining_to_next_maneuver
                );
            }
            Err(e) => debug!("RG_FWD_5201 failed: {e}"),
        }

        // Order-2 fix (pre-flash review P0): a 0x5202 may have arrived
        // BEFORE the 0x5201 that makes its index current (maneuver-
        // before-distance). It was cached, not forwarded. If this 0x5201
        // makes a cached index current that the A6 has not received,
        // emit it now — with the combo when the just-sampled distance
        // matches.
        if let Some(index) = self.current_index()
            && self.last_sent_index != Some(index)
            && self.maneuvers.contains_key(&index)
        {
            if let Some(m) = self.maneuvers.get(&index).cloned() {
                self.emit_maneuver(&m, outbox);
            }
        }
    }

    fn on_maneuver(&mut self, m: RouteGuidanceManeuverInformation, outbox: &NavOutbox) {
        self.sync_tx(outbox);
        check_rx_component_id("0x5202", m.route_guidance_display_component_id);

        // A REDEFINITION of the current maneuver (same index re-sent —
        // e.g. a same-index reroute without an inactive transition)
        // makes the distance association uncertain: drop the sample;
        // the next 0x5201 (<= 1 s) reconfirms it, and the 5 s tick
        // resumes the combo. A first store for the index keeps the
        // sample (the index-match rule already guards it). Applied
        // before the tx_ready gate so it holds even while the gate is
        // closed.
        if self.maneuvers.contains_key(&m.index) && self.current_index() == Some(m.index) {
            self.distance = None;
        }

        // The A6 stores ONE maneuver (storage capacity 1): forward only
        // the CURRENT index; upcoming indexes are cached and emitted when
        // a 0x5201 makes them current (Order-2 re-emit above).
        if !self.tx_ready || self.current_index() != Some(m.index) {
            self.maneuvers.insert(m.index, m);
            return;
        }
        info!("RG_ACTIVE_MANEUVER: index={} exit_info={:?}", m.index, m.exit_info);
        self.emit_maneuver(&m, outbox);
    }

    fn on_lanes(&mut self, l: LaneGuidanceInformation, outbox: &NavOutbox) {
        self.sync_tx(outbox);
        check_rx_component_id("0x5204", l.route_guidance_display_component_id);
        if !self.tx_ready {
            return;
        }
        let (index, lane_count) = (l.lane_guidance_index, l.lane_information.len());
        let mut out = l;
        out.route_guidance_display_component_id = TX_COMPONENT_ID;
        out.lane_guidance_description = None; // A6 lane-desc cap is 0
        match outbox.send_if_current(self.last_tx_gen, &out) {
            Ok(()) => debug!("RG_FWD_5204: index={index} lanes={lane_count}"),
            Err(e) => debug!("RG_FWD_5204 failed: {e}"),
        }
    }

    /// iPhone stream gone (0x5203 stop or RX session teardown): clear
    /// state; if the A6 is still subscribed, clear the HUD with the
    /// proven clean stop.
    fn on_disconnected(&mut self, outbox: &NavOutbox) {
        self.sync_tx(outbox);
        if self.active {
            info!("RG_ROUTE_END: iPhone stream gone (0x5203 / session teardown) — state cleared");
        }
        self.clear();
        if self.tx_ready {
            let stop = inactive_update();
            let _ = outbox.send_if_current(self.last_tx_gen, &stop);
        }
    }

    /// 5 s cadence: refresh the combo's distance while the same exit
    /// maneuver is current. Skipped when the composed text is unchanged
    /// (no repaint => no flicker) or when the distance sample is stale.
    fn on_tick(&mut self, outbox: &NavOutbox) {
        self.sync_tx(outbox);
        if !self.tx_ready || !self.active {
            return;
        }
        let Some(index) = self.current_index() else {
            return;
        };
        let Some(m) = self.maneuvers.get(&index) else {
            return;
        };
        let Some(exit) = m.exit_info.as_deref() else {
            return;
        };
        let Some(d) = self.fresh_distance(index) else {
            return;
        };
        let Some(text) = format_combo(exit, d.meters) else {
            return;
        };
        // Epoch-scoped change detection: a combo from a previous route
        // (different epoch) never suppresses this one.
        if self.last_combo.as_ref().filter(|(e, _)| *e == self.epoch).map(|(_, t)| t.as_str()) == Some(text.as_str()) {
            return;
        }
        let out = transform_maneuver(m, d.meters);
        match outbox.send_if_current(self.last_tx_gen, &out) {
            Ok(()) => {
                self.last_combo = Some((self.epoch, text.clone()));
                info!("RG_COMBO_SENT: '{text}' (5 s refresh) index={index}");
            }
            Err(e) => debug!("RG_TICK_5202 failed: {e}"),
        }
    }
}

/// Spawn the bridge. Lives for the process lifetime; exits only when the
/// RX event channel is closed (it is not, in practice).
pub fn spawn(mut events: mpsc::UnboundedReceiver<RouteGuidanceEvent>, outbox: Arc<NavOutbox>) {
    tokio::spawn(async move {
        info!(
            "RG_BRIDGE_START: iPhone->A6 route-guidance bridge live (combo refresh {COMBO_REFRESH:?}, distance freshness {DISTANCE_FRESH:?})"
        );
        let mut g = Guidance::new();
        let mut ticker = tokio::time::interval(COMBO_REFRESH);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                event = events.next() => match event {
                    Some(RouteGuidanceEvent::Update(u)) => g.on_update(u, &outbox),
                    Some(RouteGuidanceEvent::Maneuver(m)) => g.on_maneuver(m, &outbox),
                    Some(RouteGuidanceEvent::Lanes(l)) => g.on_lanes(l, &outbox),
                    Some(RouteGuidanceEvent::Disconnected) => g.on_disconnected(&outbox),
                    None => {
                        info!("RG_BRIDGE_STOP: RX event channel closed — bridge exiting");
                        break;
                    }
                },
                _ = ticker.tick() => g.on_tick(&outbox),
            }
        }
    });
}

// --- packet transforms (A6 caps + component remap + HUD combo) -----------

/// Explicit RX->TX component mapping guard (review fix #4): the ids are
/// both 1, but that is an assumption — if the iPhone ever addresses a
/// different component, log it (the remap to the A6's id still applies).
fn check_rx_component_id(context: &str, id: u16) {
    if id != RX_COMPONENT_ID {
        debug!(
            "RG_BRIDGE_WARN: {context} carries component id {id} (expected {RX_COMPONENT_ID}) — remapped to {TX_COMPONENT_ID}"
        );
    }
}

fn current_index_of(u: &RouteGuidanceUpdate) -> Option<u16> {
    u.route_guidance_maneuver_current_list
        .as_ref()
        .and_then(|l| l.0.first().copied())
}

/// 0x5201 for the A6: component remap + per-field cap enforcement
/// (over-cap road/destination names are OMITTED — the M0/M1 lesson: an
/// over-cap field poisons the whole update).
fn enforce_update_caps(u: &RouteGuidanceUpdate) -> RouteGuidanceUpdate {
    let mut out = u.clone();
    out.route_guidance_display_component_id = vec![TX_COMPONENT_ID];
    if let Some(s) = &out.current_road_name
        && s.chars().count() > A6_MAX_CURRENT_ROAD
    {
        out.current_road_name = None;
    }
    if let Some(s) = &out.destination_name
        && s.chars().count() > A6_MAX_DESTINATION
    {
        out.destination_name = None;
    }
    out
}

/// 0x5202 for the A6: component remap; `after_maneuver_road_name`
/// replaced by the HUD combo when the maneuver has exit_info and a
/// matching fresh distance was passed in. All other fields (maneuver
/// type, geometry, exit_info, static distances, index) are preserved
/// exactly as the iPhone sent them.
fn transform_maneuver(m: &RouteGuidanceManeuverInformation, meters: u32) -> RouteGuidanceManeuverInformation {
    let mut out = plain_maneuver(m);
    if let Some(exit) = m.exit_info.as_deref()
        && let Some(text) = format_combo(exit, meters)
    {
        out.after_maneuver_road_name = Some(text);
    }
    out
}

/// 0x5202 for the A6 without a combo (no exit_info, no matching
/// distance, or the combo does not fit 20 chars): street name kept,
/// truncated to the A6 cap when necessary.
fn plain_maneuver(m: &RouteGuidanceManeuverInformation) -> RouteGuidanceManeuverInformation {
    let mut out = m.clone();
    out.route_guidance_display_component_id = TX_COMPONENT_ID;
    if let Some(name) = &out.after_maneuver_road_name
        && name.chars().count() > A6_MAX_AFTER_MANEUVER
    {
        out.after_maneuver_road_name = Some(truncate_chars(name, A6_MAX_AFTER_MANEUVER));
    }
    out
}

/// Clean stop to the A6: inactive 0x5201 (the M1-proven sequence).
fn inactive_update() -> RouteGuidanceUpdate {
    RouteGuidanceUpdate {
        route_guidance_display_component_id: vec![TX_COMPONENT_ID],
        route_guidance_state: Some(RouteGuidanceState::NoRouteSetARouteIsNotSet),
        ..Default::default()
    }
}

fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combo_attached_only_for_matching_fresh_distance() {
        let mut g = Guidance::new();
        g.active = true;
        g.epoch = 1;
        g.last_update = Some(RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![RX_COMPONENT_ID],
            route_guidance_maneuver_current_list: Some(catplay_csm::decoder::CsmPacked::from(vec![0u16])),
            ..Default::default()
        });
        // Sample for maneuver 0, current epoch, fresh.
        g.distance = Some(DistanceSample {
            meters: 3200,
            maneuver_index: 0,
            epoch: 1,
            at: Instant::now(),
        });

        let m = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: RX_COMPONENT_ID,
            index: 0,
            after_maneuver_road_name: Some("I-87 North".into()),
            exit_info: Some("Exit 30".into()),
            ..Default::default()
        };
        let out = transform_maneuver(&m, g.fresh_distance(0).unwrap().meters);
        assert_eq!(out.after_maneuver_road_name.as_deref(), Some("Ext 30  3.2 km"));
        assert_eq!(out.route_guidance_display_component_id, TX_COMPONENT_ID);
        // exit_info preserved (the A6 keeps it even though it renders it nowhere).
        assert_eq!(out.exit_info.as_deref(), Some("Exit 30"));

        // Stale epoch sample: no match.
        g.distance = Some(DistanceSample {
            meters: 100,
            maneuver_index: 0,
            epoch: 2,
            at: Instant::now(),
        });
        assert!(g.fresh_distance(0).is_none());

        // Sample for a DIFFERENT maneuver index: no match (review fix #2).
        g.distance = Some(DistanceSample {
            meters: 100,
            maneuver_index: 1,
            epoch: 1,
            at: Instant::now(),
        });
        assert!(g.fresh_distance(0).is_none());
    }

    #[test]
    fn cap_enforcement_on_update() {
        let u = RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![RX_COMPONENT_ID],
            current_road_name: Some("Main Street Long".into()), // 16 > 6
            destination_name: Some("Test Destination".into()), // 18 > 6
            ..Default::default()
        };
        let out = enforce_update_caps(&u);
        assert_eq!(out.current_road_name, None);
        assert_eq!(out.destination_name, None);
        assert_eq!(out.route_guidance_display_component_id, vec![TX_COMPONENT_ID]);

        let u2 = RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![RX_COMPONENT_ID],
            current_road_name: Some("Main".into()),
            destination_name: Some("Test".into()),
            ..Default::default()
        };
        let out2 = enforce_update_caps(&u2);
        assert_eq!(out2.current_road_name.as_deref(), Some("Main"));
        assert_eq!(out2.destination_name.as_deref(), Some("Test"));
    }

    #[test]
    fn plain_maneuver_truncates_over_cap_street() {
        let m = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: RX_COMPONENT_ID,
            index: 0,
            after_maneuver_road_name: Some("A Very Long Street Name Indeed".into()),
            ..Default::default()
        };
        let out = plain_maneuver(&m);
        let name = out.after_maneuver_road_name.as_deref().unwrap();
        assert!(name.chars().count() <= A6_MAX_AFTER_MANEUVER);
        assert_eq!(name, "A Very Long Street N");
    }

    #[test]
    fn non_current_maneuver_is_cached_only() {
        let mut g = Guidance::new();
        g.active = true;
        g.epoch = 1;
        g.last_update = Some(RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![RX_COMPONENT_ID],
            route_guidance_maneuver_current_list: Some(catplay_csm::decoder::CsmPacked::from(vec![0u16])),
            ..Default::default()
        });
        let outbox = NavOutbox::new(); // no handle: sends fail, state still transitions
        let m = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: RX_COMPONENT_ID,
            index: 5, // upcoming, not current
            ..Default::default()
        };
        g.on_maneuver(m, &outbox);
        // Cached for the Order-2 re-emit; the A6 (storage 1) never sees
        // a non-current index.
        assert!(g.maneuvers.contains_key(&5));
        assert_ne!(g.last_sent_index, Some(5));
    }

    #[test]
    fn redefined_current_maneuver_invalidates_distance() {
        let mut g = Guidance::new();
        g.active = true;
        g.epoch = 1;
        g.last_update = Some(RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![RX_COMPONENT_ID],
            route_guidance_maneuver_current_list: Some(catplay_csm::decoder::CsmPacked::from(vec![0u16])),
            ..Default::default()
        });
        g.distance = Some(DistanceSample {
            meters: 3200,
            maneuver_index: 0,
            epoch: 1,
            at: Instant::now(),
        });

        let outbox = NavOutbox::new(); // no handle: sends fail, state still transitions

        // First store of the current index: the matching sample survives.
        let m1 = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: RX_COMPONENT_ID,
            index: 0,
            after_maneuver_road_name: Some("Route A Street".into()),
            exit_info: Some("Exit 30".into()),
            ..Default::default()
        };
        g.on_maneuver(m1, &outbox);
        assert!(g.fresh_distance(0).is_some());

        // Same-index redefinition (reroute without an inactive
        // transition): the distance association is now uncertain — the
        // sample is dropped until the next 0x5201 reconfirms it.
        let m2 = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: RX_COMPONENT_ID,
            index: 0,
            after_maneuver_road_name: Some("Route B Street".into()),
            exit_info: Some("Exit 12".into()),
            ..Default::default()
        };
        g.on_maneuver(m2, &outbox);
        assert!(g.fresh_distance(0).is_none());
    }
}
