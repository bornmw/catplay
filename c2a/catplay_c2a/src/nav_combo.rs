//! HUD combo formatter: "Ext 30  3.2 km" — exit number + live distance,
//! replacing after_maneuver_road_name on the A6's HUD (the one free-form
//! text field it renders; caps at 20 chars).
//!
//! Shared by the live bridge (nav_bridge) and the self-test so both emit
//! the exact production format.
//!
//! Rules (per protocol review):
//! - Exit label normalized conservatively: recognized prefixes
//!   ("Exit", "Ext", "Ausfahrt", "Bh") or bare identifiers become
//!   "Ext <id>"; unrecognizable strings are preserved as-is when they fit.
//! - Distance: >= 1000 m -> km with one decimal (integer math,
//!   nearest-100-m rounding); < 1000 m -> whole meters.
//! - Separator: two spaces (constant; MIB3 may collapse them).
//! - Total <= 20 chars including spaces and unit; fall back to a single
//!   space, then to None (caller keeps the original street name).

/// Maximum HUD text width (A6 max_after_maneuver_road_name_length).
pub const HUD_MAX: usize = 20;

/// Separator between exit label and distance.
const SEP: &str = "  ";

/// Normalize an iPhone exit_info string to "Ext <id>".
///
/// "Exit 12A" -> "Ext 12A"; "Ausfahrt 12" -> "Ext 12"; "12A" -> "Ext 12A";
/// "Ext 30" -> "Ext 30"; unrecognizable input is returned trimmed unchanged.
pub fn format_exit_label(exit: &str) -> String {
    let t = exit.trim();
    // Trailing identifier: digits with an optional letter suffix.
    let ident = t
        .rsplit(' ')
        .next()
        .filter(|tok| !tok.is_empty() && is_exit_ident(tok))
        .map(|tok| tok.to_string());
    match ident {
        Some(id) => format!("Ext {id}"),
        None => t.to_string(),
    }
}

fn is_exit_ident(tok: &str) -> bool {
    let mut chars = tok.chars();
    match chars.next() {
        Some(c) if c.is_ascii_digit() => chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        _ => false,
    }
}

/// Format a distance in meters for the HUD combo.
pub fn format_distance(meters: u32) -> String {
    if meters >= 1000 {
        // Tenths of a km, rounded to the nearest 100 m (integer math).
        let tenths = (meters + 50) / 100;
        format!("{}.{} km", tenths / 10, tenths % 10)
    } else {
        format!("{meters} m")
    }
}

/// Compose the HUD text for an exit + live distance.
///
/// Returns None when the result cannot fit HUD_MAX — the caller keeps the
/// original street name (a too-long combo is worse than the street).
pub fn format_combo(exit: &str, meters: u32) -> Option<String> {
    let exit = exit.trim();
    if exit.is_empty() {
        return None;
    }
    let label = format_exit_label(exit);
    let dist = format_distance(meters);
    let mut text = format!("{label}{SEP}{dist}");
    if text.chars().count() > HUD_MAX {
        // Retry with a single space.
        text = format!("{label} {dist}");
    }
    if text.chars().count() > HUD_MAX {
        return None;
    }
    Some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_label_normalization() {
        assert_eq!(format_exit_label("Exit 12A"), "Ext 12A");
        assert_eq!(format_exit_label("Ext 30"), "Ext 30");
        assert_eq!(format_exit_label("Ausfahrt 12"), "Ext 12");
        assert_eq!(format_exit_label("12A"), "Ext 12A");
        assert_eq!(format_exit_label("  Exit 7  "), "Ext 7");
        // No recognizable identifier: preserve (trimmed).
        assert_eq!(format_exit_label("I-87 North Exit"), "I-87 North Exit");
        assert_eq!(format_exit_label(""), "");
    }

    #[test]
    fn distance_formatting() {
        assert_eq!(format_distance(3200), "3.2 km");
        assert_eq!(format_distance(995), "995 m");
        assert_eq!(format_distance(1000), "1.0 km");
        assert_eq!(format_distance(1050), "1.1 km"); // nearest 100 m
        assert_eq!(format_distance(950), "950 m");
        assert_eq!(format_distance(12500), "12.5 km");
    }

    #[test]
    fn combo_fits() {
        assert_eq!(format_combo("Exit 30", 3200), Some("Ext 30  3.2 km".into()));
        assert_eq!(format_combo("Exit 30", 300), Some("Ext 30  300 m".into()));
        assert_eq!(
            format_combo("Exit 128", 12500),
            Some("Ext 128  12.5 km".into())
        );
        // Long exit + long distance: still fits with the two-space
        // separator (17 chars).
        let c = format_combo("Exit 128A", 12500).expect("fits");
        assert!(c.chars().count() <= HUD_MAX);
        assert_eq!(c, "Ext 128A  12.5 km");
    }

    #[test]
    fn combo_single_space_fallback() {
        // 10-char label + 9-char distance: 21 chars with two spaces,
        // 20 with one — the fallback must trigger and still fit.
        let c = format_combo("Exit 12345A", 1_234_500).expect("fits");
        assert_eq!(c, "Ext 12345A 1234.5 km");
        assert_eq!(c.chars().count(), HUD_MAX);
    }

    #[test]
    fn combo_refused_when_too_long() {
        // 30-char exit label cannot fit: keep the street name.
        assert_eq!(format_combo("An Extremely Long Name", 3200), None);
        assert_eq!(format_combo("", 3200), None);
    }
}
