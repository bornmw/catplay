#[cfg(test)]
mod tests {
    use catplay_csm::decoder::*;
    use catplay_csm::msg::iap2::PowerUpdate;

    #[test]
    fn registry_should_not_be_empty_at_runtime() {
        let registry: &'static CsmPacketRegistry = CsmPacketRegistry::static_registry();
        let ids: Vec<String> = registry
            .all_known_ids()
            .iter()
            .map(|x| format!("{:#04x}", *x))
            .collect();
        assert_eq!(ids.len(), 144);
        println!("Known IDs: {:?}", ids)
    }

    #[test]
    fn registry_encode_decode_unknown() {
        let registry = CsmPacketRegistry::new();

        let packet = CsmPacketWithPayload::new(0x1234, &[1, 2, 3]).unwrap();
        let encoded = packet.serialize();

        let expected = CsmUnknownPacket(0x1234, vec![1, 2, 3]);
        let decoded = registry.decode(&encoded).unwrap();

        assert_eq!(Some(expected).as_ref(), CsmUnknownPacket::cast(&decoded));

        let encoded = registry.encode(&decoded).unwrap();
        assert_eq!(encoded, encoded);
    }

    #[cfg(feature = "clone_box")]
    #[test]
    fn boxed_packet_clone_preserves_type_and_payload() {
        let original: CsmPacketBox = CsmUnknownPacket(0x1234, vec![1, 2, 3]).into();
        let cloned = original.clone();
        drop(original);

        assert_eq!(cloned.cast::<CsmUnknownPacket>(), Some(&CsmUnknownPacket(0x1234, vec![1, 2, 3])));
    }

    #[test]
    fn registry_creates_default_and_decodes_payload() {
        let registry = CsmPacketRegistry::static_registry();
        let id = PowerUpdate::PACKET_ID;

        let empty = registry.create_by_id(id).unwrap();
        assert_eq!(empty.cast::<PowerUpdate>(), Some(&PowerUpdate::default()));

        let value = PowerUpdate {
            maximum_current_drawn_from_accessory: Some(42),
            ..PowerUpdate::default()
        };
        let decoded = registry
            .create_by_id_from_bytes(id, &value.serialize())
            .unwrap();
        assert_eq!(decoded.cast::<PowerUpdate>(), Some(&value));
    }

    #[test]
    fn route_guidance_packets_are_registered_for_tx() {
        // The 0x520x types are registered by the register_packet_family!
        // batch in msg/iap2.rs; their packet_type! blocks carry @no_register
        // precisely to avoid a duplicate registration (which panics at
        // registry init and kills the app at boot). The nav self-test sends
        // them through CsmClientHandle::send, which requires the static
        // registry to know their IDs. Guard the encode/decode round-trip
        // here so a lost (or doubled) registration fails in CI, not in-car.
        use catplay_csm::msg::iap2::{
            LaneGuidanceInformation, LaneInformation, LaneInformationStatus, ManeuverType,
            RouteGuidanceManeuverInformation, RouteGuidanceState, RouteGuidanceUpdate,
        };

        let registry = CsmPacketRegistry::static_registry();

        let update = RouteGuidanceUpdate {
            route_guidance_display_component_id: vec![1],
            route_guidance_state: Some(RouteGuidanceState::RouteSetARouteIsSet),
            distance_remaining_to_next_maneuver: Some(500),
            ..RouteGuidanceUpdate::default()
        };
        let encoded = registry.encode(&update).expect("RouteGuidanceUpdate must be encodable");
        let decoded = registry.decode(&encoded).expect("RouteGuidanceUpdate decode");
        assert_eq!(decoded.cast::<RouteGuidanceUpdate>(), Some(&update));

        let maneuver = RouteGuidanceManeuverInformation {
            route_guidance_display_component_id: 1,
            index: 0,
            maneuver_type: Some(ManeuverType::LeftTurnAngleIsBetween45And135Degrees),
            after_maneuver_road_name: Some("Elm Street".into()),
            ..RouteGuidanceManeuverInformation::default()
        };
        let encoded =
            registry.encode(&maneuver).expect("RouteGuidanceManeuverInformation must be encodable");
        let decoded = registry.decode(&encoded).expect("RouteGuidanceManeuverInformation decode");
        assert_eq!(decoded.cast::<RouteGuidanceManeuverInformation>(), Some(&maneuver));

        let lanes = LaneGuidanceInformation {
            route_guidance_display_component_id: 1,
            lane_guidance_index: 0,
            lane_information: vec![LaneInformation {
                index: 0,
                lane_status: LaneInformationStatus::Value2,
                ..LaneInformation::default()
            }],
            ..LaneGuidanceInformation::default()
        };
        let encoded = registry.encode(&lanes).expect("LaneGuidanceInformation must be encodable");
        let decoded = registry.decode(&encoded).expect("LaneGuidanceInformation decode");
        assert_eq!(decoded.cast::<LaneGuidanceInformation>(), Some(&lanes));
    }
}
