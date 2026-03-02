"""
Detector: Heading Inconsistency

Compares the reported heading in each BSM against the bearing implied by
the vehicle's movement between two consecutive messages.  A large
discrepancy suggests the heading field has been spoofed or corrupted.

Thresholds
----------
MAX_HEADING_DIFF_DEG : 20  — allowed angular difference between reported
                             heading and GPS-derived bearing
MIN_SPEED_KMH        : 10  — only check when the vehicle is actually moving;
                             heading noise dominates at near-zero speed
MIN_DISTANCE_M       :  5  — minimum displacement needed for a reliable
                             bearing calculation
MIN_GAP_SECONDS      : 0.05 — pairs closer than this are timing artifacts
MAX_GAP_SECONDS      : 0.15 — gaps longer than this are skipped (vehicle may
                             have made a legitimate turn during the gap)
"""

import math
from typing import Optional

from .utils import (
    _haversine_m, _angular_diff, _parse_secmark, _secmark_elapsed_s, BaseDetector,
    LAT_SCALE, LON_SCALE, SPEED_UNIT_MS, MS_TO_KMH,
    HEADING_UNIT, HEADING_UNAVAILABLE,
)

MAX_HEADING_DIFF_DEG = 90.0   # degrees
MIN_SPEED_KMH        = 10.0   # km/h
MIN_DISTANCE_M       =  5.0   # metres
MIN_GAP_SECONDS      =  0.05  # seconds — pairs closer than this are timing artifacts
MAX_GAP_SECONDS      =  0.15  # seconds


def _bearing_deg(lat1, lon1, lat2, lon2) -> float:
    """Forward azimuth from point-1 to point-2, returned as 0–360°."""
    lat1r, lat2r = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(lat2r)
    y = (math.cos(lat1r) * math.sin(lat2r)
         - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon))
    return (math.degrees(math.atan2(x, y)) + 360) % 360


class HeadingInconsistencyDetector(BaseDetector):
    """Stateful detector — tracks last position per vehicle to derive bearing."""

    def __init__(self):
        # vehicle_id -> (lat, lon, secmark)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")
        h_raw      = core.get("heading")
        spd_raw    = core.get("speed")

        if any(v is None for v in [vehicle_id, lat_raw, lon_raw, h_raw, spd_raw]):
            return None

        try:
            h_raw   = int(h_raw)
            spd_raw = int(spd_raw)
            lat = round(int(lat_raw) * LAT_SCALE, 7)
            lon = round(int(lon_raw) * LON_SCALE, 7)
        except (ValueError, TypeError):
            return None

        if h_raw == HEADING_UNAVAILABLE:
            return None

        reported_deg = h_raw * HEADING_UNIT
        speed_kmh    = spd_raw * SPEED_UNIT_MS * MS_TO_KMH
        secmark      = _parse_secmark(core)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (lat, lon, secmark)

        if prev is None:
            return None  # first message for this vehicle — nothing to compare

        if speed_kmh < MIN_SPEED_KMH:
            return None  # heading noise dominates at near-zero speed

        prev_lat, prev_lon, prev_secmark = prev

        if secmark is None or prev_secmark is None:
            return None

        elapsed_s = _secmark_elapsed_s(prev_secmark, secmark)
        if elapsed_s < MIN_GAP_SECONDS or elapsed_s > MAX_GAP_SECONDS:
            return None

        distance_m = _haversine_m(prev_lat, prev_lon, lat, lon)
        if distance_m < MIN_DISTANCE_M:
            return None  # too little movement for a reliable GPS bearing

        gps_bearing  = _bearing_deg(prev_lat, prev_lon, lat, lon)
        heading_diff = _angular_diff(reported_deg, gps_bearing)

        if heading_diff <= MAX_HEADING_DIFF_DEG:
            return None

        return {
            "misbehavior":      "heading_inconsistency",
            "reported_heading": round(reported_deg, 2),
            "gps_bearing":      round(gps_bearing, 2),
            "heading_diff":     round(heading_diff, 2),
            "threshold_deg":    MAX_HEADING_DIFF_DEG,
            "speed_kmh":        round(speed_kmh, 2),
            "distance_m":       round(distance_m, 1),
        }
