"""
Detector: Implausible Heading Change Rate

Computes the rate of heading change (°/s) between consecutive messages from
the same vehicle.  At any meaningful road speed, tyre-friction physics cap
how fast a vehicle can yaw.  A rate well beyond the clean-data ceiling
indicates a spoofed or corrupted heading field.

Thresholds
----------
MAX_HEADING_RATE_DEG_S : 90.0  — heading change rate above this is flagged;
                                 the empirical maximum in clean data is 65.9 °/s
MIN_SPEED_KMH          : 10.0  — only check when actually moving; heading
                                 is effectively undefined at near-zero speed
MAX_GAP_SECONDS        :  2.0  — gaps longer than this are skipped; a legitimate
                                 large turn could occur during a long gap
MIN_DISTANCE_M         :  1.0  — require some displacement to rule out GPS jitter
"""

from typing import Optional

from .utils import (
    _haversine_m, _angular_diff, _parse_time, BaseDetector,
    LAT_SCALE, LON_SCALE, HEADING_UNIT, HEADING_UNAVAILABLE,
    SPEED_UNIT_MS, SPEED_UNAVAILABLE, MS_TO_KMH,
)

MAX_HEADING_RATE_DEG_S = 90.0
MIN_SPEED_KMH          = 10.0  # About 1.0 g
MAX_GAP_SECONDS        =  0.15
MIN_DISTANCE_M         =  5.0


class HeadingChangeRateDetector(BaseDetector):
    """Stateful detector — tracks last heading/position/time per vehicle."""

    def __init__(self):
        # vehicle_id -> (heading_deg, lat, lon, datetime)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        meta = bsm.get("metadata", {})
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        hdg_raw    = core.get("heading")
        spd_raw    = core.get("speed")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")
        ts_str     = meta.get("recordGeneratedAt", "")

        if any(v is None for v in [vehicle_id, hdg_raw, spd_raw, lat_raw, lon_raw]):
            return None

        try:
            hdg_raw = int(hdg_raw)
            spd_raw = int(spd_raw)
            lat     = round(int(lat_raw) * LAT_SCALE, 7)
            lon     = round(int(lon_raw) * LON_SCALE, 7)
        except (ValueError, TypeError):
            return None

        if hdg_raw == HEADING_UNAVAILABLE or spd_raw == SPEED_UNAVAILABLE:
            return None

        heading_deg = hdg_raw * HEADING_UNIT
        speed_kmh   = spd_raw * SPEED_UNIT_MS * MS_TO_KMH
        bsm_time    = _parse_time(ts_str)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (heading_deg, lat, lon, bsm_time)

        if prev is None:
            return None

        if speed_kmh < MIN_SPEED_KMH:
            return None

        prev_heading, prev_lat, prev_lon, prev_time = prev

        if bsm_time is None or prev_time is None:
            return None

        elapsed_s = (bsm_time - prev_time).total_seconds()
        if elapsed_s <= 0 or elapsed_s > MAX_GAP_SECONDS:
            return None

        distance_m = _haversine_m(prev_lat, prev_lon, lat, lon)
        if distance_m < MIN_DISTANCE_M:
            return None

        heading_diff_deg = _angular_diff(prev_heading, heading_deg)
        heading_rate     = heading_diff_deg / elapsed_s   # °/s

        if heading_rate <= MAX_HEADING_RATE_DEG_S:
            return None

        return {
            "misbehavior":          "implausible_heading_change_rate",
            "heading_rate_deg_s":   round(heading_rate, 2),
            "threshold_deg_s":      MAX_HEADING_RATE_DEG_S,
            "heading_diff_deg":     round(heading_diff_deg, 2),
            "elapsed_s":            round(elapsed_s, 3),
            "speed_kmh":            round(speed_kmh, 2),
        }
