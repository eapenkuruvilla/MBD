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
SPEED_GATE_KMH         : 20.0  — skip if EITHER the current or previous message
                                 speed is below this; heading is effectively
                                 undefined at low speed and turns are frequent
MAX_GPS_ACCURACY_M     :  5.0  — skip if positional accuracy (semiMajor) exceeds
                                 this; poor GPS fixes inflate derived heading rates
MIN_GAP_SECONDS        : 0.05  — pairs closer than this are timing artifacts
MAX_GAP_SECONDS        : 0.15  — gaps longer than this are skipped; a legitimate
                                 large turn could occur during a long gap
MIN_DISTANCE_M         :  5.0  — require some displacement to rule out GPS jitter
CONFIRM_N              :  2    — consecutive violations required before flagging
                                 (inherited from BaseDetector)
"""

from typing import Optional

from .utils import (
    _haversine_m, _angular_diff, _parse_secmark, _secmark_elapsed_s,
    _parse_accuracy_m, BaseDetector, get_core,
    LAT_SCALE, LON_SCALE, HEADING_UNIT, HEADING_UNAVAILABLE,
    SPEED_UNIT_MS, SPEED_UNAVAILABLE, MS_TO_KMH,
)

MAX_HEADING_RATE_DEG_S = 90.0
SPEED_GATE_KMH         = 20.0   # km/h — applied to both current and previous speed
MAX_GPS_ACCURACY_M     =  5.0   # metres
MIN_GAP_SECONDS        =  0.05
MAX_GAP_SECONDS        =  0.15
MIN_DISTANCE_M         =  5.0


class HeadingChangeRateDetector(BaseDetector):
    """Stateful detector — tracks last heading/position/speed/time per vehicle."""

    def __init__(self):
        # vehicle_id -> (heading_deg, lat, lon, secmark, speed_kmh)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        core = get_core(bsm)

        vehicle_id = core.get("id")
        hdg_raw    = core.get("heading")
        spd_raw    = core.get("speed")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")

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
        secmark     = _parse_secmark(core)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (heading_deg, lat, lon, secmark, speed_kmh)

        if prev is None:
            return None

        prev_heading, prev_lat, prev_lon, prev_secmark, prev_speed_kmh = prev

        # Speed gate — skip if either end of the interval was slow
        if speed_kmh < SPEED_GATE_KMH or prev_speed_kmh < SPEED_GATE_KMH:
            return None

        # GPS accuracy gate
        accuracy_m = _parse_accuracy_m(core)
        if accuracy_m is not None and accuracy_m > MAX_GPS_ACCURACY_M:
            return None

        if secmark is None or prev_secmark is None:
            return None

        elapsed_s = _secmark_elapsed_s(prev_secmark, secmark)
        if elapsed_s < MIN_GAP_SECONDS or elapsed_s > MAX_GAP_SECONDS:
            return None

        distance_m = _haversine_m(prev_lat, prev_lon, lat, lon)
        if distance_m < MIN_DISTANCE_M:
            return None

        heading_diff_deg = _angular_diff(prev_heading, heading_deg)
        heading_rate     = heading_diff_deg / elapsed_s   # °/s

        if heading_rate <= MAX_HEADING_RATE_DEG_S:
            self._reset_streak(vehicle_id)
            return None

        if self._increment_streak(vehicle_id) < self.CONFIRM_N:
            return None

        return {
            "misbehavior":          "implausible_heading_change_rate",
            "heading_rate_deg_s":   round(heading_rate, 2),
            "threshold_deg_s":      MAX_HEADING_RATE_DEG_S,
            "heading_diff_deg":     round(heading_diff_deg, 2),
            "elapsed_s":            round(elapsed_s, 3),
            "speed_kmh":            round(speed_kmh, 2),
        }
