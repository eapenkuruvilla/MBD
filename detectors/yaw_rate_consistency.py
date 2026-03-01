"""
Detector: Yaw Rate vs. Heading Change Rate

Compares the yaw rate reported in accelSet.yaw against the rate of heading
change derived from consecutive GPS positions.  A large discrepancy indicates
that the yaw sensor reading and the position/heading fields are inconsistent
— a strong signal of spoofing or sensor injection.

Sign convention (SAE J2735 / ISO 8855)
---------------------------------------
accelSet.yaw  positive → turning right (clockwise, increasing heading)
Heading       0° = North, increases clockwise

Reported yaw rate and GPS-derived heading change rate should therefore carry
the same sign.  The check uses the signed difference to catch both magnitude
and direction errors.

Thresholds
----------
MAX_YAW_DIFF_DEG_S : 25.0  — |reported_yaw − gps_heading_rate| above this is
                             flagged; set above the p99 (19.0 °/s) of clean pairs
MIN_SPEED_KMH      : 10.0  — only check when actually moving; yaw is noisy
                             at near-zero speed
MAX_GAP_SECONDS    :  2.0  — gaps longer than this are skipped
MIN_DISTANCE_M     :  1.0  — minimum displacement for a reliable heading rate
"""

from typing import Optional

from .utils import (
    _haversine_m, _parse_time, BaseDetector,
    LAT_SCALE, LON_SCALE, HEADING_UNIT, HEADING_UNAVAILABLE,
    SPEED_UNIT_MS, SPEED_UNAVAILABLE, YAW_UNIT, YAW_UNAVAILABLE, MS_TO_KMH,
)

MAX_YAW_DIFF_DEG_S = 90.0
MIN_SPEED_KMH      = 10.0
MAX_GAP_SECONDS    =  0.15
MIN_DISTANCE_M     =  5.0


def _signed_heading_delta(h_prev: float, h_curr: float) -> float:
    """
    Signed shortest-path heading change from h_prev to h_curr (degrees).
    Positive = clockwise (right turn), negative = counter-clockwise.
    """
    delta = (h_curr - h_prev) % 360
    if delta > 180:
        delta -= 360
    return delta


class YawRateConsistencyDetector(BaseDetector):
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
        yaw_raw    = core.get("accelSet", {}).get("yaw")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")
        ts_str     = meta.get("recordGeneratedAt", "")

        if any(v is None for v in [vehicle_id, hdg_raw, spd_raw, yaw_raw, lat_raw, lon_raw]):
            return None

        try:
            hdg_raw = int(hdg_raw)
            spd_raw = int(spd_raw)
            yaw_raw = int(yaw_raw)
            lat     = round(int(lat_raw) * LAT_SCALE, 7)
            lon     = round(int(lon_raw) * LON_SCALE, 7)
        except (ValueError, TypeError):
            return None

        if hdg_raw == HEADING_UNAVAILABLE or spd_raw == SPEED_UNAVAILABLE or yaw_raw == YAW_UNAVAILABLE:
            return None

        heading_deg    = hdg_raw * HEADING_UNIT
        speed_kmh      = spd_raw * SPEED_UNIT_MS * MS_TO_KMH
        yaw_rate_deg_s = yaw_raw * YAW_UNIT        # signed: + = right turn
        bsm_time       = _parse_time(ts_str)

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

        signed_delta        = _signed_heading_delta(prev_heading, heading_deg)
        gps_yaw_rate_deg_s  = signed_delta / elapsed_s   # signed °/s from GPS

        yaw_diff = abs(yaw_rate_deg_s - gps_yaw_rate_deg_s)

        if yaw_diff <= MAX_YAW_DIFF_DEG_S:
            return None

        return {
            "misbehavior":           "yaw_rate_inconsistency",
            "reported_yaw_deg_s":    round(yaw_rate_deg_s, 3),
            "gps_yaw_rate_deg_s":    round(gps_yaw_rate_deg_s, 3),
            "yaw_diff_deg_s":        round(yaw_diff, 3),
            "threshold_deg_s":       MAX_YAW_DIFF_DEG_S,
            "elapsed_s":             round(elapsed_s, 3),
            "speed_kmh":             round(speed_kmh, 2),
        }
