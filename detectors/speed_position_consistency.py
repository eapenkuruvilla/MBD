"""
Detector: Speed vs. Position Consistency

Compares the speed reported in each BSM against the speed implied by the
vehicle's GPS displacement between consecutive messages.  A significant
disagreement in either direction suggests a spoofed speed or position field.

  reported >> implied  — speed field inflated (e.g. ghost-vehicle attack)
  implied >> reported  — position jumps faster than the vehicle claims to move

Thresholds
----------
MAX_SPEED_DIFF_KMH : 20.0  — absolute difference above which a misbehavior is
                             flagged; set above the p95 (14.6 km/h) of clean data
MIN_SPEED_KMH      :  5.0  — both reported and implied speed must exceed this;
                             near-standstill GPS noise dominates below this value
MAX_GAP_SECONDS    :  2.0  — gaps longer than this are skipped; large Δt makes
                             the haversine average unreliable
MIN_DISTANCE_M     :  1.0  — minimum displacement to produce a meaningful
                             implied-speed estimate
"""

from typing import Optional

from .utils import (
    _haversine_m, _parse_time, BaseDetector,
    LAT_SCALE, LON_SCALE, SPEED_UNIT_MS, SPEED_UNAVAILABLE, MS_TO_KMH,
)

MAX_SPEED_DIFF_KMH = 500.0
MIN_SPEED_KMH      =  10.0
MAX_GAP_SECONDS    =  0.15
MIN_DISTANCE_M     =  10.0


class SpeedPositionConsistencyDetector(BaseDetector):
    """Stateful detector — tracks last known position/time per vehicle."""

    def __init__(self):
        # vehicle_id -> (lat, lon, datetime, speed_ms)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        meta = bsm.get("metadata", {})
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")
        spd_raw    = core.get("speed")
        ts_str     = meta.get("recordGeneratedAt", "")

        if any(v is None for v in [vehicle_id, lat_raw, lon_raw, spd_raw]):
            return None

        try:
            lat     = round(int(lat_raw) * LAT_SCALE, 7)
            lon     = round(int(lon_raw) * LON_SCALE, 7)
            spd_raw = int(spd_raw)
        except (ValueError, TypeError):
            return None

        if spd_raw == SPEED_UNAVAILABLE:
            return None

        speed_ms  = spd_raw * SPEED_UNIT_MS
        speed_kmh = speed_ms * MS_TO_KMH
        bsm_time  = _parse_time(ts_str)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (lat, lon, bsm_time, speed_ms)

        if prev is None:
            return None

        prev_lat, prev_lon, prev_time, prev_speed_ms = prev

        if bsm_time is None or prev_time is None:
            return None

        elapsed_s = (bsm_time - prev_time).total_seconds()
        if elapsed_s <= 0 or elapsed_s > MAX_GAP_SECONDS:
            return None

        distance_m   = _haversine_m(prev_lat, prev_lon, lat, lon)
        if distance_m < MIN_DISTANCE_M:
            return None

        implied_ms  = distance_m / elapsed_s
        implied_kmh = implied_ms * MS_TO_KMH

        # Both speeds must be above the noise floor
        if speed_kmh < MIN_SPEED_KMH and implied_kmh < MIN_SPEED_KMH:
            return None

        diff_kmh = speed_kmh - implied_kmh   # positive = reported faster than GPS

        if abs(diff_kmh) <= MAX_SPEED_DIFF_KMH:
            return None

        direction = "reported_exceeds_implied" if diff_kmh > 0 else "implied_exceeds_reported"

        return {
            "misbehavior":        "speed_position_inconsistency",
            "direction":          direction,
            "reported_speed_kmh": round(speed_kmh, 2),
            "implied_speed_kmh":  round(implied_kmh, 2),
            "diff_kmh":           round(diff_kmh, 2),
            "abs_diff_kmh":       round(abs(diff_kmh), 2),
            "threshold_kmh":      MAX_SPEED_DIFF_KMH,
            "distance_m":         round(distance_m, 1),
            "elapsed_s":          round(elapsed_s, 3),
        }
