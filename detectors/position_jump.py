"""
Detector: Rapid Position Jump

Compares each BSM against the last known position of the same vehicle.
Flags when the implied speed (distance ÷ elapsed_time) exceeds a plausible
maximum, indicating a position spoof or severe data error.

Thresholds
----------
MAX_JUMP_SPEED_KMH :  10  — implied speed above this is flagged
MIN_JUMP_METERS    : 100  — jump must be at least this large;
                            filters out GPS noise on tiny Δt
MIN_GAP_SECONDS    : 0.05 — pairs closer than this are timing artifacts
MAX_GAP_SECONDS    : 0.15 — gaps longer than this are skipped; the vehicle
                            may have legitimately reappeared elsewhere
"""

from typing import Optional

from .utils import _haversine_m, _parse_secmark, _secmark_elapsed_s, BaseDetector, LAT_SCALE, LON_SCALE, MS_TO_KMH

MAX_JUMP_SPEED_KMH = 10.0  # km/h — implied speed must exceed this
MIN_JUMP_METERS    = 100.0  # m    — filters out GPS noise on tiny Δt
MIN_GAP_SECONDS    =  0.05  # s    — pairs closer than this are timing artifacts
MAX_GAP_SECONDS    =  0.15  # s    — ignore gaps longer than this


class PositionJumpDetector(BaseDetector):
    """Stateful detector — tracks the last known position per vehicle."""

    def __init__(self):
        # vehicle_id -> (lat, lon, secmark)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")

        if vehicle_id is None or lat_raw is None or lon_raw is None:
            return None

        try:
            lat = round(int(lat_raw) * LAT_SCALE, 7)
            lon = round(int(lon_raw) * LON_SCALE, 7)
        except (ValueError, TypeError):
            return None

        secmark = _parse_secmark(core)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (lat, lon, secmark)

        if prev is None:
            return None  # first message for this vehicle — nothing to compare

        prev_lat, prev_lon, prev_secmark = prev

        if secmark is None or prev_secmark is None:
            return None

        elapsed_s = _secmark_elapsed_s(prev_secmark, secmark)

        if elapsed_s < MIN_GAP_SECONDS or elapsed_s > MAX_GAP_SECONDS:
            return None  # timing artifact, out-of-order, or gap too large

        distance_m   = _haversine_m(prev_lat, prev_lon, lat, lon)
        implied_kmh  = (distance_m / elapsed_s) * MS_TO_KMH

        if distance_m < MIN_JUMP_METERS or implied_kmh <= MAX_JUMP_SPEED_KMH:
            return None

        return {
            "misbehavior":       "position_jump",
            "jump_m":            round(distance_m, 1),
            "elapsed_s":         round(elapsed_s, 3),
            "implied_speed_kmh": round(implied_kmh, 2),
            "threshold_kmh":     MAX_JUMP_SPEED_KMH,
            "prev_lat":          prev_lat,
            "prev_lon":          prev_lon,
        }
