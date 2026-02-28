"""
Detector: Rapid Position Jump

Compares each BSM against the last known position of the same vehicle.
Flags when the implied speed (distance ÷ elapsed_time) exceeds a plausible
maximum, indicating a position spoof or severe data error.

Thresholds
----------
MAX_JUMP_SPEED_KMH : 160  — implied speed above this is flagged
MIN_JUMP_METERS    :  50  — jump must be at least this large;
                            filters out GPS noise on tiny Δt
MAX_GAP_SECONDS    :  60  — gaps longer than this are skipped; the vehicle
                            may have legitimately reappeared elsewhere
"""

import math
from datetime import datetime
from typing import Optional

LAT_SCALE  = 1e-7
LON_SCALE  = 1e-7
MS_TO_KMH  = 3.6           # m/s → km/h

MAX_JUMP_SPEED_KMH = 120.0  # km/h — implied speed must exceed this
MIN_JUMP_METERS    = 50.0   # m    — filters out GPS noise on tiny Δt
MAX_GAP_SECONDS    = 60.0   # s    — ignore gaps longer than this


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_time(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    clean = ts.split("[")[0].strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(clean, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        return None


class PositionJumpDetector:
    """Stateful detector — tracks the last known position per vehicle."""

    def __init__(self):
        # vehicle_id -> (lat, lon, datetime)
        self._last: dict = {}

    def check(self, bsm: dict) -> Optional[dict]:
        meta = bsm.get("metadata", {})
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        lat_raw    = core.get("lat")
        lon_raw    = core.get("long")
        ts_str     = meta.get("recordGeneratedAt", "")

        if vehicle_id is None or lat_raw is None or lon_raw is None:
            return None

        try:
            lat = round(int(lat_raw) * LAT_SCALE, 7)
            lon = round(int(lon_raw) * LON_SCALE, 7)
        except (ValueError, TypeError):
            return None

        bsm_time = _parse_time(ts_str)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (lat, lon, bsm_time)

        if prev is None:
            return None  # first message for this vehicle — nothing to compare

        prev_lat, prev_lon, prev_time = prev

        if bsm_time is None or prev_time is None:
            return None

        elapsed_s = (bsm_time - prev_time).total_seconds()

        if elapsed_s <= 0 or elapsed_s > MAX_GAP_SECONDS:
            return None  # out-of-order or gap too large

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
