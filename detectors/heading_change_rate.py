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

import math
from datetime import datetime
from typing import Optional

LAT_SCALE       = 1e-7
LON_SCALE       = 1e-7
HEADING_UNIT    = 0.0125    # degrees per LSB
HEADING_UNAVAIL = 28800
SPEED_UNIT_MS   = 0.02
SPEED_UNAVAIL   = 8191
MS_TO_KMH       = 3.6

MAX_HEADING_RATE_DEG_S = 90.0
MIN_SPEED_KMH          = 200.0
MAX_GAP_SECONDS        =  1.0
MIN_DISTANCE_M         =  50.0


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _angular_diff(a: float, b: float) -> float:
    """Smallest unsigned angular difference between two headings (0–180°)."""
    diff = abs(a - b) % 360
    return diff if diff <= 180 else 360 - diff


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


class HeadingChangeRateDetector:
    """Stateful detector — tracks last heading/position/time per vehicle."""

    def __init__(self):
        # vehicle_id -> (heading_deg, lat, lon, datetime)
        self._last: dict = {}

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

        if hdg_raw == HEADING_UNAVAIL or spd_raw == SPEED_UNAVAIL:
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
