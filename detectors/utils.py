"""Shared utilities for BSM misbehaviour detectors.

Centralises:
  - Unit-conversion constants (SAE J2735)
  - Sentinel / unavailable values
  - Pure-function helpers (haversine distance, heading geometry, timestamp parsing)
  - BaseDetector — lightweight base class for stateful (per-vehicle) detectors
"""

import math
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# SAE J2735 encoding constants
# ---------------------------------------------------------------------------

LAT_SCALE       = 1e-7      # degrees per LSB (latitude / longitude)
LON_SCALE       = 1e-7
SPEED_UNIT_MS   = 0.02      # m/s per LSB
HEADING_UNIT    = 0.0125    # degrees per LSB
YAW_UNIT        = 0.01      # degrees/s per LSB
ACCEL_UNIT_MS2  = 0.01      # m/s² per LSB
G_MS2           = 9.80665   # standard gravity, m/s²
MS_TO_KMH       = 3.6       # m/s → km/h

# Sentinel values meaning "field not available"
SPEED_UNAVAILABLE   = 8191
HEADING_UNAVAILABLE = 28800
YAW_UNAVAILABLE     = 32767
ACCEL_UNAVAILABLE   = 2001


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance between two WGS-84 points, in metres."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _angular_diff(a: float, b: float) -> float:
    """Smallest unsigned difference between two compass headings (0–180°)."""
    diff = abs(a - b) % 360
    return diff if diff <= 180 else 360 - diff


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def _parse_time(ts: str) -> Optional[datetime]:
    """Parse a BSM timestamp string; return datetime or None on failure."""
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


# ---------------------------------------------------------------------------
# Base class for stateful (per-vehicle) detectors
# ---------------------------------------------------------------------------

class BaseDetector:
    """Lightweight base for detectors that maintain per-vehicle state.

    Subclasses store whatever tuple they need in ``self._last[vehicle_id]``
    and call ``super().__init__()`` from their own ``__init__``.
    """

    def __init__(self):
        self._last: dict = {}
