"""
Detector: Longitudinal Acceleration/Deceleration Exceeds Threshold

BSM accelSet.long field (SAE J2735):
  - Unit: 0.01 m/s² per LSB
  - Range: -2000 to 2000  (negative = deceleration)
  - 2001 = unavailable

Threshold: 1.0 g  (1 g = 9.80665 m/s²)
  1.0 × 9.80665 = 9.80665 m/s²  →  raw threshold = 980.665
  Flag when |raw| > 980.665, i.e. |raw| ≥ 981
"""

from typing import Optional

from .utils import ACCEL_UNAVAILABLE, ACCEL_UNIT_MS2, G_MS2

THRESHOLD_G = 1.0
THRESHOLD_MS2 = THRESHOLD_G * G_MS2


def check(bsm: dict) -> Optional[dict]:
    """
    Returns a misbehavior record if |longitudinal acceleration| exceeds
    the threshold, else None.
    """
    core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

    raw = core.get("accelSet", {}).get("long")
    if raw is None:
        return None

    try:
        raw = int(raw)
    except (ValueError, TypeError):
        return None

    if raw == ACCEL_UNAVAILABLE:
        return None

    accel_ms2 = raw * ACCEL_UNIT_MS2
    accel_g = accel_ms2 / G_MS2

    if abs(accel_g) <= THRESHOLD_G:
        return None

    return {
        "misbehavior": "accel_exceeded",
        "accel_g": round(accel_g, 4),
        "accel_ms2": round(accel_ms2, 4),
        "threshold_g": THRESHOLD_G,
        "accel_raw": raw,
    }
