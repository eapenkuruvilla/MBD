"""
Detector: Speed Exceeds Threshold

BSM speed field (coreData.speed) is encoded per SAE J2735:
  - Unit: 0.02 m/s per LSB
  - Range: 0–8190 (8191 = unavailable)

70 mph = 31.2928 m/s → 1564.64 units → flag when raw value > 1564
"""

from typing import Optional

SPEED_UNIT_MS = 0.02          # m/s per LSB
MS_TO_MPH = 2.23694
SPEED_UNAVAILABLE = 8191
THRESHOLD_MPH = 70.0
THRESHOLD_RAW = THRESHOLD_MPH / (SPEED_UNIT_MS * MS_TO_MPH)  # ~1564.64


def check(bsm: dict) -> Optional[dict]:
    """
    Returns a misbehavior record if speed exceeds the threshold, else None.
    """
    core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

    raw = core.get("speed")
    if raw is None:
        return None

    try:
        raw = int(raw)
    except (ValueError, TypeError):
        return None

    if raw == SPEED_UNAVAILABLE:
        return None

    speed_mph = raw * SPEED_UNIT_MS * MS_TO_MPH

    if speed_mph <= THRESHOLD_MPH:
        return None

    return {
        "misbehavior": "speed_exceeded",
        "speed_mph": round(speed_mph, 2),
        "threshold_mph": THRESHOLD_MPH,
        "speed_raw": raw,
    }
