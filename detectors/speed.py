"""
Detector: Speed Exceeds Threshold

BSM speed field (coreData.speed) is encoded per SAE J2735:
  - Unit: 0.02 m/s per LSB
  - Range: 0–8190 (8191 = unavailable)

120 km/h = 33.333 m/s → 1666.67 units → flag when raw value > 1666
"""

from typing import Optional

SPEED_UNIT_MS = 0.02          # m/s per LSB
MS_TO_KMH = 3.6
SPEED_UNAVAILABLE = 8191
THRESHOLD_KMH = 500.0
THRESHOLD_RAW = THRESHOLD_KMH / (SPEED_UNIT_MS * MS_TO_KMH)  # ~1666.67


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

    speed_kmh = raw * SPEED_UNIT_MS * MS_TO_KMH

    if speed_kmh <= THRESHOLD_KMH:
        return None

    return {
        "misbehavior": "speed_exceeded",
        "speed_kmh": round(speed_kmh, 2),
        "threshold_kmh": THRESHOLD_KMH,
        "speed_raw": raw,
    }
