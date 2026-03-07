"""
Detector: Speed Exceeds Threshold

BSM speed field (coreData.speed) is encoded per SAE J2735:
  - Unit: 0.02 m/s per LSB
  - Range: 0–8190 (8191 = unavailable)

200 km/h = 55.556 m/s → 2777.78 units → flag when speed_kmh > 200
"""

from typing import Optional

from .utils import MS_TO_KMH, SPEED_UNAVAILABLE, SPEED_UNIT_MS, get_core

THRESHOLD_KMH = 200.0


class SpeedDetector:
    """Stateless detector — flags BSMs where reported speed exceeds the threshold."""

    def check(self, bsm: dict) -> Optional[dict]:
        """Returns a misbehavior record if speed exceeds the threshold, else None."""
        core = get_core(bsm)

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


# Module-level singleton — allows direct `from detectors import speed; speed.check(bsm)`
_detector = SpeedDetector()


def check(bsm: dict) -> Optional[dict]:
    return _detector.check(bsm)
