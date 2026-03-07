"""
Detector: Longitudinal Acceleration/Deceleration Exceeds Threshold

BSM accelSet.long field (SAE J2735):
  - Unit: 0.01 m/s² per LSB
  - Range: -2000 to 2000  (negative = deceleration)
  - 2001 = unavailable

Threshold: 2.0 g  (1 g = 9.80665 m/s²)
  2.0 × 9.80665 = 19.6133 m/s²  →  raw threshold = 1961.33
  Flag when |raw| > 1961.33, i.e. |raw| ≥ 1962
"""

from typing import Optional

from .utils import ACCEL_UNAVAILABLE, ACCEL_UNIT_MS2, G_MS2, get_core

THRESHOLD_G = 2.0
THRESHOLD_MS2 = THRESHOLD_G * G_MS2


class AccelDetector:
    """Stateless detector — flags BSMs where |longitudinal acceleration| exceeds the threshold."""

    def check(self, bsm: dict) -> Optional[dict]:
        """Returns a misbehavior record if |longitudinal acceleration| exceeds
        the threshold, else None."""
        core = get_core(bsm)

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


# Module-level singleton — allows direct `from detectors import accel; accel.check(bsm)`
_detector = AccelDetector()


def check(bsm: dict) -> Optional[dict]:
    return _detector.check(bsm)
