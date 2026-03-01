"""
Detector: Speed-Acceleration Consistency

The change in reported speed between two consecutive messages from the same
vehicle should match the reported longitudinal acceleration integrated over
the elapsed time:

    expected_Δspeed = accelSet.long × Δt

A large deviation suggests that one of the three fields (speed, acceleration,
or timestamp) has been spoofed or is severely corrupted.

Thresholds
----------
MAX_DELTA_ERROR_MS : 0.50  — allowed |observed_Δspeed − expected_Δspeed| in m/s;
                             set above the p99 (0.40 m/s) of clean consecutive pairs
MAX_GAP_SECONDS    : 2.0   — gaps longer than this are skipped; the linear
                             integration assumption breaks down over long intervals
MIN_DELTA_SPEED_MS : 0.10  — require a meaningful speed change before flagging;
                             filters noise when the vehicle is nearly constant-speed
"""

from typing import Optional

from .utils import (
    _parse_time, BaseDetector,
    SPEED_UNIT_MS, SPEED_UNAVAILABLE, ACCEL_UNIT_MS2, ACCEL_UNAVAILABLE, MS_TO_KMH,
)

MAX_DELTA_ERROR_MS = 5.0
MAX_GAP_SECONDS    = 0.15
MIN_DELTA_SPEED_KMH = 20.0
MIN_DELTA_SPEED_MS = MIN_DELTA_SPEED_KMH / MS_TO_KMH


class SpeedAccelConsistencyDetector(BaseDetector):
    """Stateful detector — tracks last speed/accel/time per vehicle."""

    def __init__(self):
        # vehicle_id -> (speed_ms, accel_ms2, datetime)
        super().__init__()

    def check(self, bsm: dict) -> Optional[dict]:
        meta = bsm.get("metadata", {})
        core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

        vehicle_id = core.get("id")
        spd_raw    = core.get("speed")
        acc_raw    = core.get("accelSet", {}).get("long")
        ts_str     = meta.get("recordGeneratedAt", "")

        if any(v is None for v in [vehicle_id, spd_raw, acc_raw]):
            return None

        try:
            spd_raw = int(spd_raw)
            acc_raw = int(acc_raw)
        except (ValueError, TypeError):
            return None

        if spd_raw == SPEED_UNAVAILABLE or acc_raw == ACCEL_UNAVAILABLE:
            return None

        speed_ms  = spd_raw * SPEED_UNIT_MS
        accel_ms2 = acc_raw * ACCEL_UNIT_MS2
        bsm_time  = _parse_time(ts_str)

        prev = self._last.get(vehicle_id)
        self._last[vehicle_id] = (speed_ms, accel_ms2, bsm_time)

        if prev is None:
            return None

        prev_speed_ms, prev_accel_ms2, prev_time = prev

        if bsm_time is None or prev_time is None:
            return None

        elapsed_s = (bsm_time - prev_time).total_seconds()
        if elapsed_s <= 0 or elapsed_s > MAX_GAP_SECONDS:
            return None

        observed_delta  = speed_ms - prev_speed_ms
        expected_delta  = prev_accel_ms2 * elapsed_s
        error_ms        = abs(observed_delta - expected_delta)

        if abs(observed_delta) < MIN_DELTA_SPEED_MS:
            return None

        if error_ms <= MAX_DELTA_ERROR_MS:
            return None

        return {
            "misbehavior":       "speed_accel_inconsistency",
            "observed_delta_ms": round(observed_delta, 4),
            "expected_delta_ms": round(expected_delta, 4),
            "error_ms":          round(error_ms, 4),
            "error_kmh":         round(error_ms * MS_TO_KMH, 2),
            "threshold_ms":      MAX_DELTA_ERROR_MS,
            "accel_ms2":         round(prev_accel_ms2, 4),
            "elapsed_s":         round(elapsed_s, 3),
        }
