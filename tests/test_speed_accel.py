"""Tests for SpeedAccelConsistencyDetector.

The detector flags when |observed_Δspeed − expected_Δspeed| > MAX_DELTA_ERROR_MS (5 m/s)
AND |observed_Δspeed| > MIN_DELTA_SPEED_MS (~5.56 m/s = 20 km/h).

Flagged case:
  prev speed = 20 m/s, prev accel = 5 m/s²
  curr speed = 40 m/s  →  observed_Δ = 20 m/s
  expected_Δ = 5 × 0.1 = 0.5 m/s  →  error = 19.5 m/s >> 5 m/s

Clean case: observed_Δ = 0.4 m/s < MIN_DELTA_SPEED_MS → skipped.
"""

import pytest
from detectors.speed_accel_consistency import SpeedAccelConsistencyDetector
from detectors.utils import SPEED_UNIT_MS, ACCEL_UNIT_MS2
from conftest import make_bsm


def _spd_raw(ms: float) -> int:
    return round(ms / SPEED_UNIT_MS)


def _acc_raw(ms2: float) -> int:
    return round(ms2 / ACCEL_UNIT_MS2)


@pytest.fixture
def det():
    return SpeedAccelConsistencyDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(secmark=0)) is None


def test_small_delta_skipped(det):
    # Δspeed = 0.4 m/s = 1.44 km/h < MIN_DELTA_SPEED_KMH (20) → not evaluated
    bsm1 = make_bsm(secmark=0)
    bsm1["payload"]["data"]["coreData"]["speed"]          = _spd_raw(20.0)
    bsm1["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    bsm2 = make_bsm(secmark=100)
    bsm2["payload"]["data"]["coreData"]["speed"]          = _spd_raw(20.4)
    bsm2["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    det.check(bsm1)
    assert det.check(bsm2) is None


def test_inconsistent_speed_accel_flags(det):
    # Speed jumps 20 m/s but accel only predicts 0.5 m/s change → error = 19.5 >> 5
    bsm1 = make_bsm(secmark=0)
    bsm1["payload"]["data"]["coreData"]["speed"]          = _spd_raw(20.0)
    bsm1["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    bsm2 = make_bsm(secmark=100)
    bsm2["payload"]["data"]["coreData"]["speed"]          = _spd_raw(40.0)
    bsm2["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    det.check(bsm1)
    result = det.check(bsm2)
    assert result is not None
    assert result["misbehavior"] == "speed_accel_inconsistency"
    assert result["error_ms"] > 5.0


def test_gap_too_small_returns_none(det):
    bsm1 = make_bsm(secmark=0)
    bsm1["payload"]["data"]["coreData"]["speed"]          = _spd_raw(20.0)
    bsm1["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    bsm2 = make_bsm(secmark=10)   # 10 ms < MIN_GAP_SECONDS (50 ms)
    bsm2["payload"]["data"]["coreData"]["speed"]          = _spd_raw(40.0)
    bsm2["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    det.check(bsm1)
    assert det.check(bsm2) is None


def test_missing_secmark_returns_none(det):
    bsm1 = make_bsm(secmark=0)
    bsm1["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    bsm2 = make_bsm(secmark=None)
    bsm2["payload"]["data"]["coreData"]["accelSet"]["long"] = _acc_raw(5.0)

    det.check(bsm1)
    assert det.check(bsm2) is None
