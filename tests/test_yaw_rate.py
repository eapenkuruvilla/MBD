"""Tests for YawRateConsistencyDetector.

The detector flags when |reported_yaw − gps_yaw_rate| > 90 °/s.

Common setup: both positions ~22 m apart (> MIN_DISTANCE_M = 5 m),
100 ms gap, speed 72 km/h.

GPS yaw rate = heading_change / elapsed.

Flagged case: heading changes 18° in 100 ms → GPS yaw = 180 °/s; reported = 0 °/s → diff = 180 >> 90.
Clean case:   heading changes 1° in 100 ms  → GPS yaw =  10 °/s; reported = 10 °/s → diff = 0.
"""

import pytest
from detectors.yaw_rate_consistency import YawRateConsistencyDetector
from conftest import make_bsm

LAT1, LON1 = 41.0, -81.0
LAT2, LON2 = 41.0002, -81.0   # ~22 m north


@pytest.fixture
def det():
    return YawRateConsistencyDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(lat_deg=LAT1, secmark=0)) is None


def test_consistent_yaw_returns_none(det):
    # heading Δ = 1° in 100 ms → GPS yaw = 10 °/s; reported yaw = 10 °/s → diff = 0
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0,
                        yaw_deg_s=10.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=1.0,
                                 yaw_deg_s=10.0, secmark=100))
    assert result is None


def test_inconsistent_yaw_flags(det):
    # heading Δ = 18° in 100 ms → GPS yaw = 180 °/s; reported yaw = 0 °/s → diff = 180 >> 90
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0,
                        yaw_deg_s=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=18.0,
                                 yaw_deg_s=0.0, secmark=100))
    assert result is not None
    assert result["misbehavior"] == "yaw_rate_inconsistency"
    assert result["yaw_diff_deg_s"] > 90.0


def test_below_min_speed_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=18.0,
                                 speed_kmh=5.0, yaw_deg_s=0.0, secmark=100))
    assert result is None


def test_too_little_displacement_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.000001, lon_deg=LON1, heading_deg=18.0,
                                 yaw_deg_s=0.0, secmark=100))
    assert result is None


def test_missing_secmark_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=18.0,
                                 yaw_deg_s=0.0, secmark=None))
    assert result is None
