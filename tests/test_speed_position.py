"""Tests for SpeedPositionConsistencyDetector.

Geometry: at lat=41°, 1° lat ≈ 111 319 m.

Clean test uses tiny displacement (< MIN_DISTANCE_M = 5 m) so the pair is
skipped before any comparison.

Flagged test uses 0.002° north ≈ 222 m in 100 ms → implied ≈ 8 000 km/h,
reported 72 km/h → diff ≈ 7 928 km/h >> MAX_SPEED_DIFF_KMH (500).
"""

import pytest
from detectors.speed_position_consistency import SpeedPositionConsistencyDetector
from conftest import make_bsm

LAT1, LON1 = 41.0, -81.0


@pytest.fixture
def det():
    return SpeedPositionConsistencyDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(lat_deg=LAT1, secmark=0)) is None


def test_tiny_displacement_returns_none(det):
    # ~0.1 m — below MIN_DISTANCE_M (5 m), skipped
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, secmark=0))
    result = det.check(make_bsm(lat_deg=41.000001, lon_deg=LON1, secmark=100))
    assert result is None


def test_large_position_jump_flags(det):
    # 222 m in 100 ms, reported 72 km/h → diff >> 500 km/h
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, speed_kmh=72.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.002, lon_deg=LON1, speed_kmh=72.0, secmark=100))
    assert result is not None
    assert result["misbehavior"] == "speed_position_inconsistency"
    assert abs(result["diff_kmh"]) > 500.0


def test_both_speeds_below_floor_returns_none(det):
    # reported < 10 km/h AND implied < 10 km/h (tiny distance, tiny speed)
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, speed_kmh=5.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.000001, lon_deg=LON1, speed_kmh=5.0, secmark=100))
    assert result is None


def test_missing_secmark_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, secmark=0))
    result = det.check(make_bsm(lat_deg=41.002, lon_deg=LON1, secmark=None))
    assert result is None
