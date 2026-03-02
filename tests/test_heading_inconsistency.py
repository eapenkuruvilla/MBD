"""Tests for HeadingInconsistencyDetector.

The detector compares the reported heading against the GPS-derived bearing
between consecutive positions.  MAX_HEADING_DIFF_DEG = 90°.

Setup for each stateful test:
  - Message 1: anchor position (41.0°, -81.0°), heading = test value
  - Message 2: moved ~22 m north (lat += 0.0002°), heading = test value
    → GPS bearing ≈ 0° (due north)

All tests use speed = 72 km/h (> MIN_SPEED_KMH = 10) and
secmark 0 → 100 (100 ms gap, within [50, 150] ms window).
"""

import pytest
from detectors.heading_inconsistency import HeadingInconsistencyDetector
from conftest import make_bsm

LAT1, LON1 = 41.0, -81.0
LAT2, LON2 = 41.0002, -81.0   # ~22 m north; GPS bearing ≈ 0°


@pytest.fixture
def det():
    return HeadingInconsistencyDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, secmark=0)) is None


def test_consistent_heading_returns_none(det):
    # Reported 0° (north), moving north → diff ≈ 0° ≤ 20°
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=0.0, secmark=100))
    assert result is None


def test_inconsistent_heading_flags(det):
    # Moving north (bearing ≈ 0°) but reports 135° → diff = 135° > MAX (90°)
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=135.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=135.0, secmark=100))
    assert result is not None
    assert result["misbehavior"] == "heading_inconsistency"
    assert result["heading_diff"] > 90.0


def test_below_min_speed_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, secmark=0))
    # speed = 5 km/h < MIN_SPEED_KMH (10)
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, speed_kmh=5.0,
                                 heading_deg=135.0, secmark=100))
    assert result is None


def test_too_little_displacement_returns_none(det):
    # ~0.1 m movement — below MIN_DISTANCE_M (5 m)
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, secmark=0))
    result = det.check(make_bsm(lat_deg=41.000001, lon_deg=LON1,
                                 heading_deg=90.0, secmark=100))
    assert result is None
