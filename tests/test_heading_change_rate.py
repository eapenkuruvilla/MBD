"""Tests for HeadingChangeRateDetector.

The detector flags when |heading_diff| / elapsed_s > 90 °/s.

Common setup: both messages at ~22 m apart (distance > MIN_DISTANCE_M = 5 m),
100 ms gap (within [50, 150] ms), speed 72 km/h.
"""

import pytest
from detectors.heading_change_rate import HeadingChangeRateDetector
from conftest import make_bsm

LAT1, LON1 = 41.0, -81.0
LAT2, LON2 = 41.0002, -81.0   # ~22 m north


@pytest.fixture
def det():
    return HeadingChangeRateDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(lat_deg=LAT1, secmark=0)) is None


def test_small_heading_change_returns_none(det):
    # 1° change in 100 ms → 10 °/s ≤ 90 °/s threshold
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=1.0, secmark=100))
    assert result is None


def test_implausible_heading_rate_flags(det):
    # 180° change in 100 ms → 1 800 °/s >> 90 °/s
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=180.0, secmark=100))
    assert result is not None
    assert result["misbehavior"] == "implausible_heading_change_rate"
    assert result["heading_rate_deg_s"] > 90.0


def test_below_min_speed_returns_none(det):
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=LAT2, lon_deg=LON2, heading_deg=180.0,
                                 speed_kmh=5.0, secmark=100))
    assert result is None


def test_too_little_displacement_returns_none(det):
    # ~0.1 m movement — below MIN_DISTANCE_M (5 m)
    det.check(make_bsm(lat_deg=LAT1, lon_deg=LON1, heading_deg=0.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.000001, lon_deg=LON1, heading_deg=180.0, secmark=100))
    assert result is None
