"""Tests for PositionJumpDetector.

Geometry note (lat=41°):
  1° lat ≈ 111 319 m
  0.001° lat ≈ 111 m   (used for the jump test)
  0.00001° lat ≈ 1.1 m (used for the tiny-move test)

secMark pairs are 100 ms apart (0 → 100), well within the [50, 150] ms window.
"""

import pytest
from detectors.position_jump import PositionJumpDetector
from conftest import make_bsm


@pytest.fixture
def det():
    return PositionJumpDetector()


def test_first_message_returns_none(det):
    assert det.check(make_bsm(secmark=0)) is None


def test_tiny_movement_returns_none(det):
    # ~1 m displacement — well below MIN_JUMP_METERS (100 m)
    det.check(make_bsm(lat_deg=41.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.00001, secmark=100))
    assert result is None


def test_large_jump_flags(det):
    # ~111 m north in 100 ms → implied ≈ 4 000 km/h >> 10 km/h threshold
    det.check(make_bsm(lat_deg=41.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.001, secmark=100))
    assert result is not None
    assert result["misbehavior"] == "position_jump"
    assert result["jump_m"] > 100.0
    assert result["implied_speed_kmh"] > 10.0


def test_gap_too_small_returns_none(det):
    # 10 ms gap < MIN_GAP_SECONDS (50 ms)
    det.check(make_bsm(lat_deg=41.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.001, secmark=10))
    assert result is None


def test_gap_too_large_returns_none(det):
    # 500 ms gap > MAX_GAP_SECONDS (150 ms)
    det.check(make_bsm(lat_deg=41.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.001, secmark=500))
    assert result is None


def test_missing_secmark_returns_none(det):
    det.check(make_bsm(lat_deg=41.0, secmark=0))
    result = det.check(make_bsm(lat_deg=41.001, secmark=None))
    assert result is None


def test_each_instance_is_independent():
    # State is per-instance; a fresh detector has no memory
    det1 = PositionJumpDetector()
    det2 = PositionJumpDetector()
    bsm_prev = make_bsm(lat_deg=41.0, secmark=0)
    bsm_jump = make_bsm(lat_deg=41.001, secmark=100)

    det1.check(bsm_prev)
    assert det1.check(bsm_jump) is not None   # det1 saw both
    assert det2.check(bsm_jump) is None        # det2 only saw the second
