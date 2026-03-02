from detectors import speed as det
from conftest import make_bsm
from detectors.utils import SPEED_UNAVAILABLE


def test_below_threshold_returns_none():
    assert det.check(make_bsm(speed_kmh=100.0)) is None


def test_at_threshold_returns_none():
    # 200.0 km/h is not exactly representable in J2735 (1 LSB = 0.072 km/h);
    # round(200.0 / 0.072) = 2778 → decodes to 200.016 km/h and would flag.
    # Use raw 2777 = 199.944 km/h — the highest encodable value below the threshold.
    assert det.check(make_bsm(speed_kmh=199.944)) is None


def test_above_threshold_flags():
    result = det.check(make_bsm(speed_kmh=250.0))
    assert result is not None
    assert result["misbehavior"] == "speed_exceeded"
    assert result["speed_kmh"] > 200.0


def test_unavailable_returns_none():
    bsm = make_bsm()
    bsm["payload"]["data"]["coreData"]["speed"] = SPEED_UNAVAILABLE
    assert det.check(bsm) is None


def test_missing_field_returns_none():
    bsm = make_bsm()
    del bsm["payload"]["data"]["coreData"]["speed"]
    assert det.check(bsm) is None
