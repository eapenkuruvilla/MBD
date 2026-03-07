import pytest
from detectors.accel import AccelDetector
from conftest import make_bsm
from detectors.utils import ACCEL_UNAVAILABLE


@pytest.fixture
def det(det_config):
    return AccelDetector(det_config.section("accel"))


def test_below_threshold_returns_none(det):
    assert det.check(make_bsm(accel_long_ms2=5.0)) is None


def test_at_threshold_returns_none(det):
    # 9.80665 m/s² (1.0 g) is not exactly representable in J2735 (1 LSB = 0.01 m/s²);
    # round(9.80665 / 0.01) = 981 → decodes to 9.81 m/s² = 1.0003 g and would flag.
    # Use raw 980 = 9.80 m/s² = 0.9993 g — the highest encodable value below the threshold.
    assert det.check(make_bsm(accel_long_ms2=9.80)) is None


def test_positive_accel_exceeded_flags(det):
    result = det.check(make_bsm(accel_long_ms2=25.0))
    assert result is not None
    assert result["misbehavior"] == "accel_exceeded"
    assert result["accel_g"] > 2.0


def test_negative_decel_exceeded_flags(det):
    result = det.check(make_bsm(accel_long_ms2=-25.0))
    assert result is not None
    assert result["misbehavior"] == "accel_exceeded"
    assert result["accel_g"] < -2.0


def test_unavailable_returns_none(det):
    bsm = make_bsm()
    bsm["payload"]["data"]["coreData"]["accelSet"]["long"] = ACCEL_UNAVAILABLE
    assert det.check(bsm) is None


def test_missing_field_returns_none(det):
    bsm = make_bsm()
    del bsm["payload"]["data"]["coreData"]["accelSet"]
    assert det.check(bsm) is None
