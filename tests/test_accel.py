from detectors import accel as det
from conftest import make_bsm
from detectors.utils import ACCEL_UNAVAILABLE


def test_below_threshold_returns_none():
    assert det.check(make_bsm(accel_long_ms2=5.0)) is None


def test_at_threshold_returns_none():
    # 9.80665 m/s² (1.0 g) is not exactly representable in J2735 (1 LSB = 0.01 m/s²);
    # round(9.80665 / 0.01) = 981 → decodes to 9.81 m/s² = 1.0003 g and would flag.
    # Use raw 980 = 9.80 m/s² = 0.9993 g — the highest encodable value below the threshold.
    assert det.check(make_bsm(accel_long_ms2=9.80)) is None


def test_positive_accel_exceeded_flags():
    result = det.check(make_bsm(accel_long_ms2=12.0))
    assert result is not None
    assert result["misbehavior"] == "accel_exceeded"
    assert result["accel_g"] > 1.0


def test_negative_decel_exceeded_flags():
    result = det.check(make_bsm(accel_long_ms2=-12.0))
    assert result is not None
    assert result["misbehavior"] == "accel_exceeded"
    assert result["accel_g"] < -1.0


def test_unavailable_returns_none():
    bsm = make_bsm()
    bsm["payload"]["data"]["coreData"]["accelSet"]["long"] = ACCEL_UNAVAILABLE
    assert det.check(bsm) is None


def test_missing_field_returns_none():
    bsm = make_bsm()
    del bsm["payload"]["data"]["coreData"]["accelSet"]
    assert det.check(bsm) is None
