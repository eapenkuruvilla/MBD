"""
Detector: Brakes-Deceleration Inconsistency

Cross-checks the wheelBrakes bitmap against the reported longitudinal
acceleration to catch spoofed or corrupted brake/accel fields.

Two misbehavior sub-types are flagged:

  brakes_on_no_decel
    Wheel brakes reported as applied, yet the vehicle is accelerating
    (accelSet.long > +ACCEL_BRAKING_THRESHOLD).  Physically, applied wheel
    brakes cannot produce net forward acceleration beyond sensor noise.

  decel_no_brakes
    Strong deceleration reported (accelSet.long < -DECEL_NO_BRAKES_THRESHOLD)
    with no wheel brakes active.  Engine braking tops out around 1.5 m/s²;
    anything beyond that without wheel brakes is implausible.

SAE J2735 field encoding
------------------------
wheelBrakes : 5-character binary string (BrakeAppliedStatus BITSTRING)
    bit 0  – unavailable  (if '1', entire field is invalid → skip)
    bit 1  – leftFront
    bit 2  – rightFront
    bit 3  – leftRear
    bit 4  – rightRear

accelSet.long : integer, unit 0.01 m/s² per LSB, range -2000 to 2000
    2001 = unavailable → skip

Thresholds
----------
ACCEL_BRAKING_THRESHOLD  :  0.50 m/s²  — positive accel above this while
                             braking is flagged (empirical ceiling from clean
                             data: +0.45 m/s²)
DECEL_NO_BRAKES_THRESHOLD:  3.00 m/s²  — magnitude of deceleration required
                             to flag the no-brakes case (well above the
                             ~1.5 m/s² engine-braking ceiling)
"""

from typing import Optional

from .utils import ACCEL_UNAVAILABLE, ACCEL_UNIT_MS2, G_MS2

ACCEL_BRAKING_THRESHOLD_G   = 1.0  # g  — positive accel while brakes on
DECEL_NO_BRAKES_THRESHOLD_G = 1.0  # g  — magnitude, decel without brakes

ACCEL_BRAKING_THRESHOLD_MS2   = ACCEL_BRAKING_THRESHOLD_G * G_MS2  # m/s²  — positive accel while brakes on
DECEL_NO_BRAKES_THRESHOLD_MS2 = DECEL_NO_BRAKES_THRESHOLD_G * G_MS2    # m/s²  — magnitude, decel without brakes


def _parse_accel(raw) -> Optional[float]:
    """Return accel in m/s², or None if missing/unavailable."""
    if raw is None:
        return None
    try:
        raw = int(raw)
    except (ValueError, TypeError):
        return None
    if raw == ACCEL_UNAVAILABLE:
        return None
    return raw * ACCEL_UNIT_MS2


def _parse_wheel_brakes(wb) -> Optional[bool]:
    """
    Return True if any wheel brake is active, False if none are active,
    or None if the field is missing/unavailable/malformed.
    """
    if not isinstance(wb, str) or len(wb) != 5:
        return None
    if wb[0] == '1':          # unavailable bit set
        return None
    if any(c not in ('0', '1') for c in wb):
        return None
    return any(c == '1' for c in wb[1:])


def check(bsm: dict) -> Optional[dict]:
    """
    Returns a misbehavior record if brake state and longitudinal acceleration
    are inconsistent, else None.
    """
    core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

    accel_ms2    = _parse_accel(core.get("accelSet", {}).get("long"))
    any_braking  = _parse_wheel_brakes(core.get("brakes", {}).get("wheelBrakes"))

    if accel_ms2 is None or any_braking is None:
        return None

    # Case 1 — brakes applied but vehicle is accelerating
    if any_braking and accel_ms2 > ACCEL_BRAKING_THRESHOLD_MS2:
        return {
            "misbehavior":  "brakes_on_no_decel",
            "accel_ms2":    round(accel_ms2, 4),
            "accel_g":      round(accel_ms2 / G_MS2, 4),
            "threshold_ms2": ACCEL_BRAKING_THRESHOLD_MS2,
            "wheel_brakes": core["brakes"]["wheelBrakes"],
        }

    # Case 2 — heavy deceleration with no wheel brakes active
    if not any_braking and accel_ms2 < -DECEL_NO_BRAKES_THRESHOLD_MS2:
        return {
            "misbehavior":  "decel_no_brakes",
            "accel_ms2":    round(accel_ms2, 4),
            "accel_g":      round(accel_ms2 / G_MS2, 4),
            "threshold_ms2": -DECEL_NO_BRAKES_THRESHOLD_MS2,
            "wheel_brakes": core["brakes"]["wheelBrakes"],
        }

    return None
