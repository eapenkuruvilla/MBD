"""
V2X BSM Misbehavior Detector

Reads a NDJSON BSM data file, runs all registered detectors, and writes
a JSON-lines log file (one entry per misbehavior detected) suitable for
ingestion by Logstash / ELK.

Usage:
    python detector.py <bsm_file> [--log <log_file>]
"""

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

from detectors import accel as accel_detector
from detectors import brakes_inconsistency as brakes_detector
from detectors import speed as speed_detector
from detectors.heading_inconsistency import HeadingInconsistencyDetector
from detectors.position_jump import PositionJumpDetector

# Register detectors here as more are added.
# Module-level detectors (stateless) and class instances (stateful) both work
# because detector.check(bsm) resolves to the module function or instance method.
DETECTORS = [
    speed_detector,
    accel_detector,
    brakes_detector,
    PositionJumpDetector(),
    HeadingInconsistencyDetector(),
]

LAT_SCALE = 1e-7   # BSM lat/long are integers × 1e-7 degrees
LON_SCALE = 1e-7

# Suppress duplicate map dots: same vehicle+type within this distance AND time.
COOLDOWN_METERS = 50.0
COOLDOWN_SECONDS = 30.0


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_bsm_time(ts: str):
    """Parse recordGeneratedAt to a datetime; return None on failure.

    Handles formats seen in USDOT CV Pilot data, e.g.:
      '2020-05-06 07:06:03.419 [ET]'
      '2020-05-06T07:06:03.419Z'
      epoch-milliseconds as a string
    """
    if not ts:
        return None
    # Strip trailing timezone label like " [ET]", " [UTC]", etc.
    clean = ts.split("[")[0].strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(clean, fmt)
        except ValueError:
            pass
    # ISO 8601 with explicit offset
    try:
        return datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        pass
    # Epoch milliseconds
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def parse_args():
    parser = argparse.ArgumentParser(description="V2X BSM Misbehavior Detector")
    parser.add_argument("bsm_file", help="Path to NDJSON BSM data file")
    parser.add_argument(
        "--log",
        default="logs/misbehaviors.log",
        help="Output log file path (default: logs/misbehaviors.log)",
    )
    return parser.parse_args()


def extract_context(bsm: dict) -> dict:
    """Pull common fields used by all log entries."""
    meta = bsm.get("metadata", {})
    core = bsm.get("payload", {}).get("data", {}).get("coreData", {})

    lat_raw = core.get("lat")
    lon_raw = core.get("long")

    return {
        "record_generated_at": meta.get("recordGeneratedAt", ""),
        "rsu_id": meta.get("RSUID", ""),
        "bsm_source": meta.get("bsmSource", ""),
        "vehicle_id": core.get("id", ""),
        "msg_cnt": core.get("msgCnt", ""),
        "lat": round(int(lat_raw) * LAT_SCALE, 7) if lat_raw is not None else None,
        "lon": round(int(lon_raw) * LON_SCALE, 7) if lon_raw is not None else None,
    }


def process_file(bsm_path: Path, log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    flagged = 0
    suppressed = 0
    # cooldown[(vehicle_id, misbehavior_type)] = (lat, lon, bsm_datetime)
    cooldown = {}

    with bsm_path.open() as bsm_f, log_path.open("w") as log_f:
        for line_num, line in enumerate(bsm_f, start=1):
            line = line.strip()
            if not line:
                continue
            total += 1

            try:
                bsm = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"[WARN] line {line_num}: JSON parse error – {exc}", file=sys.stderr)
                continue

            context = extract_context(bsm)

            for detector in DETECTORS:
                result = detector.check(bsm)
                if result is None:
                    continue

                key = (context["vehicle_id"], result["misbehavior"])
                lat, lon = context.get("lat"), context.get("lon")
                bsm_time = _parse_bsm_time(context.get("record_generated_at", ""))
                prev = cooldown.get(key)

                if prev is not None and lat is not None and lon is not None:
                    prev_lat, prev_lon, prev_time = prev
                    close_space = _haversine_m(lat, lon, prev_lat, prev_lon) <= COOLDOWN_METERS
                    if bsm_time is not None and prev_time is not None:
                        close_time = abs((bsm_time - prev_time).total_seconds()) <= COOLDOWN_SECONDS
                    else:
                        close_time = False
                    if close_space and close_time:
                        suppressed += 1
                        continue

                cooldown[key] = (lat, lon, bsm_time)

                event_id = hashlib.sha1(
                    f"{context['vehicle_id']}|{context['record_generated_at']}|{result['misbehavior']}".encode()
                ).hexdigest()[:16]
                log_entry = {
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "event_id": event_id,
                    **context,
                    **result,
                }
                log_f.write(json.dumps(log_entry) + "\n")
                flagged += 1

    return total, flagged, suppressed


def main():
    args = parse_args()
    bsm_path = Path(args.bsm_file)
    log_path = Path(args.log)

    if not bsm_path.exists():
        print(f"Error: BSM file not found: {bsm_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Processing: {bsm_path}")
    print(f"Log output: {log_path}")

    total, flagged, suppressed = process_file(bsm_path, log_path)

    print(f"Done. Processed {total} records, flagged {flagged} misbehaviors ({suppressed} suppressed as nearby duplicates).")


if __name__ == "__main__":
    main()
