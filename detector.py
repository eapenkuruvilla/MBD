"""
V2X BSM Misbehavior Detector

Reads a NDJSON BSM data file, runs all registered detectors, and writes
a JSON-lines log file (one entry per misbehavior detected) suitable for
ingestion by Logstash / ELK.

Usage:
    python detector.py <bsm_file> [--log <log_file>]
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from detectors import accel as accel_detector
from detectors import speed as speed_detector

# Register detectors here as more are added.
DETECTORS = [
    speed_detector,
    accel_detector,
]

LAT_SCALE = 1e-7   # BSM lat/long are integers × 1e-7 degrees
LON_SCALE = 1e-7


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

                log_entry = {
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    **context,
                    **result,
                }
                log_f.write(json.dumps(log_entry) + "\n")
                flagged += 1

    return total, flagged


def main():
    args = parse_args()
    bsm_path = Path(args.bsm_file)
    log_path = Path(args.log)

    if not bsm_path.exists():
        print(f"Error: BSM file not found: {bsm_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Processing: {bsm_path}")
    print(f"Log output: {log_path}")

    total, flagged = process_file(bsm_path, log_path)

    print(f"Done. Processed {total} records, flagged {flagged} misbehaviors.")


if __name__ == "__main__":
    main()
