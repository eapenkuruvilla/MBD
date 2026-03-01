"""
V2X BSM Misbehavior Detector

Reads BSM data and runs all registered detectors, writing a JSON-lines log
file suitable for ingestion by Logstash / ELK.

Input can be:
  - A plain NDJSON file
  - A ZIP archive containing one or more NDJSON data files at any depth;
    every non-directory entry in the archive is treated as a data file.

Usage:
    python detector.py <bsm_file_or_zip> [--log <log_file>]
"""

import argparse
import hashlib
import io
import json
import math
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from detectors import accel as accel_detector
from detectors import brakes_inconsistency as brakes_detector
from detectors import speed as speed_detector
from detectors.heading_change_rate import HeadingChangeRateDetector
from detectors.heading_inconsistency import HeadingInconsistencyDetector
from detectors.position_jump import PositionJumpDetector
from detectors.speed_accel_consistency import SpeedAccelConsistencyDetector
from detectors.speed_position_consistency import SpeedPositionConsistencyDetector
from detectors.yaw_rate_consistency import YawRateConsistencyDetector

# Register detectors here as more are added.
# Module-level detectors (stateless) and class instances (stateful) both work
# because detector.check(bsm) resolves to the module function or instance method.
DETECTORS = [
    speed_detector,
    accel_detector,
    brakes_detector,
    PositionJumpDetector(),
    HeadingInconsistencyDetector(),
    SpeedPositionConsistencyDetector(),
    SpeedAccelConsistencyDetector(),
    HeadingChangeRateDetector(),
    YawRateConsistencyDetector(),
]

LAT_SCALE = 1e-7   # BSM lat/long are integers × 1e-7 degrees
LON_SCALE = 1e-7

# Suppress duplicate map dots: same vehicle+type within this distance AND time.
COOLDOWN_METERS = 50.0
COOLDOWN_SECONDS = 30.0

# Progress line update interval (seconds).
PROGRESS_INTERVAL = 1.0
# Width used to overwrite previous progress lines (avoids leftover characters).
_PROGRESS_WIDTH = 110


def _fmt_eta(seconds: float) -> str:
    """Format a duration in seconds as a compact human-readable string."""
    seconds = int(seconds)
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _progress(msg: str) -> None:
    """Overwrite the current terminal line with msg, padded to a fixed width."""
    print(f"\r{msg:<{_PROGRESS_WIDTH}}", end="", flush=True)


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
    try:
        return datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def parse_args():
    parser = argparse.ArgumentParser(description="V2X BSM Misbehavior Detector")
    parser.add_argument(
        "bsm_file",
        help="Path to a NDJSON BSM data file or a ZIP archive of BSM data files",
    )
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


def _process_lines(lines, log_f, cooldown: dict, counts: dict,
                   report_progress: bool = False):
    """
    Core processing loop.  Runs all detectors over an iterable of raw text
    lines, writes flagged events to log_f, and updates the shared cooldown
    and counts dicts in place.

    When report_progress is True, a \r progress line is printed at most once
    per PROGRESS_INTERVAL seconds (used for plain-file mode).

    Returns (total_records, flagged, suppressed) for this batch of lines.
    """
    total = 0
    flagged = 0
    suppressed = 0
    t0 = time.monotonic()
    last_print = t0

    for line_num, line in enumerate(lines, start=1):
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="replace")
        line = line.strip()
        if not line:
            continue
        total += 1

        if report_progress:
            now = time.monotonic()
            if now - last_print >= PROGRESS_INTERVAL:
                elapsed = now - t0
                rate = total / elapsed if elapsed > 0 else 0
                _progress(
                    f"  Records: {total:>10,} | Flagged: {flagged:>7,} | {rate:>8,.0f} rec/s"
                )
                last_print = now

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
                    close_time = (
                        abs((bsm_time - prev_time).total_seconds()) <= COOLDOWN_SECONDS
                    )
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
            counts[result["misbehavior"]] += 1

    return total, flagged, suppressed


def process_input(bsm_path: Path, log_path: Path):
    """
    Process a plain NDJSON file or a ZIP archive.  Returns
    (total_records, total_flagged, total_suppressed, counts_by_type).
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    flagged = 0
    suppressed = 0
    counts = defaultdict(int)
    cooldown = {}

    with log_path.open("w") as log_f:
        if zipfile.is_zipfile(bsm_path):
            with zipfile.ZipFile(bsm_path) as zf:
                data_entries = [e for e in zf.infolist() if not e.filename.endswith("/")]
                n = len(data_entries)
                print(f"  ZIP contains {n:,} data file(s)")
                t0 = time.monotonic()
                last_print = t0
                for i, entry in enumerate(data_entries, start=1):
                    with zf.open(entry) as raw_f:
                        lines = io.TextIOWrapper(raw_f, encoding="utf-8", errors="replace")
                        t, fl, sup = _process_lines(lines, log_f, cooldown, counts)
                    total += t
                    flagged += fl
                    suppressed += sup

                    now = time.monotonic()
                    if now - last_print >= PROGRESS_INTERVAL or i == n:
                        elapsed = now - t0
                        rate = total / elapsed if elapsed > 0 else 0
                        pct = 100.0 * i / n
                        eta = (
                            _fmt_eta(elapsed / i * (n - i))
                            if i < n and elapsed > 0
                            else "done"
                        )
                        _progress(
                            f"  Files: {i:>{len(str(n))},}/{n:,} ({pct:5.1f}%)"
                            f" | Records: {total:>10,}"
                            f" | Flagged: {flagged:>7,}"
                            f" | {rate:>8,.0f} rec/s"
                            f" | ETA: {eta}"
                        )
                        last_print = now
                print()  # move past the progress line
        else:
            with bsm_path.open() as f:
                t, fl, sup = _process_lines(f, log_f, cooldown, counts,
                                            report_progress=True)
            print()  # move past the progress line
            total += t
            flagged += fl
            suppressed += sup

    return total, flagged, suppressed, counts


def _print_summary(total, flagged, suppressed, counts):
    print(f"\nProcessed : {total:,} records")
    print(f"Flagged   : {flagged:,} misbehaviors ({suppressed:,} suppressed as nearby duplicates)")
    if counts:
        print("\nMisbehaviors by type:")
        width = max(len(k) for k in counts)
        for mtype, cnt in sorted(counts.items()):
            print(f"  {mtype:<{width}}  {cnt:>6,}")
    else:
        print("\nNo misbehaviors detected.")


def main():
    args = parse_args()
    bsm_path = Path(args.bsm_file)
    log_path = Path(args.log)

    if not bsm_path.exists():
        print(f"Error: file not found: {bsm_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Input  : {bsm_path}")
    print(f"Log    : {log_path}")

    total, flagged, suppressed, counts = process_input(bsm_path, log_path)
    _print_summary(total, flagged, suppressed, counts)


if __name__ == "__main__":
    main()
