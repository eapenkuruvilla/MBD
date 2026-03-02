# V2X BSM Misbehavior Detection (MBD)

A Python + ELK-stack pipeline that ingests SAE J2735 Basic Safety Messages (BSMs)
from connected vehicles, runs a suite of physics-based detectors to flag
anomalous behaviour, and visualises findings in Kibana.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Workflow: Ingesting New Data](#workflow-ingesting-new-data)
5. [Workflow: Wiping Data and Starting Fresh](#workflow-wiping-data-and-starting-fresh)
6. [Makefile Reference](#makefile-reference)
7. [Detectors](#detectors)
8. [Threshold System (L1 / L2)](#threshold-system-l1--l2)
9. [Kibana Dashboards](#kibana-dashboards)
10. [Troubleshooting](#troubleshooting)
11. [Development: Unit Tests](#development-unit-tests)
12. [Project Structure](#project-structure)

---

## Architecture

```
BSM data file(s)
(NDJSON or ZIP)
       │
       ▼
 detector.py          ← Python: runs all 9 detectors, one BSM at a time
       │
       │  logs/misbehaviors.log  (JSON-lines, one event per line)
       ▼
  Logstash             ← Filebeat watches the log; Logstash parses & ships
       │
       ▼
Elasticsearch          ← Index: mbd-misbehaviors-YYYY.MM.dd
       │
       ├─ mbd-display alias  ← L2 filter from thresholds.json
       │                        (manage_display_filter.py)
       ▼
   Kibana               ← Dashboards, KPI panels, interactive Controls sliders
```

**Key components:**

| Component | Role |
|---|---|
| `detector.py` | Reads BSMs, runs detectors, writes `logs/misbehaviors.log` |
| `detectors/` | Nine physics-based detector modules |
| `logs/misbehaviors.log` | JSON-lines event log consumed by Logstash |
| Logstash | Ships log lines to Elasticsearch; creates date-stamped indices |
| Elasticsearch | Stores events; hosts the `mbd-display` filtered alias |
| Kibana | Dashboards and Controls sliders for interactive exploration |
| `manage_display_filter.py` | Pushes L2 filter from `thresholds.json` into ES as an alias |
| `thresholds.json` | Editable per-type L2 display thresholds |
| `Makefile` | Single entry point for all common operations |

All ELK services run in Docker (`docker-compose.yml`).  `detector.py` runs
locally (outside Docker) and writes to `logs/`, which is volume-mounted into
the Logstash container.

---

## Prerequisites

- Docker + Docker Compose
- Python 3.9+ with dependencies:

```bash
pip install -r requirements.txt
```

`requirements.txt` includes `elasticsearch>=8,<9` and `pytest>=8.0`.

---

## Quick Start

```bash
# 1. Start the ELK stack (first run pulls images; takes a few minutes)
docker compose up -d

# 2. Detect misbehaviors in a BSM data file
make run DATA=data/tampa_BSM_2021.zip

# 3. Restart Logstash so it ingests the new log entries
make ingest

# 4. Push the L2 display filter to Elasticsearch
make filter

# 5. Open Kibana and select the "MBD Misbehaviors" dashboard
#    http://localhost:5601
```

Or run all three steps at once:

```bash
make full DATA=data/tampa_BSM_2021.zip
```

---

## Workflow: Ingesting New Data

### Step-by-step

```bash
# Run detectors (appends to the existing log by default)
make run DATA=data/new_data.zip

# Tell Logstash to re-read the log
make ingest

# Refresh the L2 alias filter (only needed if thresholds.json changed)
make filter
```

### What happens under the hood

1. `detector.py` reads every BSM in the file or ZIP archive.
2. Each BSM is passed through all 9 detectors.  When a detector fires, a
   JSON event is written to `logs/misbehaviors.log`.
3. A cooldown mechanism suppresses duplicate events for the same vehicle
   and misbehavior type within 50 m / 30 s (prevents map dot spam).
4. Restarting Logstash (`make ingest`) causes Filebeat to re-read the log
   from the beginning and ship all lines to Elasticsearch.
5. Logstash creates or appends to today's date-stamped index
   (`mbd-misbehaviors-YYYY.MM.dd`).
6. Kibana's `mbd-misbehaviors*` data view covers all daily indices
   automatically.

### Checking progress while `detector.py` runs

The terminal displays a live progress line:

```
  Files: 142/500 ( 28.4%) | Records:  1,423,719 | Flagged:  3,218 | 52,340 rec/s | ETA: 4m 12s
  spd:14  acc:22  pos:3,180  hdg:0  sac:2
```

The second line shows counts by misbehavior abbreviation:
`spd` speed_exceeded, `acc` accel_exceeded, `brk+` brakes_on_no_decel,
`brk-` decel_no_brakes, `pos` position_jump, `hdg` heading_inconsistency,
`spc` speed_position_inconsistency, `sac` speed_accel_inconsistency,
`hcr` implausible_heading_change_rate, `yaw` yaw_rate_inconsistency.

---

## Workflow: Wiping Data and Starting Fresh

Use this when you want to discard **today's** results and re-run a clean
analysis (e.g., after changing detector thresholds or fixing a bug).

> **Scope:** `make fresh` only removes today's ES index.  Indices from
> previous dates remain in Elasticsearch and will still appear in Kibana.
> To wipe all historical data see [Deleting all historical indices](#deleting-all-historical-indices) below.

```bash
make fresh DATA=data/tampa_BSM_2021.zip
```

This single command:
1. Deletes today's Elasticsearch index (`mbd-misbehaviors-YYYY.MM.dd`).
2. Truncates `logs/misbehaviors.log` (via `--clear` flag on `detector.py`).
3. Runs `detector.py` on the specified data file.
4. Restarts Logstash to re-ingest the fresh log.
5. Pushes the L2 display filter to Elasticsearch.

### Deleting all historical indices

`make fresh` only deletes today's index.  To wipe all MBD indices across all
dates, run:

```bash
# List all MBD indices
curl http://localhost:9200/mbd-misbehaviors*?pretty

# Delete them one by one (wildcards are blocked by ES safety setting)
curl -X DELETE http://localhost:9200/mbd-misbehaviors-2024.06.01
curl -X DELETE http://localhost:9200/mbd-misbehaviors-2024.06.02
# ... repeat for each index shown above ...

# Re-establish the alias after deleting all indices
make filter
```

---

## Makefile Reference

```
make run    [DATA=<file>]   Detect misbehaviors; append to log
make filter                 Push thresholds.json → ES display alias
make ingest                 Restart Logstash to ingest the latest log
make full   [DATA=<file>]   run + ingest + filter
make clear                  Delete today's ES index
make fresh  [DATA=<file>]   clear + run (truncating log) + ingest + filter
make test                   Run pytest unit tests
make help                   Print this list
```

`DATA` defaults to the first `*.zip` or `*.json` file found in `data/`.

Override defaults:

```bash
make run DATA=data/custom.zip LOG=logs/custom.log
make filter ES=http://remote-host:9200
```

---

## Detectors

All detectors read from `payload.data.coreData` (SAE J2735 BSM core data).
Fields are integers encoded per J2735 unit conventions; the detector code
converts them to SI / human-readable units.

### Stateless detectors (single-message checks)

#### 1. Speed Exceeded (`speed_exceeded`)

**Field:** `coreData.speed`
**Unit:** 0.02 m/s per LSB · 3.6 = km/h
**Threshold:** > 200 km/h

Flags any BSM where the reported speed exceeds a physically implausible
absolute limit.  The L2 threshold raises this to 200 km/h (same as L1 here)
to retain all hits in the display filter by default.

**Output fields:** `speed_kmh`, `threshold_kmh`, `speed_raw`

---

#### 2. Acceleration Exceeded (`accel_exceeded`)

**Field:** `coreData.accelSet.long`
**Unit:** 0.01 m/s² per LSB
**Threshold:** |accel| > 1.0 g (9.81 m/s²)

Flags BSMs reporting longitudinal acceleration or deceleration beyond the
physical limit of tyre-road friction for a road vehicle.  Covers both
hard-braking and implausible forward acceleration.

**Output fields:** `accel_g`, `accel_ms2`, `threshold_g`, `accel_raw`

---

#### 3. Brakes–Deceleration Inconsistency (`brakes_on_no_decel` / `decel_no_brakes`)

**Fields:** `coreData.brakes.wheelBrakes` (5-bit bitmap), `coreData.accelSet.long`

Two sub-types are detected:

- **`brakes_on_no_decel`** — wheel brakes reported as applied, yet the
  vehicle is accelerating beyond 1.0 g.  Applied wheel brakes cannot produce
  net forward acceleration; this combination is physically impossible.

- **`decel_no_brakes`** — heavy deceleration (> 1.0 g magnitude) reported
  with no wheel brakes active.  Engine braking tops out around 0.15 g;
  anything harder requires wheel brakes.

The `wheelBrakes` unavailable bit (bit 0) causes the entire message to be
skipped so corrupted bitmap values do not generate spurious flags.

**Output fields:** `accel_g`, `accel_ms2`, `threshold_ms2`, `wheel_brakes`

---

### Stateful detectors (compare consecutive messages per vehicle)

Stateful detectors maintain a `_last` dictionary keyed by vehicle ID
(`coreData.id`).  They pair each incoming BSM with the previous one from
the same vehicle and use the **secMark** field (`coreData.secMark`) for
elapsed-time calculations.

**secMark** is a J2735 DSSecond value: milliseconds within the current
minute, 0–59999.  Value 65535 = unavailable.  The helper
`_secmark_elapsed_s(prev, curr)` handles minute wraparound via modulo 60 000.

All stateful detectors apply a **timing window** filter:

| Guard | Value | Purpose |
|---|---|---|
| `MIN_GAP_SECONDS` | 0.05 s | Reject near-simultaneous pairs (timing artifacts) |
| `MAX_GAP_SECONDS` | 0.15 s | Reject large gaps; vehicle may have turned or moved |

BSMs are transmitted at ~10 Hz (≈ 100 ms apart), so valid consecutive pairs
land squarely in the 50–150 ms window.

---

#### 4. Position Jump (`position_jump`)

**Fields:** `lat`, `long`, `secMark`
**Threshold:** implied speed > 10 km/h AND displacement > 100 m in one interval

Calculates the Haversine distance between consecutive positions of the same
vehicle.  A jump larger than 100 m in ≤ 150 ms implies a speed impossible
for a ground vehicle — a strong indicator of position spoofing or a GPS
outlier.

**Output fields:** `jump_m`, `elapsed_s`, `implied_speed_kmh`, `threshold_kmh`,
`prev_lat`, `prev_lon`

---

#### 5. Heading Inconsistency (`heading_inconsistency`)

**Fields:** `heading`, `lat`, `long`, `speed`, `secMark`
**Threshold:** |reported heading − GPS-derived bearing| > 90°
**Minimum speed:** 10 km/h
**Minimum displacement:** 5 m

Derives the true bearing of motion from GPS displacement and compares it
against the reported heading.  A discrepancy larger than 90° means the
vehicle claims to be pointing in a direction more than perpendicular to its
actual movement — a strong sign of heading field spoofing.

**Output fields:** `reported_heading`, `gps_bearing`, `heading_diff`,
`threshold_deg`, `speed_kmh`, `distance_m`

---

#### 6. Speed–Position Consistency (`speed_position_inconsistency`)

**Fields:** `speed`, `lat`, `long`, `secMark`
**Threshold:** |reported speed − implied speed| > 500 km/h
**Minimum speed (either):** 10 km/h
**Minimum displacement:** 5 m

Computes implied speed from GPS displacement ÷ elapsed time and compares it
against the reported speed field.  A large discrepancy in either direction is
suspicious:

- **`reported_exceeds_implied`** — speed field inflated (ghost-vehicle attack)
- **`implied_exceeds_reported`** — position jumps faster than speed claims

**Output fields:** `direction`, `reported_speed_kmh`, `implied_speed_kmh`,
`diff_kmh`, `threshold_kmh`, `distance_m`, `elapsed_s`

---

#### 7. Speed–Acceleration Consistency (`speed_accel_inconsistency`)

**Fields:** `speed`, `accelSet.long`, `secMark`
**Threshold:** |observed Δspeed − expected Δspeed| > 5 m/s
**Minimum Δspeed:** 20 km/h (filters near-constant-speed segments)

Newton's second law says speed change ≈ acceleration × time.  The detector
computes `expected_Δspeed = prev_accel × elapsed_s` and compares it against
the actual speed change between messages.  An error larger than 5 m/s means
at least one of the three fields (speed, acceleration, timestamp) is spoofed
or severely corrupted.

**Output fields:** `observed_delta_ms`, `expected_delta_ms`, `error_ms`,
`error_kmh`, `threshold_ms`, `accel_ms2`, `elapsed_s`

---

#### 8. Implausible Heading Change Rate (`implausible_heading_change_rate`)

**Fields:** `heading`, `lat`, `long`, `speed`, `secMark`
**Threshold:** heading change rate > 90 °/s
**Minimum speed:** 10 km/h
**Minimum displacement:** 5 m

Divides the angular heading change by elapsed time to get a turning rate in
°/s.  Tyre-friction physics limit how fast a road vehicle can yaw; the
empirical maximum in clean BSM data is ≈ 65.9 °/s.  Rates above 90 °/s
indicate a spoofed heading sequence.

**Output fields:** `heading_rate_deg_s`, `threshold_deg_s`, `heading_diff_deg`,
`elapsed_s`, `speed_kmh`

---

#### 9. Yaw Rate Consistency (`yaw_rate_inconsistency`)

**Fields:** `heading`, `accelSet.yaw`, `lat`, `long`, `speed`, `secMark`
**Threshold:** |reported yaw rate − GPS-derived yaw rate| > 90 °/s
**Minimum speed:** 10 km/h
**Minimum displacement:** 5 m

Compares the gyroscope yaw rate (`accelSet.yaw`, signed °/s) against the
heading change rate derived from consecutive GPS positions.  The two values
should agree in both magnitude and sign (positive = right turn in SAE J2735).
A large disagreement means the inertial sensor output and the position/heading
fields are mutually inconsistent — a strong signal of sensor injection or
data fabrication.

**Output fields:** `reported_yaw_deg_s`, `gps_yaw_rate_deg_s`, `yaw_diff_deg_s`,
`threshold_deg_s`, `elapsed_s`, `speed_kmh`

---

## Threshold System (L1 / L2)

The system uses two filtering levels:

| Level | Description | Where configured |
|---|---|---|
| **L1** | Detector fires: the physics check failed | Threshold constants in each `detectors/*.py` file |
| **L2** | Display filter: higher-confidence subset shown in Kibana | `thresholds.json` → `mbd-display` ES alias |

**L1** produces all flagged events in `logs/misbehaviors.log` and in the
raw `mbd-misbehaviors-*` indices.

**L2** is a server-side Elasticsearch alias filter.  Kibana's default data
view (`mbd-display`) points at this alias, so analysts see only the
higher-significance slice without re-ingesting data.

### Adjusting L2 thresholds

1. Edit `thresholds.json` (values are in km/h, m, g, °, °/s as labelled).
2. Push the new filter — no data re-ingestion needed:

```bash
make filter
# or directly:
python manage_display_filter.py
```

3. Refresh Kibana.

To inspect the currently active alias filter:

```bash
python manage_display_filter.py --show
```

To preview the filter JSON without pushing:

```bash
python manage_display_filter.py --dry-run
```

To switch between L1 (all events) and L2 (filtered) in Kibana, change the
**data view** in the Kibana toolbar:

- `mbd-misbehaviors*` — all L1 events
- `mbd-display` — L2 filtered events

---

## Kibana Dashboards

Two dashboards are imported automatically on first `docker compose up`:

| Dashboard | Data view | Contents |
|---|---|---|
| **MBD Misbehaviors** | `mbd-misbehaviors*` (all L1 events) | Maps, time series, breakdown tables |
| **MBD Display** | `mbd-display` (L2 filtered) | Same layout, higher-confidence subset |

### Interactive Controls sliders

Run once after the stack is up to add range-slider Controls to the MBD
dashboard:

```bash
python manage_display_filter.py --setup-kibana
```

This adds 10 interactive controls at the top of the dashboard:

| Control | Field | Type |
|---|---|---|
| Misbehavior Type | `misbehavior` | Dropdown |
| Speed (km/h) | `speed_kmh` | Range slider |
| Acceleration (g) | `accel_g` | Range slider |
| Position Jump (m) | `jump_m` | Range slider |
| Implied Speed (km/h) | `implied_speed_kmh` | Range slider |
| Heading Diff (°) | `heading_diff` | Range slider |
| Heading Rate (°/s) | `heading_rate_deg_s` | Range slider |
| Speed Diff (km/h) | `diff_kmh` | Range slider |
| Accel Error (km/h) | `error_kmh` | Range slider |
| Yaw Diff (°/s) | `yaw_diff_deg_s` | Range slider |

Dragging a slider instantly filters the dashboard panels without touching
the underlying data.  The Controls layer is client-side and on top of
whichever L1/L2 data view is selected.

---

## Troubleshooting

### Elasticsearch

```bash
# Cluster health
curl http://localhost:9200/_cat/health?v

# List all MBD indices with document counts
curl http://localhost:9200/_cat/indices/mbd-misbehaviors*?v&h=index,docs.count,store.size

# Check the mbd-display alias and its filter
python manage_display_filter.py --show
# or
curl http://localhost:9200/_alias/mbd-display?pretty

# Count documents in the alias
curl http://localhost:9200/mbd-display/_count?pretty

# Delete today's index (e.g., to restart a run)
make clear
# or manually:
curl -X DELETE http://localhost:9200/mbd-misbehaviors-$(date +%Y.%m.%d)
```

**Common issues:**

- `NotFoundError` from `manage_display_filter.py` after deleting all indices:
  the alias needs at least one concrete index.  Run `make filter` — it
  creates a dated placeholder automatically.

- `action.destructive_requires_name` error on wildcard DELETE: ES blocks
  wildcard deletes by safety default.  Use the exact index name.

---

### Logstash

```bash
# Container status
docker compose ps logstash

# Live logs (shows parse errors and indexing activity)
docker compose logs -f logstash

# Force re-ingestion (Logstash re-reads the log from the start on restart)
make ingest
# or:
docker compose restart logstash

# Check Logstash API health
curl http://localhost:9600/?pretty
```

**Common issues:**

- Logstash shows no output after `make run`: check that
  `logs/misbehaviors.log` exists and is non-empty.

- Documents not appearing in Kibana: confirm the index name with
  `curl http://localhost:9200/_cat/indices/mbd*?v` and verify the data view
  pattern matches.

---

### Kibana

```bash
# Container status
docker compose ps kibana

# Live logs
docker compose logs -f kibana

# API health (returns JSON with status)
curl http://localhost:5601/api/status | python3 -m json.tool | grep '"level"'
```

**Common issues:**

- Kibana shows "No results": check the time picker — set it to cover the
  `@timestamp` range of your data (usually the run date).

- Dashboard missing: re-run the setup container:

```bash
docker compose up setup
```

- Controls sliders missing: run `python manage_display_filter.py --setup-kibana`.

---

### Python detector

```bash
# Run with explicit paths
python detector.py data/tampa_BSM_2021.zip --log logs/misbehaviors.log

# Fresh run — truncate the log first
python detector.py data/tampa_BSM_2021.zip --log logs/misbehaviors.log --clear

# Inspect the last few log entries
tail -5 logs/misbehaviors.log | python3 -m json.tool

# Count events by type in the log
python3 -c "
import json, collections
counts = collections.Counter()
for line in open('logs/misbehaviors.log'):
    try: counts[json.loads(line)['misbehavior']] += 1
    except: pass
for k,v in sorted(counts.items()): print(f'{v:>8,}  {k}')
"

# Preview the L2 filter without pushing it
python manage_display_filter.py --dry-run
```

---

### Docker (general)

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# Rebuild images after config changes
docker compose down && docker compose up -d --build

# Check volume usage
docker system df -v | grep mbd
```

---

## Development: Unit Tests

Tests live in `tests/` and use `pytest`.  They run **outside Docker** on
your local Python environment.

```bash
make test
# or:
pytest tests/ -v
```

### Test coverage

| Test file | Detector |
|---|---|
| `test_speed.py` | speed_exceeded |
| `test_accel.py` | accel_exceeded |
| `test_brakes.py` | brakes_on_no_decel / decel_no_brakes |
| `test_position_jump.py` | position_jump |
| `test_heading_inconsistency.py` | heading_inconsistency |
| `test_heading_change_rate.py` | implausible_heading_change_rate |
| `test_speed_position.py` | speed_position_inconsistency |
| `test_speed_accel.py` | speed_accel_inconsistency |
| `test_yaw_rate.py` | yaw_rate_inconsistency |

### Writing new tests

`tests/conftest.py` exports `make_bsm()` which builds a synthetic BSM dict
from physical-unit parameters.  All J2735 encoding (multiply by the
appropriate scale factor) is handled internally:

```python
from conftest import make_bsm

bsm = make_bsm(
    vehicle_id="veh-001",
    secmark=100,          # ms within the minute
    lat_deg=41.0,
    lon_deg=-81.0,
    speed_kmh=72.0,
    heading_deg=0.0,
    accel_long_ms2=0.0,
    yaw_deg_s=0.0,
    wheel_brakes="00000",
)
```

---

## Project Structure

```
MBD/
├── detector.py                  Main entry point — reads BSMs, runs detectors
├── manage_display_filter.py     Pushes L2 thresholds to ES; adds Kibana Controls
├── thresholds.json              L2 display thresholds (editable)
├── Makefile                     Single entry point for all common operations
├── requirements.txt             Python dependencies
├── docker-compose.yml           ELK stack (Elasticsearch, Logstash, Kibana, setup)
│
├── detectors/
│   ├── utils.py                 J2735 constants, geometry helpers, BaseDetector
│   ├── speed.py                 speed_exceeded
│   ├── accel.py                 accel_exceeded
│   ├── brakes_inconsistency.py  brakes_on_no_decel / decel_no_brakes
│   ├── position_jump.py         position_jump
│   ├── heading_inconsistency.py heading_inconsistency
│   ├── heading_change_rate.py   implausible_heading_change_rate
│   ├── speed_position_consistency.py  speed_position_inconsistency
│   ├── speed_accel_consistency.py     speed_accel_inconsistency
│   └── yaw_rate_consistency.py        yaw_rate_inconsistency
│
├── tests/
│   ├── conftest.py              make_bsm() helper; shared fixtures
│   ├── test_speed.py
│   ├── test_accel.py
│   ├── test_brakes.py
│   ├── test_position_jump.py
│   ├── test_heading_inconsistency.py
│   ├── test_heading_change_rate.py
│   ├── test_speed_position.py
│   ├── test_speed_accel.py
│   └── test_yaw_rate.py
│
├── elk/
│   ├── elasticsearch/
│   │   ├── index-template.json  Field mappings for mbd-misbehaviors-* indices
│   │   └── display-alias.json   Initial alias definition (superseded by manage_display_filter.py)
│   ├── logstash/
│   │   ├── config/logstash.yml
│   │   └── pipeline/misbehaviors.conf  Logstash pipeline: parse JSON-lines → ES
│   ├── kibana/
│   │   ├── dashboard.ndjson      Main MBD Misbehaviors dashboard
│   │   ├── display-dashboard.ndjson
│   │   ├── display-filter.ndjson
│   │   └── kpi-vega.ndjson       Vega KPI panel
│   └── setup.sh                  One-shot setup: templates, alias, Kibana imports
│
├── data/                         BSM input files (not committed; add your own)
├── logs/
│   └── misbehaviors.log          Detector output; volume-mounted into Logstash
└── docs/
    └── V2X Communications Message Set Dictionary.pdf   SAE J2735 reference
```
