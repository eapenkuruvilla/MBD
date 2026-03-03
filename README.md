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
10. [Network Access & Security](#network-access--security)
11. [Troubleshooting](#troubleshooting)
12. [Development: Unit Tests](#development-unit-tests)
13. [Future Work](#future-work)
14. [Project Structure](#project-structure)

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
   Kibana               ← Dashboards and KPI panels
```

**Key components:**

| Component | Role |
|---|---|
| `detector.py` | Reads BSMs, runs detectors, writes `logs/misbehaviors.log` |
| `detectors/` | Nine physics-based detector modules |
| `logs/misbehaviors.log` | JSON-lines event log consumed by Logstash |
| Logstash | Ships log lines to Elasticsearch; creates date-stamped indices |
| Elasticsearch | Stores events; hosts the `mbd-display` filtered alias |
| Kibana | Dashboards and KPI panels for interactive exploration |
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
   and misbehavior type within 50 m / 30 s.
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

The easiest way to wipe all MBD data across all dates is to bring the stack
down with the `-v` flag, which removes Docker volumes (Elasticsearch data and
Kibana state), then restart:

```bash
docker compose down -v
docker compose up -d

# Wait for the stack to be healthy, then restore data
make ingest    # re-ingest the detection log into Elasticsearch
make filter    # re-create the mbd-display alias (filtered dashboard)
```

Dashboards and data views are re-imported automatically on the next `up`
(see `setup.sh`), but Elasticsearch data and the display alias are not —
`make ingest` and `make filter` are required to repopulate both dashboards.

If you want to keep Kibana configuration (saved dashboards, customisations)
and only remove Elasticsearch indices, delete them one by one — wildcards are
blocked by the ES safety setting:

```bash
# List all MBD indices
curl http://localhost:9200/mbd-misbehaviors*?pretty

# Delete them one by one
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

### Filtering within the dashboard

Kibana's built-in **KQL bar** (top of every dashboard) supports ad-hoc
filtering on any field without modifying the data or the L2 alias.
Examples:

```
# Show only position jumps
misbehavior : "position_jump"

# Speed events above 250 km/h
misbehavior : "speed_exceeded" and speed_kmh > 250

# Large heading discrepancies
heading_diff > 170

# Specific vehicle
vehicle_id : "0123456789abcdef"
```

Click the **+** icon in the filter bar for a point-and-click field/value
picker if you prefer not to type KQL.

### Preserving dashboard changes

Dashboard edits made in the Kibana UI are stored in Elasticsearch (the
`.kibana_*` index), not on disk.  The NDJSON files in `elk/kibana/` are only
read at `docker compose up` time.  If you wipe the ES volume
(`docker compose down -v`) your changes are lost.

To save your current Kibana state back to disk before a destructive operation:

```bash
curl -s "http://localhost:5601/api/saved_objects/_export" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{"type":"dashboard","includeReferencesDeep":true}' \
  > elk/kibana/dashboard.ndjson
```

This overwrites `elk/kibana/dashboard.ndjson` with your current dashboard
state so it is reimported the next time the stack starts from scratch.

### Pushing edited NDJSON files to Kibana

After editing the NDJSON files on disk (e.g., changing the default time range,
adding a panel, or modifying a visualisation), push the changes to the live
Kibana instance without restarting the stack:

```bash
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@elk/kibana/dashboard.ndjson"

curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@elk/kibana/display-dashboard.ndjson"
```

Then hard-refresh Kibana (`Ctrl+Shift+R`).

### Discarding dashboard edits and reloading from disk

To throw away all Kibana UI changes and restore the dashboards exactly as
they are in `elk/kibana/`:

```bash
# Reimport all Kibana saved objects from the source files (overwrites live state)
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@elk/kibana/dashboard.ndjson"

curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@elk/kibana/display-dashboard.ndjson"
```

Then hard-refresh Kibana (`Ctrl+Shift+R`).  No stack restart is needed.

### Docker down vs. curl reimport

| Command | ES data | Kibana saved objects | Reimports NDJSON? |
|---|---|---|---|
| `docker compose down` | kept | kept | No |
| `docker compose down -v` | **wiped** | **wiped** | Yes, on next `up` |
| curl reimport (above) | kept | overwritten | Immediately, no restart |

Use `docker compose down -v` only as a last resort — it also wipes all
misbehavior event indices, requiring a full `make fresh` to reingest data.

---

## Network Access & Security

### Default binding

The `docker-compose.yml` publishes Kibana as `"5601:5601"`, which Docker
binds to **all network interfaces** (`0.0.0.0`).  Kibana is therefore
reachable from any host that can reach this machine on port 5601.  With the
Basic license there is no login prompt, so anyone who can reach the port has
full read/write access to the dashboards.

### Restricting to localhost

For a machine with a public IP, restrict the binding to loopback only:

```yaml
# docker-compose.yml
ports:
  - "127.0.0.1:5601:5601"
```

Then restart the stack:

```bash
docker compose down && docker compose up -d
```

### Secure remote access via SSH tunnel

Keep the `127.0.0.1` binding and open a tunnel from your laptop:

```bash
ssh -L 5601:localhost:5601 user@your-host
```

Then browse to `http://localhost:5601` on your laptop.  The tunnel encrypts
the traffic and no port needs to be opened in the firewall.

### Verifying exposure

To check whether Kibana is reachable from another machine:

```bash
curl -s http://<host-ip>:5601/api/status | head -c 100
```

A JSON response means Kibana is publicly accessible; a connection error means
the firewall is blocking the port.

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

- Unexpected "can't be loaded" errors on panels: reimport the dashboard from the source file — `curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" -H "kbn-xsrf: true" -F "file=@elk/kibana/dashboard.ndjson"`

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

## Future Work

### Platform: migrate to OpenSearch

The current stack uses Elasticsearch and Kibana under the **Elastic Basic**
license, which is free but has notable restrictions:

- **Kibana Controls** (range sliders for interactive threshold filtering)
  require a Gold/Platinum license.
- Certain security features (TLS, role-based access control) also require
  a paid tier.

**OpenSearch** (Apache 2.0 licence, fork of ES 7.10 / Kibana 7.10) includes
all of these features at no cost.  Migration is largely a matter of swapping
Docker images (`elasticsearch` → `opensearchproject/opensearch`,
`kibana` → `opensearchproject/opensearch-dashboards`) and adjusting a handful
of API paths.  Doing so would allow interactive Controls panels for the
L2 threshold sliders directly in the dashboard, removing the need to edit
`thresholds.json` and rerun `make filter`.

---

### Detectors: candidates for future implementation

#### Frozen / Replayed BSM

If a vehicle sends the same latitude, longitude, speed, and heading for N
consecutive messages with non-trivial time elapsed, the data is either frozen
(sensor failure) or replayed (attack).  Implementation is a simple stateful
counter per vehicle — no geometry required.

#### Message Counter Anomaly (`msg_cnt`)

The `msg_cnt` field is a 1-byte wrapping counter (0–127).  If it goes
backwards, repeats, or jumps by a large amount without a wrap, it signals
replayed or injected messages.  Can be implemented as a stateful check
alongside the existing heading/position detectors.

#### msgCount Discontinuity

The `msgCount` field increments by 1 each transmission and wraps at 127.  A
non-sequential jump (e.g., 42 → 99) for the same vehicle ID indicates a replay
attack or injected message.  Similar to the message counter anomaly detector
but targets a different field with different wrap semantics.

#### BSM Frequency Anomaly

SAE J2735 specifies ~10 Hz broadcast rate.  A vehicle sending 200 msg/s is
flooding the channel; 0.1 msg/s is suspiciously slow.  Requires a sliding time
window per vehicle to compute the observed message rate and compare it against
configurable bounds.

#### Stale / Replayed Timestamp

Flag BSMs whose `recordGeneratedAt` timestamp is significantly older than the
median or current processing time (e.g., > 10 seconds behind).  A classic
replay attack indicator that requires no per-vehicle state — just a comparison
against a rolling reference time.

#### Sybil Detection (Co-location)

Multiple distinct vehicle IDs reporting positions within a very small radius
(e.g., < 5 m) at the same time.  One physical device impersonating several
virtual vehicles is a **Sybil attack** — a major V2X security threat.
Implementation requires a spatial index (e.g., geohash bucketing) over all
active vehicles in each time window, making it the most computationally
intensive candidate on this list.

---

### Architecture: distributed ODE deployment

The current implementation processes a single ZIP/NDJSON file on one machine.
A production deployment would integrate with the
**USDOT Operational Data Environment (ODE)**, which runs as a distributed
Kubernetes cluster.  In that model:

- **Agents** run inside the ODE cluster, one per data source or geographic
  region.  Each agent continuously consumes BSM streams from the ODE and runs
  the misbehavior detectors in near-real time.
- **Each agent writes misbehavior events to a local JSON-lines log file.**
  A Filebeat sidecar tails the file and ships events to the central Logstash
  instance — agents have no direct connection to Elasticsearch.
- **Logstash** (or OpenSearch's Data Prepper) acts as the ingest layer,
  providing buffering, back-pressure handling, and field normalisation before
  events reach Elasticsearch.

Key changes required relative to the current design:

| Concern | Current | Production |
|---|---|---|
| Input | ZIP/NDJSON file | ODE BSM stream (REST/WebSocket) |
| Execution | Single process, one machine | Kubernetes `Deployment` with N replicas |
| Output | JSON-lines log → Logstash | JSON-lines log → **Filebeat** → Logstash → ES |
| State (stateful detectors) | In-process Python dict | Shared store (Redis or ES itself) |
| Index naming | `mbd-misbehaviors` | Data stream with ILM rollover policy |

#### Recommended ingest path: agent → Filebeat → Logstash → ES

The agent (detector process) writes misbehavior events to a local JSON-lines
log file, exactly as it does today.  **Filebeat** runs as a sidecar container
in the same Kubernetes pod, tails the log file, and ships events to the
central Logstash instance.  This approach:

- **Keeps the agent simple** — no ES client code, no network retry logic.
- **Decouples transport from detection** — Filebeat handles back-pressure,
  retries, and TLS without any changes to detector code.
- **Matches the current architecture** — the existing Logstash pipeline and
  Elasticsearch index template require no changes.
- **Scales naturally** — each agent pod has its own Filebeat sidecar; all
  sidecars fan-in to the same Logstash endpoint.

```
ODE BSM stream
      │
      ▼
 Agent pod (Kubernetes)
 ┌─────────────────────────────┐
 │  detector.py → misbehaviors │
 │  .log (JSON-lines)          │
 │          │                  │
 │   Filebeat sidecar ─────────┼──► Logstash ──► Elasticsearch ──► Kibana
 └─────────────────────────────┘
```

#### Eliminating shared state with pinned routing

The stateful detectors (position jump, heading, yaw, speed/accel consistency)
keep per-vehicle state in a Python dictionary.  In a naive multi-replica
deployment, BSMs from the same vehicle could arrive at different replicas,
corrupting state.

This can be avoided without Redis by **pinning each Filebeat instance to a
dedicated Logstash replica** and configuring the ODE to route BSMs from a
given geographic region or vehicle ID range to the same agent pod.  Because
each Logstash replica then sees a consistent subset of vehicles, per-vehicle
state stays in-process with no shared store required.

```
ODE region A ──► Agent pod A ──► Filebeat A ──► Logstash replica A ──┐
ODE region B ──► Agent pod B ──► Filebeat B ──► Logstash replica B ──┼──► ES ──► Kibana
ODE region C ──► Agent pod C ──► Filebeat C ──► Logstash replica C ──┘
```

Filebeat's `output.logstash` supports a static `hosts` list, so pinning is
simply a matter of pointing each Filebeat sidecar at a specific Logstash
`ClusterIP` or pod DNS name in the Kubernetes manifest.  The Logstash replicas
do not need to communicate with each other, and ES handles fan-in from all
replicas without coordination.

#### Index Lifecycle Management (ILM)

In a continuous ODE deployment, misbehavior events accumulate indefinitely.
**ILM** is an Elasticsearch feature that automatically manages index size and
age by moving data through a series of phases:

| Phase | Description |
|---|---|
| **Hot** | Active writes and fast reads |
| **Warm** | Read-only; compressed to slower, cheaper storage |
| **Cold** | Rarely accessed; further compressed |
| **Delete** | Automatically removed after a configured retention period |

Rollover rules trigger a transition to a new index when the current one
exceeds a size limit (e.g., 50 GB), a document count, or a time threshold
(e.g., 30 days).  This keeps individual indices at a manageable size and
query performance consistent over time.

The current MBD setup uses date-suffixed indices (`mbd-misbehaviors-YYYY.MM.DD`)
as a simple manual approximation of the same idea; ILM would automate and
generalise this for a production deployment.

---

## Project Structure

```
MBD/
├── detector.py                  Main entry point — reads BSMs, runs detectors
├── manage_display_filter.py     Pushes L2 thresholds to ES; creates mbd-display data view
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
