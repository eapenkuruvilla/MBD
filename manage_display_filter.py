"""manage_display_filter.py — Level-2 display filter for MBD Kibana.

Reads thresholds.json and creates / replaces the Elasticsearch alias
mbd-display, which acts as a server-side pre-filter on top of the raw
mbd-misbehaviors-* indices.  Kibana is pointed at mbd-display so analysts
see only the higher-significance slice of the dataset by default.

With --setup-kibana the script also creates the Kibana data view that
references the alias.  Interactive Controls (range sliders) require a Gold
license and are therefore not available in the Basic distribution.

Workflow
--------
  1. Edit thresholds.json (adjust Level-2 values).
  2. Run:  python manage_display_filter.py
  3. Refresh Kibana — no data re-ingestion required.

Usage
-----
  python manage_display_filter.py [options]

  --es-url URL         Elasticsearch URL  (default: http://localhost:9200)
  --kibana-url URL     Kibana URL         (default: http://localhost:5601)
  --thresholds FILE    thresholds.json path (default: next to this script)
  --show               Print the currently active alias filter and exit.
  --dry-run            Print the generated filter without pushing it.
  --setup-kibana       Also create the Kibana data view for the mbd-display alias.
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

try:
    from elasticsearch import Elasticsearch, NotFoundError
except ImportError:
    raise SystemExit("Run:  pip install 'elasticsearch>=8,<9'")

ALIAS_NAME    = "mbd-display"
SOURCE_INDEX  = "mbd-misbehaviors*"
DATA_VIEW_ID  = "mbd-display-view"
DATA_VIEW_TITLE = "mbd-display"

DEFAULT_THRESHOLDS = Path(__file__).with_name("thresholds.json")

# ── Threshold loading ─────────────────────────────────────────────────────────

def load_thresholds(path: Path) -> dict:
    with path.open() as f:
        data = json.load(f)
    level2 = data.get("level2")
    if not level2:
        raise SystemExit(f"No 'level2' key found in {path}")
    return level2


# ── ES alias filter builder ───────────────────────────────────────────────────

def build_filter(level2: dict) -> dict:
    """
    Build an ES bool/should filter from per-misbehavior threshold dicts.

    For each misbehavior type, ALL listed field conditions must match (AND).
    Any misbehavior type can satisfy the filter (OR across types).

    A field condition can be:
      - a dict  → single range clause (ANDed with the rest)
      - a list  → multiple range clauses ORed together for that field

    Example — single condition (AND):
        "position_jump": {
            "jump_m": {"gte": 1000}
        }
    Becomes:
        {"bool": {"filter": [
            {"term":  {"misbehavior": "position_jump"}},
            {"range": {"jump_m": {"gte": 1000}}}
        ]}}

    Example — list condition (OR on same field):
        "speed_position_inconsistency": {
            "diff_kmh": [{"gte": 500}, {"lte": -500}]
        }
    Becomes:
        {"bool": {"filter": [
            {"term": {"misbehavior": "speed_position_inconsistency"}},
            {"bool": {"should": [
                {"range": {"diff_kmh": {"gte":  500}}},
                {"range": {"diff_kmh": {"lte": -500}}}
            ], "minimum_should_match": 1}}
        ]}}
    """
    should_clauses = []
    for mtype, field_conditions in level2.items():
        filters = [{"term": {"misbehavior": mtype}}]
        for field, condition in field_conditions.items():
            if isinstance(condition, list):
                filters.append({"bool": {"should": [
                    {"range": {field: c}} for c in condition
                ], "minimum_should_match": 1}})
            else:
                filters.append({"range": {field: condition}})
        should_clauses.append({"bool": {"filter": filters}})

    return {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1,
        }
    }


# ── ES alias management ───────────────────────────────────────────────────────

def show_alias(es: Elasticsearch) -> None:
    """Print the filter currently attached to the alias."""
    try:
        result = es.indices.get_alias(name=ALIAS_NAME)
        print(json.dumps(result.body, indent=2))
    except NotFoundError:
        print(f"Alias '{ALIAS_NAME}' does not exist yet.")
    except Exception as exc:
        print(f"Could not retrieve alias: {exc}", file=sys.stderr)


def push_alias(es: Elasticsearch, filter_query: dict, dry_run: bool = False) -> None:
    """Remove the old alias (if any) and create a fresh one with the new filter."""
    actions = []

    # Remove old alias from any index that has it
    try:
        existing = es.indices.get_alias(name=ALIAS_NAME)
        for index_name in existing.body:
            actions.append({"remove": {"index": index_name, "alias": ALIAS_NAME}})
    except NotFoundError:
        pass  # first time — nothing to remove

    # Add new alias with embedded filter
    actions.append({
        "add": {
            "index":  SOURCE_INDEX,
            "alias":  ALIAS_NAME,
            "filter": filter_query,
        }
    })

    if dry_run:
        print(json.dumps({"actions": actions}, indent=2))
        print("\n[dry-run] Not pushed to Elasticsearch.")
        return

    # update_aliases requires at least one concrete index matching SOURCE_INDEX.
    # If none exists yet (e.g. after a fresh delete), create a placeholder so
    # the alias can be established before the first detector run.
    try:
        hits = es.cat.indices(index=SOURCE_INDEX, h="index").body.strip()
    except Exception:
        hits = ""
    if not hits:
        today = date.today().strftime("%Y.%m.%d")
        placeholder = f"mbd-misbehaviors-{today}"
        es.indices.create(index=placeholder, ignore=400)
        print(f"  Created placeholder index '{placeholder}'.")

    es.indices.update_aliases(body={"actions": actions})
    print(f"✓ Alias '{ALIAS_NAME}' → '{SOURCE_INDEX}' created/updated.")


# ── Kibana setup ──────────────────────────────────────────────────────────────

def _kibana_headers() -> dict:
    return {"kbn-xsrf": "true", "Content-Type": "application/json"}


def _kibana_post(kibana_url: str, path: str, body: dict) -> dict:
    """POST to Kibana API; returns parsed JSON response or raises."""
    import urllib.request
    import urllib.error

    url  = kibana_url.rstrip("/") + path
    data = json.dumps(body).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers=_kibana_headers(), method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        raise RuntimeError(f"Kibana POST {path} → HTTP {exc.code}: {body_text}") from exc


def _kibana_get(kibana_url: str, path: str) -> dict:
    import urllib.request
    import urllib.error

    url = kibana_url.rstrip("/") + path
    req = urllib.request.Request(url, headers={"kbn-xsrf": "true"})
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        raise RuntimeError(f"Kibana GET {path} → HTTP {exc.code}: {body_text}") from exc


def create_kibana_data_view(kibana_url: str) -> None:
    """Create (or silently skip if already present) the mbd-display data view."""
    body = {
        "data_view": {
            "id":            DATA_VIEW_ID,
            "title":         DATA_VIEW_TITLE,
            "name":          "MBD Display (filtered)",
            "timeFieldName": "@timestamp",
        },
        "override": True,   # overwrite if it already exists
    }
    _kibana_post(kibana_url, "/api/data_views/data_view", body)
    print(f"✓ Kibana data view '{DATA_VIEW_TITLE}' ({DATA_VIEW_ID}) created/updated.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Create/update the mbd-display ES alias from thresholds.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--es-url", default="http://localhost:9200",
        help="Elasticsearch URL  (default: http://localhost:9200)",
    )
    parser.add_argument(
        "--kibana-url", default="http://localhost:5601",
        help="Kibana URL  (default: http://localhost:5601)",
    )
    parser.add_argument(
        "--thresholds", type=Path, default=DEFAULT_THRESHOLDS,
        metavar="FILE",
        help="Path to thresholds.json  (default: thresholds.json next to this script)",
    )
    parser.add_argument(
        "--show", action="store_true",
        help="Print the currently active alias filter and exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the generated alias actions JSON without pushing to Elasticsearch",
    )
    parser.add_argument(
        "--setup-kibana", action="store_true",
        help="Also create the mbd-display Kibana data view (run once after docker compose up)",
    )
    return parser.parse_args()


def main():
    args   = parse_args()
    es     = Elasticsearch(args.es_url)

    if args.show:
        show_alias(es)
        return

    level2       = load_thresholds(args.thresholds)
    filter_query = build_filter(level2)

    print(f"Thresholds : {args.thresholds}  ({len(level2)} misbehavior types)")
    print(f"Alias      : {ALIAS_NAME}  →  {SOURCE_INDEX}")
    print()

    push_alias(es, filter_query, dry_run=args.dry_run)

    if args.setup_kibana and not args.dry_run:
        print()
        create_kibana_data_view(args.kibana_url)

    if not args.dry_run:
        print()
        print("Done.  In Kibana, switch your data view to "
              f"'{DATA_VIEW_TITLE}' to see the filtered dataset.")
        print("Use  python manage_display_filter.py --show  to inspect the active filter.")


if __name__ == "__main__":
    main()
