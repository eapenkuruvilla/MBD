#!/bin/sh
# Applies the ES index template and imports the Kibana dashboard.
# Critical: both the index template and Kibana dashboard import must succeed.

ES="http://elasticsearch:9200"
KB="http://kibana:5601"

# curl_json <method> <url> [extra curl args...]
# Writes body to /tmp/resp.txt, returns HTTP status code only.
curl_json() {
  method="$1"; url="$2"; shift 2
  curl -s -o /tmp/resp.txt -w "%{http_code}" -X "$method" "$url" "$@"
}

# ── Wait for Elasticsearch ────────────────────────────────────────────────────
echo "[setup] Waiting for Elasticsearch..."
until curl -s "$ES/_cat/health?h=status" 2>/dev/null | grep -qE "green|yellow"; do
  sleep 3
done
echo "[setup] Elasticsearch is up."

# ── Apply index template ──────────────────────────────────────────────────────
echo "[setup] Applying index template..."
code=$(curl_json PUT "$ES/_index_template/mbd-misbehaviors" \
  -H "Content-Type: application/json" \
  --data-binary @/setup/index-template.json)
echo "[setup] Template response (HTTP $code): $(cat /tmp/resp.txt)"

if [ "$code" != "200" ]; then
  echo "[setup] ERROR: index template failed — aborting."
  exit 1
fi
echo "[setup] Index template applied."

# ── Wait for Kibana saved objects service ────────────────────────────────────
echo "[setup] Waiting for Kibana saved objects service..."
until curl -s -o /dev/null -w "%{http_code}" \
    -H "kbn-xsrf: true" \
    "$KB/api/saved_objects/_find?type=index-pattern&per_page=1" \
    2>/dev/null | grep -q "200"; do
  sleep 5
done
echo "[setup] Kibana is up."

# ── Import dashboard (fatal) ──────────────────────────────────────────────────
echo "[setup] Importing Kibana dashboard..."
code=$(curl_json POST "$KB/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F file=@/setup/dashboard.ndjson)
echo "[setup] Import response (HTTP $code): $(cat /tmp/resp.txt)"

if [ "$code" = "200" ]; then
  echo "[setup] Dashboard imported."
else
  echo "[setup] ERROR: dashboard import returned HTTP $code — aborting."
  echo "[setup] Import manually: Kibana → Stack Management → Saved Objects → Import"
  echo "[setup]   file: elk/kibana/dashboard.ndjson"
  exit 1
fi

echo "[setup] Done — open http://localhost:5601"
