#!/usr/bin/env bash
# Runs the Bistro load test at several concurrency levels.
#
# For EACH level: rebuild the isolated `bistro_loadtest` DB, start a fresh headless server
# (MQTT off, no email), run the load generator, stop the server. Results are appended to
# loadtest/results/<LABEL>.csv and each server log is kept in loadtest/.run/.
#
# Usage (Git Bash, from the repo root):
#   loadtest/run.sh                                   # 10 25 50 100 clients, 10s warm-up + 60s each
#   LEVELS="10 50" DURATION=30 LABEL=quick loadtest/run.sh
#
# Needs: JDK 21+, MySQL running, Bistro_Server/server.properties with db.user / db.password.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LEVELS="${LEVELS:-10 25 50 100}"
DURATION="${DURATION:-60}"
WARMUP="${WARMUP:-10}"
RAMPUP="${RAMPUP:-5}"     # seconds to spread the client connections over (0 = all at once, burst test)
LABEL="${LABEL:-baseline}"
PORT="${PORT:-5555}"
JAVAFX_LIB="${JAVAFX_LIB:-C:/Users/yazan/Downloads/openjfx-25.0.1_windows-x64_bin-sdk/javafx-sdk-25.0.1/lib}"

RUN="$ROOT/loadtest/.run"
BUILD="$RUN/build"
OUT="$ROOT/loadtest/results/${LABEL}.csv"

w() { cygpath -m "$1"; }   # Windows-style path for java/javac
LIBS="$(for j in "$ROOT"/Bistro_Server/lib/*.jar; do printf '%s;' "$(w "$j")"; done)"

echo "[run] compiling OCSF, server and load generator..."
rm -rf "$BUILD"; mkdir -p "$BUILD/ocsf" "$BUILD/server" "$BUILD/loadtest" "$RUN/server"
javac -nowarn -d "$(w "$BUILD/ocsf")" $(find "$ROOT/OCSF/src" -name '*.java') 2>/dev/null
javac -nowarn -d "$(w "$BUILD/server")" -cp "$(w "$BUILD/ocsf");$LIBS$JAVAFX_LIB/*" \
      $(find "$ROOT/Bistro_Server/src" -name '*.java')
javac -nowarn -d "$(w "$BUILD/loadtest")" -cp "$(w "$BUILD/ocsf");$(w "$BUILD/server");$LIBS" \
      "$ROOT/loadtest/src/LoadTest.java"

# Server config for the isolated DB (credentials copied from your git-ignored server.properties)
{
  grep -E '^db\.(host|port|user|password)=' "$ROOT/Bistro_Server/server.properties" | tr -d '\r'
  echo "db.name=bistro_loadtest"
  echo "app.timezone=Asia/Jerusalem"
  echo "mqtt.enabled=false"
  # Optional server tuning (used for the before/after comparison):
  #   SERVER_WORKERS=0 SERVER_BACKLOG=10 POOL_MAX_IDLE=10  -> original one-request-at-a-time server
  [ -n "${SERVER_WORKERS:-}" ] && echo "server.workers=$SERVER_WORKERS"
  [ -n "${SERVER_BACKLOG:-}" ] && echo "server.backlog=$SERVER_BACKLOG"
  [ -n "${POOL_MAX_IDLE:-}" ]  && echo "db.pool.maxIdle=$POOL_MAX_IDLE"
  true
} > "$RUN/server/server.properties"

SERVER_CP="$(w "$BUILD/server");$(w "$BUILD/ocsf");$LIBS$JAVAFX_LIB/*"
CLIENT_CP="$(w "$BUILD/loadtest");$(w "$BUILD/server");$(w "$BUILD/ocsf");$LIBS"

stop_server() {
  kill "$1" 2>/dev/null || true
  for _ in $(seq 1 20); do
    netstat -ano | grep -q "LISTENING.*:$PORT \|:$PORT .*LISTENING" || return 0
    sleep 0.5
  done
  # still listening: force-kill whatever owns the port
  local pid; pid="$(netstat -ano | grep ":$PORT " | grep LISTENING | awk '{print $NF}' | head -1)"
  [ -n "$pid" ] && taskkill //F //PID "$pid" >/dev/null 2>&1 || true
}

mkdir -p "$(dirname "$OUT")"
rm -f "$OUT"
echo "label,clients,confirmed_reservations,time_slots_checked,overbooking_violations" \
    > "$ROOT/loadtest/results/${LABEL}_integrity.csv"

for N in $LEVELS; do
  echo "================ $LABEL: $N clients ================"
  bash "$ROOT/loadtest/reset_db.sh" 2>&1 | grep -v "character set\|Using a password" || true

  pushd "$RUN/server" >/dev/null
  java -cp "$SERVER_CP" Server.HeadlessMain --client-port "$PORT" > "$RUN/server_${LABEL}_${N}.log" 2>&1 &
  SERVER_PID=$!
  popd >/dev/null

  # wait until the server accepts connections (max 30 s)
  for _ in $(seq 1 60); do
    if netstat -ano | grep LISTENING | grep -q ":$PORT "; then break; fi
    sleep 0.5
  done
  sleep 2  # let background jobs do their first pass

  java -cp "$CLIENT_CP" LoadTest --clients "$N" --duration "$DURATION" --warmup "$WARMUP" --rampup "$RAMPUP" \
       --port "$PORT" --label "$LABEL" --out "$(w "$OUT")" || echo "[run] load generator failed for $N"

  stop_server "$SERVER_PID"

  # Integrity: no time slot may hold more parties than the tables can seat (one table per party)
  (
    export MYSQL_PWD="$(grep -E '^db\.password=' "$ROOT/Bistro_Server/server.properties" | cut -d= -f2- | tr -d '\r')"
    DB_USER="$(grep -E '^db\.user=' "$ROOT/Bistro_Server/server.properties" | cut -d= -f2- | tr -d '\r')"
    printf '%s,%s,' "$LABEL" "$N"
    "${MYSQL:-/c/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe}" -u "$DB_USER" -h 127.0.0.1 -N -B \
        bistro_loadtest < "$ROOT/loadtest/check_overbooking.sql" 2>/dev/null | tr '\t' ','
  ) | tee -a "$ROOT/loadtest/results/${LABEL}_integrity.csv"
done

echo "[run] done -> $OUT"
