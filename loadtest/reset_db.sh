#!/usr/bin/env bash
# Rebuilds the ISOLATED load-test database `bistro_loadtest` from the project's SQL dumps.
# Your real `bistro` database is never touched: the dumps' "CREATE DATABASE / USE `bistro`"
# lines are rewritten to `bistro_loadtest` on the fly (the dumps contain DROP TABLE statements).
#
# Usage (Git Bash):  loadtest/reset_db.sh
# Reads db.user / db.password / db.host / db.port from Bistro_Server/server.properties.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROPS="$ROOT/Bistro_Server/server.properties"
DUMPS="$ROOT/G20_Assignment3/G20_Assignment3_DB"
MYSQL="${MYSQL:-/c/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe}"
DB="bistro_loadtest"

prop() { grep -E "^$1=" "$PROPS" | head -1 | cut -d= -f2- | tr -d '\r'; }
DB_USER="$(prop db.user)"; DB_HOST="$(prop db.host)"; DB_PORT="$(prop db.port)"
export MYSQL_PWD="$(prop db.password)"   # env var, so the password never appears in the process list

run_sql() { "$MYSQL" --default-character-set=utf8mb4 -u "$DB_USER" -h "${DB_HOST:-127.0.0.1}" -P "${DB_PORT:-3306}" "$@"; }

run_sql -e "DROP DATABASE IF EXISTS \`$DB\`; CREATE DATABASE \`$DB\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

# Order does not matter (dumps disable FK checks), but keep parents first for readability
for t in subscribers staff restaurant_table opening_hours reservation waiting_list user_activity visit bill performance_log; do
  sed -e "s/\`bistro\`/\`$DB\`/g" "$DUMPS/bistro_$t.sql" | run_sql "$DB"
done

run_sql "$DB" < "$ROOT/loadtest/seed.sql"
echo "[reset_db] $DB rebuilt and seeded"
