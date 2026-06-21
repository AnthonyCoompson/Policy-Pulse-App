"""
One-time script: copy all data from local SQLite into Turso.

Run locally, from the project root (same folder as database.py):

    pip install libsql-experimental==0.0.55 --break-system-packages
    TURSO_URL=libsql://your-db.turso.io TURSO_TOKEN=your-token \
    LOCAL_DB_PATH=policypulse.db \
    python migrate_to_turso.py

What it does:
  1. Connects to your LOCAL sqlite file (read-only, source of truth).
  2. Connects to Turso via the *same* get_conn()/init_db() logic the real
     app uses, so the remote schema is guaranteed to exactly match what
     main.py expects — you don't need to deploy first just to create tables.
  3. Copies every application table row by row using INSERT OR IGNORE, so
     it's safe to re-run (won't create duplicates if you run it twice).
  4. Prints a before/after row count for each table so you can confirm
     nothing silently failed to copy.

This only needs to be run once. After this, the Render-deployed app reads
and writes directly to Turso and your local file is no longer the source
of truth.
"""

import os
import sqlite3
import sys

LOCAL_DB_PATH = os.environ.get("LOCAL_DB_PATH", os.environ.get("DB_PATH", "policypulse.db"))

# System tables SQLite manages itself — never copy these.
SKIP_TABLES = {"sqlite_sequence", "sqlite_master", "sqlite_stat1"}


def main():
    if not os.environ.get("TURSO_URL"):
        print("ERROR: TURSO_URL is not set. Set TURSO_URL and TURSO_TOKEN before running.")
        sys.exit(1)

    if not os.path.exists(LOCAL_DB_PATH):
        print(f"ERROR: local database not found at '{LOCAL_DB_PATH}'. "
              f"Set LOCAL_DB_PATH if it lives somewhere else.")
        sys.exit(1)

    # Importing database.py here means we reuse the exact same get_conn() /
    # init_db() / _LibsqlConnection wrapper the real app uses — no risk of
    # the migration script's schema drifting from what main.py expects.
    import database

    if not database._use_turso:
        print("ERROR: database._use_turso is False — TURSO_URL didn't register. "
              "Check it's exported in this shell before running the script.")
        sys.exit(1)

    print(f"Source (local):  {LOCAL_DB_PATH}")
    print(f"Target (Turso):  {database.TURSO_URL}")
    print()

    # ── 1. Ensure the remote schema exists ────────────────────────────────────
    print("Creating/verifying schema on Turso (safe to run even if tables already exist)...")
    database.init_db()
    print("Schema ready.\n")

    # ── 2. Open both connections ───────────────────────────────────────────────
    local = sqlite3.connect(LOCAL_DB_PATH)
    local.row_factory = sqlite3.Row

    remote = database.get_conn()

    tables = [
        r[0] for r in local.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        if r[0] not in SKIP_TABLES
    ]
    print(f"Tables to migrate: {tables}\n")

    grand_total_copied = 0
    grand_total_skipped = 0

    for table in tables:
        rows = local.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: empty, skipping")
            continue

        cols = rows[0].keys()
        col_names = ",".join(cols)
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"

        before = remote.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        copied, errors = 0, 0
        for row in rows:
            try:
                remote.execute(sql, tuple(row))
                copied += 1
            except Exception as e:
                errors += 1
                print(f"    [skip row in {table}] {e}")
        remote.commit()

        after = remote.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        net_new = after - before
        print(f"  {table}: {len(rows)} local rows -> attempted {copied}, "
              f"{net_new} new rows added on Turso (rest already present), {errors} errors")

        grand_total_copied += net_new
        grand_total_skipped += (len(rows) - net_new)

    local.close()
    remote.close()

    print()
    print(f"Migration complete. {grand_total_copied} new rows written to Turso, "
          f"{grand_total_skipped} already present / skipped.")
    print("Re-running this script again is safe — it will report 0 new rows on a second pass.")


if __name__ == "__main__":
    main()
