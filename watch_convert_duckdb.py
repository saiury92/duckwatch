import os
import shutil
import time
import traceback
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import duckdb
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

WATCH_DIR = Path(os.getenv("WATCH_DIR", "/app/data/watched_folder")).resolve()
PARQUET_DIR = Path(os.getenv("PARQUET_DIR", "/app/data/parquet_out")).resolve()
DUCKDB_DB = Path(os.getenv("DUCKDB_DB", "/app/data/db/mydb.duckdb")).resolve()

CSV_SUFFIXES = (".csv", ".CSV")
WRITE_COMPRESSION = "snappy"
STABILITY_CHECK_SECS = 0.75
STABILITY_RETRIES = 6

def ensure_dirs():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_DB.parent.mkdir(parents=True, exist_ok=True)

def wait_until_stable(path: Path) -> bool:
    try:
        prev = -1
        for _ in range(STABILITY_RETRIES):
            cur = path.stat().st_size
            if cur > 0 and cur == prev:
                return True
            prev = cur
            time.sleep(STABILITY_CHECK_SECS)
        cur = path.stat().st_size
        return cur > 0 and cur == prev
    except FileNotFoundError:
        return False

def csv_to_parquet(csv_path: Path, parquet_path: Path):
    table = pacsv.read_csv(csv_path)  # let Arrow infer types

    # Check if table has a 'date' column for partitioning
    if 'date' in table.column_names:
        # Get unique dates from the table
        unique_dates = table['date'].unique().to_pylist()

        # Clean up existing partition directories for these dates
        for date_val in unique_dates:
            partition_dir = parquet_path / f"date={date_val}"
            if partition_dir.exists():
                shutil.rmtree(partition_dir)
                print(f"🗑️  Cleaned existing partition: {partition_dir}")

        # Write partitioned parquet by date directly to PARQUET_DIR
        pq.write_to_dataset(
            table,
            root_path=str(parquet_path),
            partition_cols=['date'],
            compression=WRITE_COMPRESSION,
            existing_data_behavior='overwrite_or_ignore'
        )
    else:
        # Fallback to single file if no date column
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_path, compression=WRITE_COMPRESSION)

def target_parquet_path(csv_path: Path) -> Path:
    # For partitioned parquet, we'll use PARQUET_DIR as the root
    # The actual partitioning will be handled by write_to_dataset
    return PARQUET_DIR

def init_duckdb():
    con = duckdb.connect(str(DUCKDB_DB))
    # Try creating the view only if there are parquet files present
    try:
        any_parquet = any(PARQUET_DIR.rglob("**/*.parquet"))
    except FileNotFoundError:
        any_parquet = False

    if any_parquet:
        # DuckDB does not allow parameters in this DDL context; inline the glob literal
        glob_pattern = str(PARQUET_DIR / "**/*.parquet").replace("'", "''")
        con.execute(f"""
            CREATE VIEW IF NOT EXISTS all_data AS
            SELECT * FROM read_parquet('{glob_pattern}')
        """)
        print(f"Created view for partitioned parquet files in {PARQUET_DIR}")
    else:
        # No parquet files yet; skip creating the view for now
        print(f"No parquet files yet; skipping view creation for now.")
        pass
    con.close()

def ensure_view_exists_if_parquet_present():
    """Create the `all_data` view if parquet files now exist and view is missing."""
    try:
        has_parquet = any(PARQUET_DIR.rglob("**/*.parquet"))
    except FileNotFoundError:
        has_parquet = False
    if not has_parquet:
        return

    print(f"Creating view for partitioned parquet files in {PARQUET_DIR}")
    con = duckdb.connect(str(DUCKDB_DB))
    try:
        glob_pattern = str(PARQUET_DIR / "**/*.parquet").replace("'", "''")
        con.execute(f"""
            CREATE VIEW IF NOT EXISTS all_data AS
            SELECT * FROM read_parquet('{glob_pattern}')
        """)
    finally:
        con.close()

def sample_query():
    con = duckdb.connect(str(DUCKDB_DB))
    try:
        row = con.execute("SELECT count(*) FROM all_data").fetchone()
        rows = int(row[0]) if row is not None else 0
        print(f"🦆 DuckDB: {rows} rows in all_data (if view exists).")
    except Exception:
        print(f"Error querying DuckDB: {traceback.format_exc()}")
        pass
    finally:
        con.close()

def process_csv(csv_path: Path):
    if not csv_path.suffix.lower() == ".csv":
        return
    if not wait_until_stable(csv_path):
        print(f"⏭️  Skipping (not stable yet): {csv_path}")
        return
    pq_path = target_parquet_path(csv_path)
    try:
        csv_to_parquet(csv_path, pq_path)
        print(f"✅ Converted: {csv_path} -> {pq_path}")
        ensure_view_exists_if_parquet_present()
        sample_query()
    except Exception as e:
        print(f"❌ Failed converting {csv_path}: {e}\n{traceback.format_exc()}")

class CsvHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            process_csv(Path(event.src_path))
    def on_modified(self, event):
        if not event.is_directory:
            process_csv(Path(event.src_path))
    def on_moved(self, event):
        if not event.is_directory:
            process_csv(Path(event.dest_path))

def bootstrap_convert_existing():
    for csv in WATCH_DIR.rglob("*.csv"):
        process_csv(csv)

def main():
    if not WATCH_DIR.exists():
        raise SystemExit(f"Missing {WATCH_DIR}. Mount a volume there.")
    ensure_dirs()
    init_duckdb()
    bootstrap_convert_existing()

    observer = Observer()
    observer.schedule(CsvHandler(), str(WATCH_DIR), recursive=True)
    observer.start()
    print(f"👀 Watching: {WATCH_DIR}")
    print(f"📦 Parquet out: {PARQUET_DIR}")
    print(f"🦆 DuckDB DB: {DUCKDB_DB} (view: all_data)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
