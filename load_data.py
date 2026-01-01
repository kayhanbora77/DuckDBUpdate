import os
import time
from pathlib import Path

import duckdb
import pandas as pd
import glob
import re

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME
THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"
EXCEL_DIR = Path("/home/kayhan/Desktop/Gelen_Datalar/KayaBey")
TABLE_NAME = "TRAVEL_AGENT_RECORDS"
CVS_DIR = Path("/home/kayhan/Desktop/Gelen_Datalar/KayaBey/CVS")


# ==================================================
# LOGGING
# ==================================================
def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ==================================================
# DUCKDB CONNECTION
# ==================================================
def connect_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    return con


# ==================================================
# LOAD MULTIPLE EXCEL FILES


def load_multiple_excel_files(con) -> None:
    dfs = []

    for file in sorted(EXCEL_DIR.glob("*.xlsx")):
        log(f"Loading {file.name}")

        # FORCE everything to string to avoid timestamp casting
        df = pd.read_excel(file, sheet_name=0, dtype=str)

        df["source_file"] = file.name
        dfs.append(df)

    if not dfs:
        raise RuntimeError("No Excel files found")

    # Concatenate safely
    final_df = pd.concat(dfs, ignore_index=True)

    # Recreate table
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.register("final_df", final_df)

    con.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT * FROM final_df
    """)


def load_multiple_cvs_files(con) -> None:
    csv_files = glob.glob(os.path.join(CVS_DIR, "*.csv"))
    if not csv_files:
        print("❌ No CSV files found.")
    else:
        print(f"📁 Found {len(csv_files)} CSV file(s). Creating tables...\n")
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        # Sanitize table name: allow only letters, digits, underscores
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", os.path.splitext(filename)[0])

        # Ensure table name doesn't start with a digit (prepend underscore if needed)
        if table_name[0].isdigit():
            table_name = f"_{table_name}"

        print(f" ➤ Creating table: `{table_name}` from `{filename}`")

        try:
            # Use read_csv_auto() — DuckDB infers header and types automatically
            con.execute(f"""
                CREATE TABLE "{table_name}" AS
                SELECT * FROM read_csv_auto('{csv_path}')
            """)
            # Optional: report schema
            schema = con.execute(f'DESCRIBE "{table_name}"').fetchdf()
            print(f"   Columns: {list(schema['column_name'])}")
        except Exception as e:
            print(f"   ❌ Failed: {e}")


def main() -> None:
    start = time.time()
    log(f"⏰ Loading started at {now_str()}")

    con = connect_db()

    # load_multiple_excel_files(con)
    load_multiple_cvs_files(con)

    con.close()

    elapsed = time.time() - start
    log(f"🎉 Loading completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")


if __name__ == "__main__":
    main()
