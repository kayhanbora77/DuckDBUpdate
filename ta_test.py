import time
from pathlib import Path

import duckdb

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TA_MASTER"
TARGET_TABLE = "TA_MASTER_TARGET"
FINAL_MASTER_TABLE = "TA_MASTER_FINAL"

BATCH_SIZE = 500_000

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2027

THREADS = 4
MEMORY_LIMIT = "6GB"
TEMP_DIR = "/tmp/duckdb_temp"


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
# SCHEMA & MACROS
# ==================================================
def create_target_table(con: duckdb.DuckDBPyConnection) -> None:
    log("♻️ Recreating target table")

    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            "Pax Name" TEXT,
            "PNR CRS" TEXT,
            "PNR Airline" TEXT,
            Airlines TEXT,
            "Ticket Number" TEXT,
            S1FltNo TEXT,
            S2FltNo TEXT,
            S3FltNo TEXT,
            S4FltNo TEXT,
            S5FltNo TEXT,
            S6FltNo TEXT,
            S1Date TIMESTAMP,
            S2Date TIMESTAMP,
            S3Date TIMESTAMP,
            S4Date TIMESTAMP,
            S5Date TIMESTAMP,
            S6Date TIMESTAMP,
            Airport1 TEXT,
            Airport2 TEXT,
            Airport3 TEXT,
            Airport4 TEXT,
            Airport5 TEXT,
            Airport6 TEXT,
            Airport7 TEXT
        );
    """)


def create_macros(con: duckdb.DuckDBPyConnection) -> None:
    log("🧩 Creating helper macros")

    con.execute("""
        CREATE OR REPLACE MACRO is_vo(flight) AS (
            CASE
                WHEN flight IS NULL THEN FALSE
                WHEN regexp_matches(TRIM(UPPER(flight)), '^[A-Z]{1,2}$') THEN TRUE
                ELSE FALSE
            END
        )
    """)

    con.execute("""
        CREATE OR REPLACE MACRO normalize_flight(flight) AS (
            CASE
                WHEN flight IS NULL THEN NULL
                -- airline code (2–3 letters) + optional space + digits
                WHEN regexp_matches(UPPER(TRIM(flight)), '^[A-Z]{2,3}\\s*[0-9]+$')
                THEN
                    regexp_replace(
                        regexp_replace(UPPER(TRIM(flight)), '\\s+', ''),  -- remove spaces
                        '^([A-Z]{2,3})0+([0-9]+)$',                       -- drop leading zeros
                        '\\1\\2'
                    )

                ELSE UPPER(TRIM(flight))
            END
        )
    """)

    con.execute(f"""
        CREATE OR REPLACE MACRO normalize_date(dt) AS (
            CASE
                WHEN dt IS NULL THEN NULL
                WHEN TRY_CAST(dt AS TIMESTAMP) IS NULL THEN NULL
                WHEN YEAR(TRY_CAST(dt AS TIMESTAMP)) < {VALID_YEAR_MIN}
                  OR YEAR(TRY_CAST(dt AS TIMESTAMP)) > {VALID_YEAR_MAX}
                THEN NULL
                ELSE TRY_CAST(dt AS TIMESTAMP)
            END
        )
    """)


# ==================================================
# METRICS
# ==================================================
def get_total_rows(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]


def get_target_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]


# delete rows if starts with only letters
def delete_invalid_flight_no(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        DELETE
        FROM TA_MASTER
        WHERE regexp_matches(TRIM(UPPER("S1FltNo")), '^[A-Z]{1,2}$');
    """)


def process_batch(con: duckdb.DuckDBPyConnection, offset: int, limit: int) -> None:
    con.execute(f"""
        INSERT INTO {TARGET_TABLE}

        WITH src AS (
            SELECT *
            FROM {SOURCE_TABLE}
            WHERE rowid >= {offset}
              AND rowid < {offset + limit}
        ),
unpivoted AS (
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S1FltNo AS FltNo,
         S1Date  AS Dte, "Airport 1" AS DepAir, "Airport 2" AS ArrAir, 1 AS Segment
    FROM src
    WHERE TRIM(S1FltNo) <> ''
    UNION ALL
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S2FltNo AS FltNo,
        S2Date  AS Dte, "Airport 2" AS DepAir, "Airport 3" AS ArrAir, 2 AS Segment
    FROM src
    WHERE TRIM(S2FltNo) <> ''
    UNION ALL
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S3FltNo AS FltNo,
        S3Date  AS Dte, "Airport 3" AS DepAir, "Airport 4" AS ArrAir, 3 AS Segment
    FROM src
    WHERE TRIM(S3FltNo) <> ''
    UNION ALL
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S4FltNo AS FltNo,
        S4Date  AS Dte, "Airport 4" AS DepAir, "Airport 5" AS ArrAir, 4 AS Segment
    FROM src
    WHERE TRIM(S4FltNo) <> ''
    UNION ALL
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S5FltNo AS FltNo,
        S5Date  AS Dte, "Airport 5" AS DepAir, "Airport 6" AS ArrAir, 5 AS Segment
    FROM src
    WHERE TRIM(S5FltNo) <> ''
    UNION ALL
    SELECT "Pax Name", "PNR CRS", "PNR Airline", Airlines, "Ticket Number", S6FltNo AS FltNo,
        S6Date  AS Dte, "Airport 6" AS DepAir, "Airport 7" AS ArrAir, 6 AS Segment
    FROM src
    WHERE TRIM(S6FltNo) <> ''
),
cleaned AS (
    SELECT
        "Pax Name",
        "PNR CRS",
        "PNR Airline",
        Airlines,
        "Ticket Number",
        normalize_flight(FltNo) AS clean_flt,
        normalize_date(Dte) AS clean_dte,
        DepAir,
        ArrAir,
        Segment
    FROM unpivoted
),
filtered AS (
    SELECT *
    FROM cleaned
    WHERE clean_flt IS NOT NULL
      AND clean_dte IS NOT NULL
      AND NOT is_vo(clean_flt)
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY "Pax Name", "PNR CRS", clean_dte
            ORDER BY Segment
        ) AS rn
    FROM filtered
),
with_prev AS (
    SELECT *,
        LAG(clean_dte) OVER (
            PARTITION BY "Pax Name", "PNR CRS"
             ORDER BY clean_dte, Segment
        ) AS prev_dte
    FROM deduped
    WHERE rn = 1
),
with_trip_id AS (
    SELECT *,
        SUM(
            CASE
                WHEN prev_dte IS NULL THEN 1
                WHEN clean_dte < prev_dte THEN 1
                WHEN EXTRACT(EPOCH FROM (clean_dte - prev_dte)) > 36 * 3600 THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY "Pax Name", "PNR CRS"
            ORDER BY clean_dte, Segment
            ROWS UNBOUNDED PRECEDING
        ) AS trip_id
    FROM with_prev
),
sequenced AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY "Pax Name", "PNR CRS", trip_id
            ORDER BY clean_dte, Segment
        ) AS seq_id
    FROM with_trip_id
),
pivoted AS (
    SELECT
        "Pax Name",
        "PNR CRS",
        ANY_VALUE("PNR Airline")  AS "PNR Airline",
		ANY_VALUE(Airlines)        AS Airlines,
		ANY_VALUE("Ticket Number") AS "Ticket Number",
        MAX(CASE WHEN seq_id = 1 THEN clean_flt END) AS S1FltNo,
        MAX(CASE WHEN seq_id = 2 THEN clean_flt END) AS S2FltNo,
        MAX(CASE WHEN seq_id = 3 THEN clean_flt END) AS S3FltNo,
        MAX(CASE WHEN seq_id = 4 THEN clean_flt END) AS S4FltNo,
        MAX(CASE WHEN seq_id = 5 THEN clean_flt END) AS S5FltNo,
        MAX(CASE WHEN seq_id = 6 THEN clean_flt END) AS S6FltNo,
        MAX(CASE WHEN seq_id = 1 THEN clean_dte END) AS S1Date,
        MAX(CASE WHEN seq_id = 2 THEN clean_dte END) AS S2Date,
        MAX(CASE WHEN seq_id = 3 THEN clean_dte END) AS S3Date,
        MAX(CASE WHEN seq_id = 4 THEN clean_dte END) AS S4Date,
        MAX(CASE WHEN seq_id = 5 THEN clean_dte END) AS S5Date,
        MAX(CASE WHEN seq_id = 6 THEN clean_dte END) AS S6Date,
        MAX(CASE WHEN seq_id = 1 THEN DepAir END) AS "Airport 1",
        MAX(CASE WHEN seq_id = 1 THEN ArrAir END) AS "Airport 2",
        MAX(CASE WHEN seq_id = 2 THEN ArrAir END) AS "Airport 3",
        MAX(CASE WHEN seq_id = 3 THEN ArrAir END) AS "Airport 4",
        MAX(CASE WHEN seq_id = 4 THEN ArrAir END) AS "Airport 5",
        MAX(CASE WHEN seq_id = 5 THEN ArrAir END) AS "Airport 6",
        MAX(CASE WHEN seq_id = 6 THEN ArrAir END) AS "Airport 7"
    FROM sequenced
    GROUP BY "Pax Name", "PNR CRS", trip_id
)
SELECT
    "Pax Name",
    "PNR CRS",
    "PNR Airline",
    Airlines,
	"Ticket Number",
    S1FltNo, S2FltNo, S3FltNo, S4FltNo, S5FltNo, S6FltNo,
    S1Date, S2Date, S3Date,S4Date, S5Date, S6Date,
    "Airport 1", "Airport 2", "Airport 3", "Airport 4",
    "Airport 5", "Airport 6", "Airport 7"
FROM pivoted
    """)


def seperate_flights_by_airports(con: duckdb.DuckDBPyConnection) -> None:
    log("🔀 Creating final master table with separated flights by airports")
    con.execute(f"DROP TABLE IF EXISTS {FINAL_MASTER_TABLE}")

    con.execute("""
    CREATE TABLE TA_MASTER_FINAL AS
    WITH base AS (
        SELECT *
        FROM TA_MASTER_TARGET
    ),
    to_split AS (
        SELECT *
        FROM base
        WHERE "Airport1" IS NOT NULL
        AND "Airport2" IS NOT NULL
        AND "Airport3" IS NOT NULL
    ),
    not_split AS (
        SELECT *
        FROM base
        WHERE NOT (
            "Airport1" IS NOT NULL
            AND "Airport2" IS NOT NULL
            AND "Airport3" IS NOT NULL
        )
    ),
    -- -------- LEG 1 --------
    leg1 AS (
        SELECT
            "Pax Name",
            "PNR CRS",
            "PNR Airline",
            "Airlines",
            "Ticket Number",
            "S1FltNo" AS "S1FltNo",
            NULL AS "S2FltNo",
            NULL AS "S3FltNo",
            NULL AS "S4FltNo",
            NULL AS "S5FltNo",
            NULL AS "S6FltNo",
            "S1Date" AS "S1Date",
            NULL AS "S2Date",
            NULL AS "S3Date",
            NULL AS "S4Date",
            NULL AS "S5Date",
            NULL AS "S6Date",
            "Airport1" AS "Airport1",
            "Airport2" AS "Airport2",
            NULL AS "Airport3",
            NULL AS "Airport4",
            NULL AS "Airport5",
            NULL AS "Airport6",
            NULL AS "Airport7"
        FROM to_split
    ),
    -- -------- LEG 2 --------
    leg2 AS (
        SELECT
            "Pax Name",
            "PNR CRS",
            "PNR Airline",
            "Airlines",
            "Ticket Number",
            "S2FltNo" AS "S1FltNo",
            NULL AS "S2FltNo",
            NULL AS "S3FltNo",
            NULL AS "S4FltNo",
            NULL AS "S5FltNo",
            NULL AS "S6FltNo",
            "S2Date" AS "S1Date",
            NULL AS "S2Date",
            NULL AS "S3Date",
            NULL AS "S4Date",
            NULL AS "S5Date",
            NULL AS "S6Date",
            "Airport2" AS "Airport1",
            "Airport3" AS "Airport2",
            NULL AS "Airport3",
            NULL AS "Airport4",
            NULL AS "Airport5",
            NULL AS "Airport6",
            NULL AS "Airport7"
        FROM to_split
    )
    -- -------- FINAL RESULT --------
    SELECT * FROM not_split
    UNION ALL
    SELECT * FROM leg1
    UNION ALL
    SELECT * FROM leg2;
    """)


def main() -> None:
    start = time.time()
    log(f"⏰ ETL started at {now_str()}")

    con = connect_db()
    # first run below code. Than run seperate_flights_by_airports
    # seperate_flights_by_airports(con)

    create_target_table(con)
    total_rows = get_total_rows(con)
    log(f"📊 Source rows: {total_rows:,}")
    create_macros(con)

    offset = 0
    batch_no = 0
    inserted_prev = 0

    while offset < total_rows:
        batch_no += 1
        log(f"🔄 Batch {batch_no} | rows {offset:,} → {offset + BATCH_SIZE:,}")

        process_batch(con, offset, BATCH_SIZE)

        inserted_now = get_target_count(con)
        log(f"✅ Added {inserted_now - inserted_prev:,} rows")

        inserted_prev = inserted_now
        offset += BATCH_SIZE

    elapsed = time.time() - start
    log(f"🎉 ETL completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")

    con.close()


if __name__ == "__main__":
    main()
