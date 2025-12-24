import time
from pathlib import Path

import duckdb

# ==================================================
# CONFIG
# ==================================================
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TBO3_MASTER"
TARGET_TABLE = "TBO3_MASTER_TARGET"

BATCH_SIZE = 500_000

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2100

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
            PaxName TEXT,
            BookingRef TEXT,
            ETicketNo TEXT,
            ClientCode TEXT,
            Airline TEXT,
            JourneyType TEXT,

            FlightNumber1 TEXT,
            FlightNumber2 TEXT,
            FlightNumber3 TEXT,
            FlightNumber4 TEXT,
            FlightNumber5 TEXT,
            FlightNumber6 TEXT,
            FlightNumber7 TEXT,

            DepartureDateLocal1 TIMESTAMP,
            DepartureDateLocal2 TIMESTAMP,
            DepartureDateLocal3 TIMESTAMP,
            DepartureDateLocal4 TIMESTAMP,
            DepartureDateLocal5 TIMESTAMP,
            DepartureDateLocal6 TIMESTAMP,
            DepartureDateLocal7 TIMESTAMP,

            Airport1 TEXT,
            Airport2 TEXT,
            Airport3 TEXT,
            Airport4 TEXT,
            Airport5 TEXT,
            Airport6 TEXT,
            Airport7 TEXT,
            Airport8 TEXT
        );
    """)


def create_macros(con: duckdb.DuckDBPyConnection) -> None:
    log("🧩 Creating helper macros")

    con.execute("""
        CREATE OR REPLACE MACRO is_rnk(flight) AS (
            CASE
                WHEN flight IS NULL THEN FALSE
                WHEN right(flight, 3) = '000' THEN TRUE
                --WHEN regexp_matches(UPPER(TRIM(flight)), '^[A-Z][A-Z0-9]*0{3,}$') THEN TRUE
                ELSE FALSE
            END
        )
    """)

    con.execute("""
        CREATE OR REPLACE MACRO normalize_flight(flight) AS (
            CASE
                WHEN flight IS NULL THEN NULL
                WHEN regexp_matches(UPPER(TRIM(flight)), '^([A-Z]{2,3})(0*)([0-9]+)$')
                    THEN regexp_replace(
                        UPPER(TRIM(flight)),
                        '^([A-Z]{2,3})0+([0-9]+)$',
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


# ==================================================
# BATCH PROCESSING (FIXED)
# ==================================================
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
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber1 AS FlightNumber, DepartureDateLocal1 AS DepartureDate,
                   Airport1 AS DepAir, Airport2 AS ArrAir, 1 AS OriginalSeq
            FROM src WHERE TRIM(FlightNumber1) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber2, DepartureDateLocal2, Airport2, Airport3, 2
            FROM src WHERE TRIM(FlightNumber2) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber3, DepartureDateLocal3, Airport3, Airport4, 3
            FROM src WHERE TRIM(FlightNumber3) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber4, DepartureDateLocal4, Airport4, Airport5, 4
            FROM src WHERE TRIM(FlightNumber4) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber5, DepartureDateLocal5, Airport5, Airport6, 5
            FROM src WHERE TRIM(FlightNumber5) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber6, DepartureDateLocal6, Airport6, Airport7, 6
            FROM src WHERE TRIM(FlightNumber6) <> ''
            UNION ALL
            SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
                   FlightNumber7, DepartureDateLocal7, Airport7, Airport8, 7
            FROM src WHERE TRIM(FlightNumber7) <> ''
        ),
        cleaned AS (
            SELECT
                PaxName,
                BookingRef,
                ClientCode,
                Airline,
                JourneyType,
                normalize_flight(FlightNumber) AS clean_flt,
                normalize_date(DepartureDate) AS clean_dte,
                DepAir,
                ArrAir,
                OriginalSeq
            FROM unpivoted
        ),
        filtered AS (
            SELECT *
            FROM cleaned
            WHERE clean_flt IS NOT NULL
              AND clean_dte IS NOT NULL
              AND NOT is_rnk(clean_flt)
        ),
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY BookingRef, PaxName, clean_dte
                    ORDER BY OriginalSeq
                ) AS rn
            FROM filtered
        ),
        with_prev AS (
            SELECT *,
                LAG(clean_dte) OVER (
                    PARTITION BY BookingRef, PaxName
                    ORDER BY OriginalSeq
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
                    PARTITION BY BookingRef, PaxName
                    ORDER BY OriginalSeq
                    ROWS UNBOUNDED PRECEDING
                ) AS trip_id
            FROM with_prev
        ),
        sequenced AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY BookingRef, PaxName, trip_id
                    ORDER BY OriginalSeq
                ) AS seq_id
            FROM with_trip_id
        ),
        pivoted AS (
            SELECT
                PaxName,
                BookingRef,
                ANY_VALUE(ClientCode) AS ClientCode,
                ANY_VALUE(Airline) AS Airline,
                ANY_VALUE(JourneyType) AS JourneyType,

                MAX(CASE WHEN seq_id = 1 THEN clean_flt END) AS FlightNumber1,
                MAX(CASE WHEN seq_id = 2 THEN clean_flt END) AS FlightNumber2,
                MAX(CASE WHEN seq_id = 3 THEN clean_flt END) AS FlightNumber3,
                MAX(CASE WHEN seq_id = 4 THEN clean_flt END) AS FlightNumber4,
                MAX(CASE WHEN seq_id = 5 THEN clean_flt END) AS FlightNumber5,
                MAX(CASE WHEN seq_id = 6 THEN clean_flt END) AS FlightNumber6,
                MAX(CASE WHEN seq_id = 7 THEN clean_flt END) AS FlightNumber7,

                MAX(CASE WHEN seq_id = 1 THEN clean_dte END) AS DepartureDateLocal1,
                MAX(CASE WHEN seq_id = 2 THEN clean_dte END) AS DepartureDateLocal2,
                MAX(CASE WHEN seq_id = 3 THEN clean_dte END) AS DepartureDateLocal3,
                MAX(CASE WHEN seq_id = 4 THEN clean_dte END) AS DepartureDateLocal4,
                MAX(CASE WHEN seq_id = 5 THEN clean_dte END) AS DepartureDateLocal5,
                MAX(CASE WHEN seq_id = 6 THEN clean_dte END) AS DepartureDateLocal6,
                MAX(CASE WHEN seq_id = 7 THEN clean_dte END) AS DepartureDateLocal7,

                MAX(CASE WHEN seq_id = 1 THEN DepAir END) AS Airport1,
                MAX(CASE WHEN seq_id = 1 THEN ArrAir END) AS Airport2,
                MAX(CASE WHEN seq_id = 2 THEN ArrAir END) AS Airport3,
                MAX(CASE WHEN seq_id = 3 THEN ArrAir END) AS Airport4,
                MAX(CASE WHEN seq_id = 4 THEN ArrAir END) AS Airport5,
                MAX(CASE WHEN seq_id = 5 THEN ArrAir END) AS Airport6,
                MAX(CASE WHEN seq_id = 6 THEN ArrAir END) AS Airport7,
                MAX(CASE WHEN seq_id = 7 THEN ArrAir END) AS Airport8
            FROM sequenced
            GROUP BY PaxName, BookingRef, trip_id
        )
        SELECT
            PaxName,
            BookingRef,
            NULL AS ETicketNo,
            ClientCode,
            Airline,
            JourneyType,
            FlightNumber1, FlightNumber2, FlightNumber3,
            FlightNumber4, FlightNumber5, FlightNumber6, FlightNumber7,
            DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3,
            DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6,
            DepartureDateLocal7,
            Airport1, Airport2, Airport3, Airport4,
            Airport5, Airport6, Airport7, Airport8
        FROM pivoted
    """)


# ==================================================
# MAIN
# ==================================================
def main() -> None:
    start = time.time()
    log(f"⏰ ETL started at {now_str()}")

    con = connect_db()

    create_target_table(con)
    create_macros(con)

    total_rows = get_total_rows(con)
    log(f"📊 Source rows: {total_rows:,}")

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
    log(f"📊 Final row count: {inserted_prev:,}")

    con.close()


if __name__ == "__main__":
    main()
