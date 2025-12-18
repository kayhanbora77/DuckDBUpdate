import duckdb
from pathlib import Path
import time

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TBO3_MASTER"
TARGET_TABLE = "TBO3_MASTER_TARGET"
TEMP_TABLE = "TBO3_MASTER_TEMP"
BATCH_SIZE = 500_000  # Process 500k records at a time

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2100

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

# Optimize settings for memory-constrained environment
con.execute("SET threads TO 4")  # Reduced threads
con.execute("SET memory_limit = '6GB'")  # Leave some headroom
con.execute("SET preserve_insertion_order = false")  # Save memory

start_time = time.time()
print(
    f"⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
)

# --------------------------------------------------
# RECREATE TARGET TABLE
# --------------------------------------------------
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
# --------------------------------------------------
# CREATE HELPER MACROS IN DUCKDB
# --------------------------------------------------

con.execute("""
CREATE OR REPLACE MACRO is_rnk(flight) AS (
    CASE
        WHEN flight IS NULL THEN FALSE
        WHEN regexp_matches(UPPER(TRIM(flight)), '^[A-Z]{2,3}0+$') THEN TRUE
        ELSE FALSE
    END
)
""")

con.execute("""
CREATE OR REPLACE MACRO normalize_flight(flight) AS (
    CASE
        WHEN flight IS NULL THEN NULL
        WHEN regexp_matches(UPPER(TRIM(flight)), '^([A-Z]{2,3})(0*)([0-9]+)$') THEN
            regexp_replace(UPPER(TRIM(flight)), '^([A-Z]{2,3})0+([0-9]+)$', '\\1\\2')
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
             OR YEAR(TRY_CAST(dt AS TIMESTAMP)) > {VALID_YEAR_MAX} THEN NULL
        ELSE TRY_CAST(dt AS TIMESTAMP)
    END
)
""")
print("✅ Helper macros created")

# --------------------------------------------------
# GET TOTAL COUNT
# --------------------------------------------------
total_rows = con.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").fetchone()[0]
print(f"📊 Total source records: {total_rows:,}")

# --------------------------------------------------
# PROCESS IN BATCHES
# --------------------------------------------------
offset = 0
batch_num = 0
total_inserted = 0

while offset < total_rows:
    batch_num += 1
    print(
        f"🔄 Processing batch {batch_num} (rows {offset:,} to {offset + BATCH_SIZE:,})..."
    )

    # Create temp table for this batch
    con.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")

    con.execute(f"""
    CREATE TABLE {TEMP_TABLE} AS
    SELECT * FROM {SOURCE_TABLE}
    LIMIT {BATCH_SIZE} OFFSET {offset}
    """)
    # Process this batch
    con.execute(f"""
    INSERT INTO {TARGET_TABLE}
    WITH unpivoted AS (
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber1 as FlightNumber, DepartureDateLocal1 as DepartureDate,
               Airport1 as DepAir, Airport2 as ArrAir, 1 as OriginalSeq
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber1, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber2, DepartureDateLocal2, Airport2, Airport3, 2
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber2, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber3, DepartureDateLocal3, Airport3, Airport4, 3
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber3, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber4, DepartureDateLocal4, Airport4, Airport5, 4
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber4, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber5, DepartureDateLocal5, Airport5, Airport6, 5
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber5, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber6, DepartureDateLocal6, Airport6, Airport7, 6
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber6, '')), '') != ''
        UNION ALL
        SELECT PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
               FlightNumber7, DepartureDateLocal7, Airport7, Airport8, 7
        FROM {TEMP_TABLE} WHERE NULLIF(TRIM(COALESCE(FlightNumber7, '')), '') != ''
    ),
    cleaned AS (
        SELECT
            PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,
            normalize_flight(FlightNumber) AS clean_flt,
            normalize_date(DepartureDate) AS clean_dte,
            DepAir, ArrAir, OriginalSeq
        FROM unpivoted
        WHERE normalize_flight(FlightNumber) IS NOT NULL
          AND normalize_date(DepartureDate) IS NOT NULL
          AND NOT is_rnk(FlightNumber)
    ),
    -- 🔹 STEP 1: Deduplicate by EXACT timestamp
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    BookingRef,
                    PaxName,
                    clean_dte
                ORDER BY OriginalSeq
            ) AS rn_timestamp
        FROM cleaned
    ),
    kept_after_dedup AS (
        SELECT * FROM deduped WHERE rn_timestamp = 1
    ),
    -- 🔹 STEP 2: Order remaining legs and detect trip breaks (>36h gap)
    with_prev AS (
        SELECT *,
            LAG(clean_dte) OVER (
                PARTITION BY BookingRef,
                PaxName
                ORDER BY OriginalSeq
            ) AS prev_dte
        FROM kept_after_dedup
    ),
    with_trip_id AS (
        SELECT *,
            SUM(
                CASE
                    WHEN prev_dte IS NULL THEN 1
                    WHEN EXTRACT(EPOCH FROM (clean_dte - prev_dte)) > 36 * 3600 THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY BookingRef,
                PaxName
                ORDER BY OriginalSeq
                ROWS UNBOUNDED PRECEDING
            ) AS trip_id
        FROM with_prev
    ),
    -- 🔹 STEP 3: Sequence within each trip
    sequenced AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY BookingRef,
                PaxName, trip_id
                ORDER BY OriginalSeq
            ) AS seq_id
        FROM with_trip_id
    ),
    -- 🔹 STEP 4: Pivot per trip
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
        PaxName, BookingRef, NULL AS ETicketNo, ClientCode, Airline, JourneyType,
        FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
        FlightNumber5, FlightNumber6, FlightNumber7,
        DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3,
        DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6,
        DepartureDateLocal7,
        Airport1, Airport2, Airport3, Airport4, Airport5, Airport6, Airport7, Airport8
    FROM pivoted
    """)
    result = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()
    batch_count = result[0] if result is not None else 0
    rows_added = batch_count - total_inserted
    total_inserted = batch_count

    print(
        f"✅ Batch {batch_num} complete. Added {rows_added:,} rows. Total: {total_inserted:,}"
    )

    offset += BATCH_SIZE

# Cleanup
con.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")

# Calculate execution time
elapsed_time = time.time() - start_time
minutes = int(elapsed_time // 60)
seconds = elapsed_time % 60
print(f"Execution Time: {minutes}m {seconds:.2f}s")
print(f"📊 Total rows in target table: {total_inserted:,}")
print("🎉 FINAL ETL COMPLETED SUCCESSFULLY")

con.close()
