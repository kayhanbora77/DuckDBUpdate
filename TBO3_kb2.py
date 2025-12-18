import time
from pathlib import Path

import duckdb

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

SOURCE_TABLE = "TBO3_MASTER"
TARGET_TABLE = "TBO3_MASTER_TARGET"

# --------------------------------------------------
# CONNECT (PERFORMANCE TUNED)
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

con.execute("PRAGMA threads = 4;")
con.execute("PRAGMA memory_limit = '4GB';")  # ⬅ critical
con.execute("PRAGMA temp_directory = '/tmp/duckdb';")
con.execute("PRAGMA enable_progress_bar = false;")

# --------------------------------------------------
# TIMING
# --------------------------------------------------
start_time = time.time()
print("⏰ Start:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))

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

    DepartureDateLocal1 DATE,
    DepartureDateLocal2 DATE,
    DepartureDateLocal3 DATE,
    DepartureDateLocal4 DATE,
    DepartureDateLocal5 DATE,
    DepartureDateLocal6 DATE,
    DepartureDateLocal7 DATE,

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
# STEP 1: EXPLODE FLIGHTS (UNNEST)
# --------------------------------------------------
con.execute(f"""
INSERT INTO {TARGET_TABLE}
SELECT
    PaxName,
    BookingRef,
    ETicketNo,
    ClientCode,
    Airline,
    JourneyType,

    flights[1], flights[2], flights[3], flights[4],
    flights[5], flights[6], flights[7],

    dates[1], dates[2], dates[3], dates[4],
    dates[5], dates[6], dates[7],

    airports[1], airports[2], airports[3], airports[4],
    airports[5], airports[6], airports[7], NULL
FROM (
    SELECT
        PaxName,
        BookingRef,
        ETicketNo,
        ClientCode,
        Airline,
        JourneyType,
        grp,

        LIST(NormFlight ORDER BY DepartureDate)     AS flights,
        LIST(DepartureDate ORDER BY DepartureDate) AS dates,
        LIST(Airport ORDER BY DepartureDate)       AS airports
    FROM (
        SELECT
            *,
            SUM(new_route) OVER (
                PARTITION BY BookingRef
                ORDER BY DepartureDate
            ) AS grp
        FROM (
            SELECT
                PaxName,
                BookingRef,
                ETicketNo,
                ClientCode,
                Airline,
                JourneyType,
                Airport,
                DepartureDate,

                CASE
                    WHEN regexp_matches(FlightNumber, '^[A-Z]{2, 3}0+[0-9]+$')
                    THEN regexp_replace(
                        FlightNumber,
                        '^([A-Z]{2, 3})0+([0-9]+)$',
                        '\\1\\2'
                    )
                    ELSE FlightNumber
                END AS NormFlight,

                CASE
                    WHEN LAG(DepartureDate) OVER w IS NULL THEN 1
                    WHEN abs(DepartureDate - LAG(DepartureDate) OVER w) <= 1 THEN 0
                    ELSE 1
                END AS new_route
            FROM (
                SELECT
                    s.PaxName,
                    s.BookingRef,
                    s.ETicketNo,
                    s.ClientCode,
                    s.Airline,
                    s.JourneyType,
                    t.fn  AS FlightNumber,
                    t.dep AS DepartureDate,
                    t.ap  AS Airport
                FROM {SOURCE_TABLE} s
                CROSS JOIN UNNEST([1,2,3,4,5,6,7]) AS idx(i)
                CROSS JOIN LATERAL (
                    SELECT
                        CASE idx.i
                            WHEN 1 THEN s.FlightNumber1
                            WHEN 2 THEN s.FlightNumber2
                            WHEN 3 THEN s.FlightNumber3
                            WHEN 4 THEN s.FlightNumber4
                            WHEN 5 THEN s.FlightNumber5
                            WHEN 6 THEN s.FlightNumber6
                            WHEN 7 THEN s.FlightNumber7
                        END AS fn,
                        CASE idx.i
                            WHEN 1 THEN TRY_CAST(s.DepartureDateLocal1 AS DATE)
                            WHEN 2 THEN TRY_CAST(s.DepartureDateLocal2 AS DATE)
                            WHEN 3 THEN TRY_CAST(s.DepartureDateLocal3 AS DATE)
                            WHEN 4 THEN TRY_CAST(s.DepartureDateLocal4 AS DATE)
                            WHEN 5 THEN TRY_CAST(s.DepartureDateLocal5 AS DATE)
                            WHEN 6 THEN TRY_CAST(s.DepartureDateLocal6 AS DATE)
                            WHEN 7 THEN TRY_CAST(s.DepartureDateLocal7 AS DATE)
                        END AS dep,
                        CASE idx.i
                            WHEN 1 THEN s.Airport1
                            WHEN 2 THEN s.Airport2
                            WHEN 3 THEN s.Airport3
                            WHEN 4 THEN s.Airport4
                            WHEN 5 THEN s.Airport5
                            WHEN 6 THEN s.Airport6
                            WHEN 7 THEN s.Airport7
                        END AS ap
                ) t
                WHERE t.fn IS NOT NULL
                  AND t.dep IS NOT NULL
                  AND t.fn NOT LIKE '%000'   -- cheap filter first
            )
            WINDOW w AS (
                PARTITION BY BookingRef
                ORDER BY DepartureDate
            )
        )
    )
    GROUP BY
        PaxName,
        BookingRef,
        ETicketNo,
        ClientCode,
        Airline,
        JourneyType,
        grp
);
""")
# --------------------------------------------------

# --------------------------------------------------
# FINISH
# --------------------------------------------------
end_time = time.time()
elapsed = end_time - start_time

h = int(elapsed // 3600)
m = int((elapsed % 3600) // 60)
s = int(elapsed % 60)

print("⏰ End:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
print(f"⏱ Execution time: {h:02d}:{m:02d}:{s:02d}")

con.close()
