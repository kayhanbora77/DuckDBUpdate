import duckdb
import pandas as pd
import re
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
BATCH_SIZE = 100_000  # Reduced for memory efficiency

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2100

# --------------------------------------------------
# CONNECT
# --------------------------------------------------
con = duckdb.connect(DB_PATH)

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
# HELPERS
# --------------------------------------------------
def is_rnk(flight):
    if not isinstance(flight, str):
        return False
    if flight.endswith("000"):
        return True
    # return bool(re.fullmatch(r"[A-Z]{2,3}0+", flight.strip().upper()))


def normalize_flight_number(fn):
    if not isinstance(fn, str):
        return None

    fn = fn.strip().upper()
    m = re.fullmatch(r"([A-Z]{2,3})(0*)(\d+)", fn)
    if not m:
        return fn

    airline, _, number = m.groups()
    return f"{airline}{int(number)}"


def normalize_date(value):
    d = pd.to_datetime(value, errors="coerce")
    if pd.isna(d):
        return None
    if d.year < VALID_YEAR_MIN or d.year > VALID_YEAR_MAX:
        return None
    return d.date()


def same_route(d1, d2):
    if d1 is None or d2 is None:
        return False
    return abs((d2 - d1).days) <= 1


# --------------------------------------------------
# PROCESS LOOP
# --------------------------------------------------
offset = 0
start_time = time.time()
print(
    f"⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
)

while True:
    df = con.execute(f"""
        SELECT *
        FROM {SOURCE_TABLE}
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """).df()

    if df.empty:
        break

    out_rows = []

    for _, row in df.iterrows():
        base = {
            "PaxName": row["PaxName"],
            "BookingRef": row["BookingRef"],
            "ETicketNo": row["ETicketNo"],
            "ClientCode": row["ClientCode"],
            "Airline": row["Airline"],
            "JourneyType": row["JourneyType"],
        }

        # --------------------------------------------------
        # 1️⃣ ROW-LEVEL DUPLICATE + RNK CLEAN (RAW DATA)
        # --------------------------------------------------
        seen_raw = set()
        cleaned_raw = []

        for i in range(1, 8):
            fn = row.get(f"FlightNumber{i}")
            dt = row.get(f"DepartureDateLocal{i}")
            ap = row.get(f"Airport{i}")

            if not fn or not dt:
                continue

            fn_clean = fn.strip().upper()

            if is_rnk(fn_clean):
                continue

            key = (fn_clean, dt)
            if key in seen_raw:
                continue

            seen_raw.add(key)
            cleaned_raw.append((fn_clean, dt, ap))

        # --------------------------------------------------
        # 2️⃣ NORMALIZE
        # --------------------------------------------------
        flights = []
        for fn, dt, ap in cleaned_raw:
            n_fn = normalize_flight_number(fn)
            n_dt = normalize_date(dt)

            if n_fn and n_dt:
                flights.append((n_fn, n_dt, ap))

        if not flights:
            continue

        # --------------------------------------------------
        # 3️⃣ SPLIT ROUTES
        # --------------------------------------------------
        routes = []
        current = []

        for fn, dt, ap in flights:
            if not current:
                current.append((fn, dt, ap))
                continue

            prev_fn, prev_dt, _ = current[-1]

            if fn == prev_fn and dt == prev_dt:
                continue

            if same_route(prev_dt, dt):
                current.append((fn, dt, ap))
            else:
                routes.append(current)
                current = [(fn, dt, ap)]

        if current:
            routes.append(current)

        # --------------------------------------------------
        # 4️⃣ BUILD OUTPUT ROWS
        # --------------------------------------------------
        for route in routes:
            new = base.copy()

            for i in range(1, 8):
                new[f"FlightNumber{i}"] = None
                new[f"DepartureDateLocal{i}"] = None
                new[f"Airport{i}"] = None
            new["Airport8"] = None

            # route-level safety duplicate
            seen = set()
            compacted = []

            for fn, dt, ap in route:
                key = (fn, dt)
                if key in seen:
                    continue
                seen.add(key)
                compacted.append((fn, dt, ap))

            for i, (fn, dt, ap) in enumerate(compacted, start=1):
                if i > 7:
                    break
                new[f"FlightNumber{i}"] = fn
                new[f"DepartureDateLocal{i}"] = dt
                new[f"Airport{i}"] = ap

            out_rows.append(new)

    out_df = pd.DataFrame(out_rows)

    if not out_df.empty:
        con.register("out_df", out_df)
        con.execute(f"""
    INSERT INTO {TARGET_TABLE} (
        PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,

        FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
        FlightNumber5, FlightNumber6, FlightNumber7,

        DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3,
        DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6,
        DepartureDateLocal7,

        Airport1, Airport2, Airport3, Airport4,
        Airport5, Airport6, Airport7, Airport8
    )
    SELECT
        PaxName, BookingRef, ETicketNo, ClientCode, Airline, JourneyType,

        FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
        FlightNumber5, FlightNumber6, FlightNumber7,

        DepartureDateLocal1, DepartureDateLocal2, DepartureDateLocal3,
        DepartureDateLocal4, DepartureDateLocal5, DepartureDateLocal6,
        DepartureDateLocal7,

        Airport1, Airport2, Airport3, Airport4,
        Airport5, Airport6, Airport7, Airport8
    FROM out_df
""")

        con.unregister("out_df")

    offset += BATCH_SIZE
    print(f"✅ Processed {offset} rows")
# Calculate execution time
end_time = time.time()
execution_time = end_time - start_time

# Convert to hours, minutes, seconds
hours = int(execution_time // 3600)
minutes = int((execution_time % 3600) // 60)
seconds = int(execution_time % 60)
print(f"⏰ End Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
print(
    f"⏱️  Execution Time: {hours:02d} hours, {minutes:02d} minutes, {seconds:02d} seconds"
)

print("🎉 FINAL ETL COMPLETED SUCCESSFULLY")
