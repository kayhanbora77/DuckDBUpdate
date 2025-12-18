from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import re
from typing import List, Tuple, Set
import time

# --------------------------------------------------
# CONFIG - OPTIMIZED FOR PERFORMANCE
# --------------------------------------------------
DATABASE_DIR = Path.home() / "my_database"
DATABASE_NAME = "my_db.duckdb"
DB_PATH = DATABASE_DIR / DATABASE_NAME

# DB_PATH = r"D:\DuckDB\my_database.duckdb"
SOURCE_TABLE = "TBO3_2021"
TARGET_TABLE = "TBO3_2021_TARGET"
BATCH_SIZE = 100_000  # Reduced for memory efficiency

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2100

# Pre-compile regex patterns for maximum speed
RNK_PATTERN = re.compile(r"^[A-Z]{2,3}0+$")
FLIGHT_NORMALIZE_PATTERN = re.compile(r"^([A-Z]{2,3})(0*)(\d+)$")

# Pre-compute column lists for vectorization
FLIGHT_COLS = [f"FlightNumber{i}" for i in range(1, 8)]
DATE_COLS = [f"DepartureDateLocal{i}" for i in range(1, 8)]
AIRPORT_COLS = [f"Airport{i}" for i in range(1, 9)]

# --------------------------------------------------
# CONNECT WITH OPTIMIZATIONS
# --------------------------------------------------
con = duckdb.connect(DB_PATH)
con.execute("PRAGMA threads = 4;")  # Adjust based on your CPU cores
con.execute("PRAGMA memory_limit = '8GB';")  # For 12GB RAM systems
con.execute("PRAGMA enable_progress_bar = false;")  # Disable for batch processing

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
# OPTIMIZED HELPERS WITH VECTORIZATION
# --------------------------------------------------
def is_rnk_vectorized(flights_series):
    """Vectorized RNK detection."""
    if flights_series.dtype != "object":
        flights_series = flights_series.astype(str)

    # Convert to uppercase and strip
    upper_flights = flights_series.str.upper().str.strip()

    # Use pandas string methods for fast matching
    return upper_flights.str.match(RNK_PATTERN, na=False)


def normalize_flight_numbers_vectorized(flights_series):
    """Vectorized flight number normalization."""
    if flights_series.dtype != "object":
        flights_series = flights_series.astype(str)

    # Convert to uppercase and strip
    upper_flights = flights_series.str.upper().str.strip()

    # Extract airline code and number using vectorized operations
    matches = upper_flights.str.extract(FLIGHT_NORMALIZE_PATTERN, expand=False)

    # Where we have matches, reconstruct; otherwise keep original
    result = flights_series.copy()

    if not matches.empty and len(matches.columns) >= 3:
        mask = matches.iloc[:, 0].notna()  # Check if first column (airline) is not NaN
        if mask.any():
            airlines = matches.iloc[:, 0][mask]
            numbers = matches.iloc[:, 2][mask].astype(
                int
            )  # Third column is the number part
            normalized = airlines + numbers.astype(str)
            result[mask] = normalized

    return result


def normalize_dates_vectorized(dates_series):
    """Vectorized date normalization."""
    # Convert to datetime with errors='coerce'
    dt_series = pd.to_datetime(dates_series, errors="coerce")

    # Filter valid years
    valid_mask = (dt_series.dt.year >= VALID_YEAR_MIN) & (
        dt_series.dt.year <= VALID_YEAR_MAX
    )
    dt_series = dt_series.where(valid_mask)

    return dt_series


def process_single_row_optimized(row_series, base_fields):
    """Optimized single row processing using vectorized operations."""
    # Extract all flight data at once using vectorized operations
    flight_nums = row_series[FLIGHT_COLS].values
    dates = row_series[DATE_COLS].values
    airports = row_series[AIRPORT_COLS[:7]].values  # First 7 airports for flights

    # Normalize flights and dates vectorized
    normalized_flights = normalize_flight_numbers_vectorized(pd.Series(flight_nums))
    normalized_dates = normalize_dates_vectorized(pd.Series(dates))

    # Create flights list with vectorized filtering
    flights = []
    for i in range(len(flight_nums)):
        fn = (
            normalized_flights.iloc[i] if pd.notna(normalized_flights.iloc[i]) else None
        )
        dt = normalized_dates.iloc[i] if pd.notna(normalized_dates.iloc[i]) else None
        ap = airports[i] if i < len(airports) and pd.notna(airports[i]) else None

        if fn and not RNK_PATTERN.fullmatch(str(fn).upper()):
            flights.append((fn, dt, ap))

    # Split routes with optimized algorithm
    routes = []
    current_route = []

    for fn, dt, ap in flights:
        if not current_route:
            current_route.append((fn, dt, ap))
            continue

        prev_fn, prev_dt, _ = current_route[-1]

        # Quick checks to avoid expensive operations
        if fn == prev_fn and dt == prev_dt:
            continue

        if dt is not None and prev_dt is not None:
            day_diff = abs((dt - prev_dt).days)
            if day_diff <= 1:
                current_route.append((fn, dt, ap))
            else:
                routes.append(current_route)
                current_route = [(fn, dt, ap)]
        else:
            routes.append(current_route)
            current_route = [(fn, dt, ap)]

    if current_route:
        routes.append(current_route)

    # Build output rows
    out_rows = []
    for route in routes:
        new_row = base_fields.copy()

        # Reset flight/date/airport columns
        for i in range(1, 8):
            new_row[f"FlightNumber{i}"] = None
            new_row[f"DepartureDateLocal{i}"] = None
            new_row[f"Airport{i}"] = None
        new_row["Airport8"] = None

        # Remove duplicates while preserving order
        seen: Set[Tuple] = set()
        compacted = []

        for fn, dt, ap in route:
            if fn is None or dt is None:
                continue

            key = (fn, dt)
            if key in seen:
                continue

            seen.add(key)
            compacted.append((fn, dt, ap))

        # Write compacted route to output row
        for i, (fn, dt, ap) in enumerate(compacted, start=1):
            if i > 7:
                break
            new_row[f"FlightNumber{i}"] = fn
            new_row[f"DepartureDateLocal{i}"] = dt
            new_row[f"Airport{i}"] = ap

        out_rows.append(new_row)

    return out_rows


# --------------------------------------------------
# OPTIMIZED PROCESSING LOOP
# --------------------------------------------------
offset = 0
total_processed = 0

print("🚀 Starting optimized ETL process...")
start_time = time.time()
print(
    f"⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
)

while True:
    # Fetch batch with optimized query
    query = f"""
        SELECT *
        FROM {SOURCE_TABLE}
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """

    df = con.execute(query).df()

    if df.empty:
        break

    print(f"📦 Processing batch starting at {offset}, size: {len(df)}")

    # Process batch more efficiently
    all_out_rows = []

    # Get base field names once
    base_field_names = [
        "PaxName",
        "BookingRef",
        "ETicketNo",
        "ClientCode",
        "Airline",
        "JourneyType",
    ]

    for idx, row in df.iterrows():
        base_fields = {field: row[field] for field in base_field_names}
        batch_rows = process_single_row_optimized(row, base_fields)
        all_out_rows.extend(batch_rows)

    # Convert to DataFrame once
    if all_out_rows:
        out_df = pd.DataFrame(all_out_rows)

        # Use bulk insert for maximum performance
        con.execute(f"""
            INSERT INTO {TARGET_TABLE}
            SELECT * FROM out_df
        """)

    offset += BATCH_SIZE
    total_processed += len(all_out_rows)
    print(f"✅ Processed {offset} source rows, generated {total_processed} output rows")

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
print(f"🎉 FINAL ETL COMPLETED SUCCESSFULLY!")
print(f"📊 Total output rows: {total_processed}")

# Clean up
con.close()
