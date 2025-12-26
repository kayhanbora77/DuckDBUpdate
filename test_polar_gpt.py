import time
from pathlib import Path
from typing import Optional, List
import re

import polars as pl

# ==================================================
# CONFIG
# ==================================================
SOURCE_FILE = Path("/home/kayhan/Desktop/Gelen_Datalar/TBO/TBO3_MASTER.parquet")
TARGET_FILE = Path("/home/kayhan/Desktop/Gelen_Datalar/TBO/TBO3_MASTER_GPT.parquet")

# Process data in chunks to avoid memory issues
CHUNK_SIZE = 100_000  # Adjust based on available RAM

VALID_YEAR_MIN = 1990
VALID_YEAR_MAX = 2100
TIME_GAP_HOURS = 36


# ==================================================
# LOGGING
# ==================================================
def log(msg: str) -> None:
    print(msg, flush=True)


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ==================================================
# DATA CLEANING FUNCTIONS
# ==================================================
def is_rnk_flight(flight: str) -> bool:
    """Check if flight number ends with '000' (RNK flight)"""
    return flight is not None and flight.endswith("000")


def normalize_flight_number(flight: Optional[str]) -> Optional[str]:
    """Normalize flight number: AA0123 -> AA123"""
    if not flight:
        return None

    flight = flight.strip().upper()

    # Match pattern: 2-3 letters + optional zeros + digits
    match = re.match(r"^([A-Z]{2,3})(0*)(\d+)$", flight)
    if match:
        airline, _, number = match.groups()
        return f"{airline}{number}"

    return flight


# ==================================================
# DATA TRANSFORMATION (OPTIMIZED FOR MEMORY)
# ==================================================
def unpivot_flights_chunk(df: pl.DataFrame) -> pl.DataFrame:
    """Convert wide format to long format (unpivot flight columns)"""

    # Build unpivot efficiently using pl.concat with alignment
    segments = []

    for i in range(1, 8):
        flt_col = f"FlightNumber{i}"
        dte_col = f"DepartureDateLocal{i}"
        dep_col = f"Airport{i}"
        arr_col = f"Airport{i + 1}"

        # Check if columns exist
        if flt_col not in df.columns:
            continue

        segment = df.select(
            [
                pl.col("PaxName"),
                pl.col("BookingRef"),
                pl.col("ClientCode"),
                pl.col("Airline"),
                pl.col("JourneyType"),
                pl.col(flt_col).cast(pl.Utf8).alias("FlightNumber"),
                pl.col(dte_col).cast(pl.Utf8).alias("DepartureDate"),
                pl.col(dep_col).cast(pl.Utf8).alias("DepAir"),
                pl.col(arr_col).cast(pl.Utf8).alias("ArrAir"),
                pl.lit(i, dtype=pl.Int32).alias("OriginalSeq"),
            ]
        ).filter(
            pl.col("FlightNumber").is_not_null()
            & (pl.col("FlightNumber").str.strip_chars() != "")
        )

        segments.append(segment)

    if not segments:
        return pl.DataFrame()

    return pl.concat(segments, how="vertical_relaxed")


def transform_chunk(df_long: pl.DataFrame) -> pl.DataFrame:
    """Apply all transformations to a chunk of unpivoted data"""

    # Step 1: Clean and filter
    df_clean = df_long.with_columns(
        [
            pl.col("FlightNumber")
            .map_elements(normalize_flight_number, return_dtype=pl.Utf8)
            .alias("clean_flt"),
            pl.when(pl.col("DepartureDate").is_null())
            .then(None)
            .otherwise(
                pl.col("DepartureDate").str.to_datetime(strict=False, time_unit="us")
            )
            .alias("clean_dte"),
        ]
    ).filter(
        pl.col("clean_flt").is_not_null()
        & pl.col("clean_dte").is_not_null()
        & pl.col("clean_dte").dt.year().is_between(VALID_YEAR_MIN, VALID_YEAR_MAX)
        & ~pl.col("clean_flt").str.ends_with("000")
    )

    if len(df_clean) == 0:
        return pl.DataFrame()
    df_clean = df_clean.sort(["BookingRef", "PaxName", "clean_dte", "OriginalSeq"])

    # Step 2: Deduplicate
    df_dedup = (
        df_clean.with_columns(
            pl.col("OriginalSeq")
            .rank("ordinal")
            .over(["BookingRef", "PaxName", "clean_dte"])
            .alias("rn")
        )
        .filter(pl.col("rn") == 1)
        .drop("rn")
    )

    # Step 3: Identify trips
    time_gap_seconds = TIME_GAP_HOURS * 3600

    df_trips = (
        df_dedup.sort(["BookingRef", "PaxName", "OriginalSeq"])
        .with_columns(
            [
                pl.col("clean_dte")
                .shift(1)
                .over(["BookingRef", "PaxName"])
                .alias("prev_dte")
            ]
        )
        .with_columns(
            [
                pl.when(
                    pl.col("prev_dte").is_null()
                    | (pl.col("clean_dte") < pl.col("prev_dte"))
                    | (
                        (pl.col("clean_dte") - pl.col("prev_dte")).dt.total_seconds()
                        > time_gap_seconds
                    )
                )
                .then(1)
                .otherwise(0)
                .alias("is_trip_start")
            ]
        )
        .with_columns(
            [
                pl.col("is_trip_start")
                .cum_sum()
                .over(["BookingRef", "PaxName"])
                .alias("trip_id")
            ]
        )
        .drop(["prev_dte", "is_trip_start"])
    )

    # Step 4: Sequence within trips
    df_seq = df_trips.with_columns(
        pl.col("OriginalSeq")
        .rank("ordinal")
        .over(["BookingRef", "PaxName", "trip_id"])
        .alias("seq_id")
    )

    return df_seq


def pivot_to_wide_format(df: pl.DataFrame) -> pl.DataFrame:
    """Convert back to wide format with up to 7 flights per trip"""

    if len(df) == 0:
        return create_empty_output_df()

    # Aggregate to wide format
    df_wide = df.group_by(["PaxName", "BookingRef", "trip_id"]).agg(
        [
            pl.col("ClientCode").first(),
            pl.col("Airline").first(),
            pl.col("JourneyType").first(),
            # Flights
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 1)
            .first()
            .alias("FlightNumber1"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 2)
            .first()
            .alias("FlightNumber2"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 3)
            .first()
            .alias("FlightNumber3"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 4)
            .first()
            .alias("FlightNumber4"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 5)
            .first()
            .alias("FlightNumber5"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 6)
            .first()
            .alias("FlightNumber6"),
            pl.col("clean_flt")
            .filter(pl.col("seq_id") == 7)
            .first()
            .alias("FlightNumber7"),
            # Dates
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 1)
            .first()
            .alias("DepartureDateLocal1"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 2)
            .first()
            .alias("DepartureDateLocal2"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 3)
            .first()
            .alias("DepartureDateLocal3"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 4)
            .first()
            .alias("DepartureDateLocal4"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 5)
            .first()
            .alias("DepartureDateLocal5"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 6)
            .first()
            .alias("DepartureDateLocal6"),
            pl.col("clean_dte")
            .filter(pl.col("seq_id") == 7)
            .first()
            .alias("DepartureDateLocal7"),
            # Airports
            pl.col("DepAir").filter(pl.col("seq_id") == 1).first().alias("Airport1"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 1).first().alias("Airport2"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 2).first().alias("Airport3"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 3).first().alias("Airport4"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 4).first().alias("Airport5"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 5).first().alias("Airport6"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 6).first().alias("Airport7"),
            pl.col("ArrAir").filter(pl.col("seq_id") == 7).first().alias("Airport8"),
        ]
    )

    return df_wide.with_columns(pl.lit(None, dtype=pl.Utf8).alias("ETicketNo")).drop(
        "trip_id"
    )


def create_empty_output_df() -> pl.DataFrame:
    """Create empty DataFrame with correct schema"""
    return pl.DataFrame(
        {
            "PaxName": [],
            "BookingRef": [],
            "ETicketNo": [],
            "ClientCode": [],
            "Airline": [],
            "JourneyType": [],
            "FlightNumber1": [],
            "FlightNumber2": [],
            "FlightNumber3": [],
            "FlightNumber4": [],
            "FlightNumber5": [],
            "FlightNumber6": [],
            "FlightNumber7": [],
            "DepartureDateLocal1": [],
            "DepartureDateLocal2": [],
            "DepartureDateLocal3": [],
            "DepartureDateLocal4": [],
            "DepartureDateLocal5": [],
            "DepartureDateLocal6": [],
            "DepartureDateLocal7": [],
            "Airport1": [],
            "Airport2": [],
            "Airport3": [],
            "Airport4": [],
            "Airport5": [],
            "Airport6": [],
            "Airport7": [],
            "Airport8": [],
        }
    )


# ==================================================
# CHUNKED PROCESSING FROM PARQUET
# ==================================================
def process_etl_chunked(source_path: Path, target_path: Path) -> None:
    """Main ETL pipeline with chunked processing to minimize memory usage"""

    start = time.time()
    log(f"⏰ ETL started at {now_str()}")

    # Get total rows using lazy scan
    log("📖 Reading source file info...")
    total_rows = pl.scan_parquet(source_path).select(pl.len()).collect().item()
    log(f"📊 Source rows: {total_rows:,}")

    # Process in chunks
    output_chunks = []
    offset = 0
    chunk_no = 0
    total_output_rows = 0

    while offset < total_rows:
        chunk_no += 1
        log(
            f"🔄 Processing chunk {chunk_no} (rows {offset:,} to {offset + CHUNK_SIZE:,})"
        )

        # Read chunk from parquet
        df_chunk = pl.scan_parquet(source_path).slice(offset, CHUNK_SIZE).collect()
        # Transform chunk
        df_long = unpivot_flights_chunk(df_chunk)
        if len(df_long) > 0:
            df_transformed = transform_chunk(df_long)
            if len(df_transformed) > 0:
                df_output = pivot_to_wide_format(df_transformed)
                output_chunks.append(df_output)
                total_output_rows += len(df_output)
                log(f"   ✅ Generated {len(df_output):,} output rows")

        # Clear memory
        del df_chunk, df_long

        offset += CHUNK_SIZE

    # Combine all chunks and write
    log("💾 Combining chunks and writing results...")

    if output_chunks:
        df_final = pl.concat(output_chunks, how="vertical_relaxed")

        # Select final columns in correct order
        final_cols = [
            "PaxName",
            "BookingRef",
            "ETicketNo",
            "ClientCode",
            "Airline",
            "JourneyType",
            "FlightNumber1",
            "FlightNumber2",
            "FlightNumber3",
            "FlightNumber4",
            "FlightNumber5",
            "FlightNumber6",
            "FlightNumber7",
            "DepartureDateLocal1",
            "DepartureDateLocal2",
            "DepartureDateLocal3",
            "DepartureDateLocal4",
            "DepartureDateLocal5",
            "DepartureDateLocal6",
            "DepartureDateLocal7",
            "Airport1",
            "Airport2",
            "Airport3",
            "Airport4",
            "Airport5",
            "Airport6",
            "Airport7",
            "Airport8",
        ]

        df_final = df_final.select(final_cols)
        df_final.write_parquet(target_path, compression="zstd")
    else:
        log("⚠️  No output data generated")
        create_empty_output_df().write_parquet(target_path, compression="zstd")

    elapsed = time.time() - start
    log(f"🎉 ETL completed in {int(elapsed // 60)}m {elapsed % 60:.2f}s")
    log(f"📊 Final row count: {total_output_rows:,}")


# ==================================================
# MAIN
# ==================================================
def main() -> None:
    # Check if source file exists
    if not SOURCE_FILE.exists():
        log(f"❌ Error: Source file not found at {SOURCE_FILE}")
        return

    # Ensure output directory exists
    TARGET_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Run ETL with chunked processing
    process_etl_chunked(SOURCE_FILE, TARGET_FILE)


if __name__ == "__main__":
    main()
