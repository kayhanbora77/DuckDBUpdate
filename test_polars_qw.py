import time
from pathlib import Path
from typing import Optional, List, Dict, Any
import re

import polars as pl


# ==============================================================================
# CONFIGURATION
# ==============================================================================
class Config:
    SOURCE_FILE = Path("/home/kayhan/Desktop/Gelen_Datalar/TBO/TBO3_MASTER.parquet")
    TARGET_FILE = Path("/home/kayhan/Desktop/Gelen_Datalar/TBO/TBO3_MASTER_QW.parquet")

    CHUNK_SIZE = 500_000
    VALID_YEAR_MIN = 1990
    VALID_YEAR_MAX = 2100
    TIME_GAP_HOURS = 36


# ==============================================================================
# LOGGING UTILITIES
# ==============================================================================
def log(msg: str) -> None:
    """Print timestamped log message."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{timestamp}] {msg}", flush=True)


# ==============================================================================
# CLEANING HELPERS
# ==============================================================================
def is_rnk_flight(flight: Optional[str]) -> bool:
    """Check if flight number is an RNK flight (ends with '000')."""
    return flight is not None and flight.endswith("000")


def normalize_flight_number(flight: Optional[str]) -> Optional[str]:
    """Normalize flight number: e.g., 'AA0123' → 'AA123'."""
    if not flight:
        return None

    flight = flight.strip().upper()
    match = re.match(r"^([A-Z]{2,3})(0*)(\d+)$", flight)
    if match:
        airline, _, number = match.groups()
        return f"{airline}{number}"
    return flight


# ==============================================================================
# SCHEMA & COLUMN UTILITIES
# ==============================================================================
def get_output_columns() -> List[str]:
    """Return ordered list of expected output columns."""
    return [
        "PaxName",
        "BookingRef",
        "ETicketNo",
        "ClientCode",
        "Airline",
        "JourneyType",
        *(f"FlightNumber{i}" for i in range(1, 8)),
        *(f"DepartureDateLocal{i}" for i in range(1, 8)),
        *(f"Airport{i}" for i in range(1, 9)),
    ]


def create_empty_output_df() -> pl.DataFrame:
    """Create empty DataFrame with correct schema."""
    schema: Dict[str, pl.DataType] = {col: pl.Utf8 for col in get_output_columns()}
    schema["DepartureDateLocal1"] = pl.Datetime("us")  # Example—adjust if needed
    # Override datetime columns
    for i in range(1, 8):
        schema[f"DepartureDateLocal{i}"] = pl.Datetime("us")

    return pl.DataFrame(schema=schema).clear()


# ==============================================================================
# DATA TRANSFORMATION
# ==============================================================================
def unpivot_flights_chunk(df: pl.DataFrame) -> pl.DataFrame:
    """Convert wide-format flight data to long format."""
    segments = []
    for i in range(1, 8):
        flt_col = f"FlightNumber{i}"
        if flt_col not in df.columns:
            continue

        segment = df.select(
            pl.col("PaxName"),
            pl.col("BookingRef"),
            pl.col("ClientCode"),
            pl.col("Airline"),
            pl.col("JourneyType"),
            pl.col(flt_col).cast(pl.Utf8).alias("FlightNumber"),
            pl.col(f"DepartureDateLocal{i}").cast(pl.Utf8).alias("DepartureDate"),
            pl.col(f"Airport{i}").cast(pl.Utf8).alias("DepAir"),
            pl.col(f"Airport{i + 1}").cast(pl.Utf8).alias("ArrAir"),
            pl.lit(i, dtype=pl.Int32).alias("OriginalSeq"),
        ).filter(
            pl.col("FlightNumber").is_not_null()
            & (pl.col("FlightNumber").str.strip_chars() != "")
        )

        segments.append(segment)

    return pl.concat(segments, how="vertical_relaxed") if segments else pl.DataFrame()


def transform_chunk(df_long: pl.DataFrame) -> pl.DataFrame:
    """Apply cleaning, filtering, deduplication, and trip segmentation."""
    if df_long.is_empty():
        return pl.DataFrame()

    time_gap_seconds = Config.TIME_GAP_HOURS * 3600

    df_clean = df_long.with_columns(
        pl.col("FlightNumber")
        .map_elements(normalize_flight_number, return_dtype=pl.Utf8)
        .alias("clean_flt"),
        pl.col("DepartureDate")
        .str.to_datetime(strict=False, time_unit="us")
        .alias("clean_dte"),
    ).filter(
        pl.col("clean_flt").is_not_null()
        & pl.col("clean_dte").is_not_null()
        & pl.col("clean_dte")
        .dt.year()
        .is_between(Config.VALID_YEAR_MIN, Config.VALID_YEAR_MAX)
        & ~pl.col("clean_flt").str.ends_with("000")
    )

    if df_clean.is_empty():
        return pl.DataFrame()

    df_dedup = (
        df_clean.sort(["BookingRef", "PaxName", "clean_dte", "OriginalSeq"])
        .with_columns(
            pl.col("OriginalSeq")
            .rank("ordinal")
            .over(["BookingRef", "PaxName", "clean_dte"])
            .alias("rn")
        )
        .filter(pl.col("rn") == 1)
        .drop("rn")
    )

    df_trips = (
        df_dedup.sort(["BookingRef", "PaxName", "OriginalSeq"])
        .with_columns(
            pl.col("clean_dte")
            .shift(1)
            .over(["BookingRef", "PaxName"])
            .alias("prev_dte")
        )
        .with_columns(
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
        )
        .with_columns(
            pl.col("is_trip_start")
            .cum_sum()
            .over(["BookingRef", "PaxName"])
            .alias("trip_id")
        )
        .drop(["prev_dte", "is_trip_start"])
    )

    df_seq = df_trips.with_columns(
        pl.col("OriginalSeq")
        .rank("ordinal")
        .over(["BookingRef", "PaxName", "trip_id"])
        .alias("seq_id")
    )

    return df_seq


def pivot_to_wide_format(df: pl.DataFrame) -> pl.DataFrame:
    """Pivot long-format data back to wide format (max 7 segments)."""
    if df.is_empty():
        return create_empty_output_df()

    agg_exprs = []

    for i in range(1, 8):
        agg_exprs.extend(
            [
                pl.col("clean_flt")
                .filter(pl.col("seq_id") == i)
                .first()
                .alias(f"FlightNumber{i}"),
                pl.col("clean_dte")
                .filter(pl.col("seq_id") == i)
                .first()
                .alias(f"DepartureDateLocal{i}"),
            ]
        )

    # Airports: Airport1 = first DepAir, Airport2-8 = ArrAir of segments 1-7
    agg_exprs.append(
        pl.col("DepAir").filter(pl.col("seq_id") == 1).first().alias("Airport1")
    )
    for i in range(1, 8):
        agg_exprs.append(
            pl.col("ArrAir")
            .filter(pl.col("seq_id") == i)
            .first()
            .alias(f"Airport{i + 1}")
        )

    # Metadata columns
    agg_exprs.extend(
        [
            pl.col("ClientCode").first(),
            pl.col("Airline").first(),
            pl.col("JourneyType").first(),
        ]
    )

    df_wide = df.group_by(["PaxName", "BookingRef", "trip_id"]).agg(agg_exprs)

    return (
        df_wide.with_columns(pl.lit(None, dtype=pl.Utf8).alias("ETicketNo"))
        .drop("trip_id")
        .select(get_output_columns())
    )


# ==============================================================================
# CHUNKED ETL PIPELINE
# ==============================================================================
def process_etl_chunked(source_path: Path, target_path: Path) -> None:
    """Main ETL pipeline with memory-efficient chunked processing."""
    log("Starting ETL pipeline...")

    # Get total rows
    total_rows = pl.scan_parquet(source_path).select(pl.len()).collect().item()
    log(f"Total source rows: {total_rows:,}")

    output_chunks: List[pl.DataFrame] = []
    offset = 0
    chunk_no = 0
    total_output_rows = 0

    while offset < total_rows:
        chunk_no += 1
        log(
            f"Processing chunk {chunk_no}: rows {offset:,} – {offset + Config.CHUNK_SIZE:,}"
        )

        df_chunk = (
            pl.scan_parquet(source_path).slice(offset, Config.CHUNK_SIZE).collect()
        )
        df_long = unpivot_flights_chunk(df_chunk)

        if not df_long.is_empty():
            df_transformed = transform_chunk(df_long)
            if not df_transformed.is_empty():
                df_wide = pivot_to_wide_format(df_transformed)
                output_chunks.append(df_wide)
                total_output_rows += df_wide.height
                log(f"  → Generated {df_wide.height:,} output rows")

        del df_chunk, df_long  # Free memory early
        offset += Config.CHUNK_SIZE

    # Final write
    if output_chunks:
        log("Combining and writing output...")
        df_final = pl.concat(output_chunks, how="vertical_relaxed")
        df_final.write_parquet(target_path, compression="zstd")
    else:
        log("⚠️ No valid data processed. Writing empty output.")
        create_empty_output_df().write_parquet(target_path, compression="zstd")

    log(f"ETL completed. Final rows: {total_output_rows:,}")


# ==============================================================================
# MAIN
# ==============================================================================
def main() -> None:
    if not Config.SOURCE_FILE.exists():
        log(f"❌ Source file not found: {Config.SOURCE_FILE}")
        return

    Config.TARGET_FILE.parent.mkdir(parents=True, exist_ok=True)
    process_etl_chunked(Config.SOURCE_FILE, Config.TARGET_FILE)


if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    log(f"✅ Total runtime: {int(elapsed // 60)}m {elapsed % 60:.2f}s")
