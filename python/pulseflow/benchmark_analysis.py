"""Summarize accepted k6 events observed in Silver Parquet."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def analyze_once(silver_dir: Path, run_id: str) -> dict[str, int | float | None | str]:
    parquet_files = sorted(silver_dir.rglob("*.parquet"))
    if not parquet_files:
        return {
            "run_id": run_id,
            "silver_events": 0,
            "missing_or_invalid_processing_timestamps": 0,
            "freshness_p50_seconds": None,
            "freshness_p95_seconds": None,
        }

    with duckdb.connect() as connection:
        relation = connection.read_parquet(
            [str(path) for path in parquet_files],
            hive_partitioning=True,
            union_by_name=True,
        )
        connection.register("silver_events", relation)
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS silver_events,
                COUNT(*) FILTER (
                    WHERE processed_at IS NULL
                       OR event_time IS NULL
                       OR processed_at < event_time
                ) AS missing_or_invalid_processing_timestamps,
                quantile_cont(epoch(processed_at) - epoch(event_time), 0.50)
                    FILTER (
                        WHERE processed_at IS NOT NULL
                          AND event_time IS NOT NULL
                          AND processed_at >= event_time
                    ) AS freshness_p50_seconds,
                quantile_cont(epoch(processed_at) - epoch(event_time), 0.95)
                    FILTER (
                        WHERE processed_at IS NOT NULL
                          AND event_time IS NOT NULL
                          AND processed_at >= event_time
                    ) AS freshness_p95_seconds
            FROM silver_events
            WHERE attributes['run_id'] = ?
            """,
            [run_id],
        ).fetchone()

    return {
        "run_id": run_id,
        "silver_events": row[0],
        "missing_or_invalid_processing_timestamps": row[1],
        "freshness_p50_seconds": row[2],
        "freshness_p95_seconds": row[3],
    }


def wait_for_events(
    silver_dir: Path,
    run_id: str,
    accepted_requests: int,
    wait_seconds: int,
    poll_seconds: float,
) -> dict[str, int | float | None | str]:
    deadline = time.monotonic() + wait_seconds
    result = analyze_once(silver_dir, run_id)
    while result["silver_events"] < accepted_requests and time.monotonic() < deadline:
        time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
        result = analyze_once(silver_dir, run_id)

    result["accepted_requests"] = accepted_requests
    result["accepted_minus_silver_discrepancy"] = (
        accepted_requests - int(result["silver_events"])
    )
    result["processing_wait_seconds"] = wait_seconds
    return result


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed


def nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Wait a bounded interval, then compare k6 accepted requests with Silver."
    )
    parser.add_argument("--silver-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--accepted-requests", type=nonnegative_int, required=True)
    parser.add_argument("--wait-seconds", type=positive_int, default=60)
    parser.add_argument("--poll-seconds", type=positive_float, default=2.0)
    args = parser.parse_args()
    report = wait_for_events(
        args.silver_dir,
        args.run_id,
        args.accepted_requests,
        args.wait_seconds,
        args.poll_seconds,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
