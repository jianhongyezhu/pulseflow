from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from pulseflow.benchmark_analysis import analyze_once, wait_for_events


def write_silver(path, rows):
    path.mkdir(parents=True)
    with duckdb.connect() as connection:
        connection.execute(
            """
            CREATE TABLE silver_events (
                attributes MAP(VARCHAR, VARCHAR),
                event_time TIMESTAMPTZ,
                processed_at TIMESTAMPTZ
            )
            """
        )
        connection.executemany(
            "INSERT INTO silver_events VALUES (?, ?, ?)",
            rows,
        )
        connection.execute(
            "COPY silver_events TO ? (FORMAT PARQUET)",
            [str(path / "events.parquet")],
        )


def test_benchmark_analysis_filters_run_and_reports_freshness_and_invalid_times(tmp_path):
    start = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    write_silver(
        tmp_path / "silver",
        [
            ({"run_id": "run-a"}, start, start + timedelta(seconds=1)),
            ({"run_id": "run-a"}, start, start + timedelta(seconds=3)),
            ({"run_id": "run-a"}, start, None),
            ({"run_id": "run-b"}, start, start + timedelta(seconds=100)),
        ],
    )

    report = analyze_once(tmp_path / "silver", "run-a")

    assert report["silver_events"] == 3
    assert report["missing_or_invalid_processing_timestamps"] == 1
    assert report["freshness_p50_seconds"] == 2.0
    assert report["freshness_p95_seconds"] == pytest.approx(2.9)


def test_empty_silver_has_no_fabricated_freshness_and_reports_discrepancy(tmp_path):
    report = wait_for_events(
        tmp_path / "missing",
        run_id="empty-run",
        accepted_requests=5,
        wait_seconds=1,
        poll_seconds=0.2,
    )

    assert report["silver_events"] == 0
    assert report["freshness_p50_seconds"] is None
    assert report["freshness_p95_seconds"] is None
    assert report["accepted_minus_silver_discrepancy"] == 5
