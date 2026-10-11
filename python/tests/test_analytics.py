from __future__ import annotations

from io import StringIO
from pathlib import Path

import duckdb
import pytest

from pulseflow import analytics
from pulseflow.analytics import format_table, run_analytics

SQL_PATH = Path(analytics.__file__).resolve().parents[2] / "analytics" / "sql" / "event-summary.sql"


@pytest.fixture
def silver_repository(tmp_path):
    sql_path = tmp_path / "analytics" / "sql" / "event-summary.sql"
    sql_path.parent.mkdir(parents=True)
    sql_path.write_text(SQL_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    silver_path = tmp_path / "data" / "silver" / "user_behaviour"
    silver_path.mkdir(parents=True)
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            """
            CREATE TABLE synthetic_silver (
                event_type VARCHAR,
                device_type VARCHAR,
                user_id VARCHAR,
                event_time TIMESTAMP,
                event_date DATE
            )
            """
        )
        connection.execute(
            """
            INSERT INTO synthetic_silver VALUES
                ('PLAY', 'mobile', 'user-1', TIMESTAMP '2026-01-01 10:00:00', DATE '2026-01-01'),
                ('PLAY', 'mobile', 'user-1', TIMESTAMP '2026-01-01 10:05:00', DATE '2026-01-01'),
                ('PLAY', 'mobile', 'user-2', TIMESTAMP '2026-01-02 09:00:00', DATE '2026-01-02'),
                ('PLAY', 'desktop', 'user-3', TIMESTAMP '2026-01-01 11:00:00', DATE '2026-01-01'),
                ('PAUSE', 'mobile', 'user-1', TIMESTAMP '2026-01-02 12:00:00', DATE '2026-01-02')
            """
        )
        connection.execute(
            f"""
            COPY synthetic_silver
            TO '{silver_path.as_posix()}'
            (FORMAT PARQUET, PARTITION_BY (event_date))
            """
        )
    return tmp_path, silver_path


def parse_table(output: str) -> tuple[list[str], list[list[str]]]:
    lines = output.strip().splitlines()
    headers = [column.strip() for column in lines[0].split("|")]
    rows = [[column.strip() for column in line.split("|")] for line in lines[2:]]
    return headers, rows


def test_production_sql_exists_and_loads():
    sql = SQL_PATH.read_text(encoding="utf-8")

    assert "read_parquet(" in sql
    assert "hive_partitioning = true" in sql
    assert "COUNT(*) AS event_count" in sql
    assert "COUNT(DISTINCT user_id) AS users" in sql
    assert "MIN(event_time) AS first_event_ts" in sql
    assert "MAX(event_time) AS latest_event_ts" in sql


def test_query_summarizes_hive_partitioned_silver_data(silver_repository, monkeypatch):
    root, silver_path = silver_repository
    partition_dirs = sorted(path.name for path in silver_path.glob("event_date=*"))
    assert partition_dirs == ["event_date=2026-01-01", "event_date=2026-01-02"]

    source_snapshot = {
        path: path.read_bytes() for path in silver_path.glob("**/*.parquet")
    }
    monkeypatch.chdir(root)
    output = StringIO()
    run_analytics(repository_root=root, output=output)

    headers, rows = parse_table(output.getvalue())
    assert headers == [
        "event_type",
        "device_type",
        "event_count",
        "users",
        "first_event_ts",
        "latest_event_ts",
    ]
    assert rows[0] == [
            "PLAY",
            "mobile",
            "3",
            "2",
            "2026-01-01 10:00:00",
            "2026-01-02 09:00:00",
    ]
    assert {int(row[2]) for row in rows} == {3, 1}
    assert [int(row[2]) for row in rows] == sorted(
        (int(row[2]) for row in rows),
        reverse=True,
    )
    assert {tuple(row) for row in rows[1:]} == {
        (
            "PLAY",
            "desktop",
            "1",
            "1",
            "2026-01-01 11:00:00",
            "2026-01-01 11:00:00",
        ),
        (
            "PAUSE",
            "mobile",
            "1",
            "1",
            "2026-01-02 12:00:00",
            "2026-01-02 12:00:00",
        ),
    }
    assert source_snapshot == {path: path.read_bytes() for path in source_snapshot}


def test_cli_reports_missing_silver_data(silver_repository, monkeypatch, capsys):
    root, silver_path = silver_repository
    for parquet_file in silver_path.glob("**/*.parquet"):
        parquet_file.unlink()
    monkeypatch.chdir(root)
    monkeypatch.setattr(analytics, "REPOSITORY_ROOT", root)

    assert analytics.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "No Silver Parquet files found" in captured.err
    assert "Traceback" not in captured.err


def test_cli_reports_success_without_spark_or_kafka(silver_repository, monkeypatch, capsys):
    root, _ = silver_repository
    monkeypatch.chdir(root)
    monkeypatch.setattr(analytics, "REPOSITORY_ROOT", root)

    assert analytics.main() == 0
    captured = capsys.readouterr()
    assert "event_type" in captured.out
    assert "event_count" in captured.out
    assert "PLAY" in captured.out
    assert captured.err == ""


def test_format_table_handles_empty_results_with_headers():
    assert format_table(["event_type", "event_count"], []) == (
        "event_type | event_count\n"
        "-----------+------------\n"
        "(no rows)"
    )
