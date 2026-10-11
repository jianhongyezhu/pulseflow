"""Run the repository's DuckDB summary query against Silver Parquet data."""

from __future__ import annotations

import sys
from contextlib import closing
from pathlib import Path
from typing import TextIO

import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SQL_PATH = REPOSITORY_ROOT / "analytics" / "sql" / "event-summary.sql"


def format_table(columns: list[str], rows: list[tuple[object, ...]]) -> str:
    """Format query results as a compact, headered text table."""
    values = [[str(value) if value is not None else "NULL" for value in row] for row in rows]
    widths = [
        max([len(column), *(len(row[index]) for row in values)])
        for index, column in enumerate(columns)
    ]
    lines = [
        " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns)),
        "-+-".join("-" * width for width in widths),
    ]
    lines.extend(
        " | ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip()
        for row in values
    )
    if not rows:
        lines.append("(no rows)")
    return "\n".join(lines)


def run_analytics(
    repository_root: Path | None = None,
    output: TextIO | None = None,
) -> None:
    """Load the SQL file, execute it in-memory and print the summary table."""
    root = repository_root or REPOSITORY_ROOT
    sql_path = root / "analytics" / "sql" / "event-summary.sql"
    silver_path = root / "data" / "silver" / "user_behaviour"
    if not any(silver_path.glob("**/*.parquet")):
        raise FileNotFoundError(f"No Silver Parquet files found under {silver_path}")

    sql = sql_path.read_text(encoding="utf-8")
    with closing(duckdb.connect(database=":memory:")) as connection:
        cursor = connection.execute(sql)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
    print(format_table(columns, rows), file=output)


def main() -> int:
    try:
        run_analytics()
    except FileNotFoundError as error:
        print(f"Analytics data or SQL file unavailable: {error}", file=sys.stderr)
        return 1
    except duckdb.Error as error:
        print(f"DuckDB analytics query failed: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"Unable to read analytics SQL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
