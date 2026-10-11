import json
import logging
import random
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from pulseflow import generator
from pulseflow.generator import (
    DEFAULT_DUPLICATE_RATE,
    DEFAULT_EVENTS,
    DEFAULT_RPS,
    DEFAULT_URL,
    generate_events,
    make_event,
    parse_args,
)

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "user-behaviour-v1.schema.json"
with SCHEMA_PATH.open(encoding="utf-8") as schema_file:
    EVENT_SCHEMA = json.load(schema_file)
EVENT_VALIDATOR = Draft202012Validator(EVENT_SCHEMA, format_checker=FormatChecker())


def test_make_event_matches_user_behaviour_v1_contract():
    event = make_event(random.Random(10))

    assert set(event) == {
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "content_id",
        "device_type",
        "country_code",
        "event_ts",
        "attributes",
    }
    UUID(event["event_id"])
    assert event["user_id"].startswith("user-")
    assert event["session_id"].startswith("session-")
    assert event["event_type"] in {
        "PLAY",
        "PAUSE",
        "SKIP",
        "SEARCH",
        "LIKE",
        "ADD_TO_PLAYLIST",
        "PAGE_VIEW",
    }
    assert event["device_type"] in {"mobile", "desktop", "tablet", "tv"}
    assert event["country_code"] in {"GB", "US", "ES", "DE", "FR"}
    assert set(event["attributes"]) == {"source", "subscription_tier"}
    assert all(isinstance(value, str) for value in event["attributes"].values())

    timestamp = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    assert timestamp.tzinfo == UTC
    EVENT_VALIDATOR.validate(event)


def test_independently_generated_events_have_distinct_ids():
    rng = random.Random(20)

    first_event = make_event(rng)
    second_event = make_event(rng)

    assert first_event["event_id"] != second_event["event_id"]


def test_parse_args_uses_expected_defaults():
    args = parse_args([])

    assert args.url == DEFAULT_URL
    assert args.events == DEFAULT_EVENTS
    assert args.rps == DEFAULT_RPS
    assert args.duplicate_rate == DEFAULT_DUPLICATE_RATE


@pytest.mark.parametrize(
    "arguments",
    [
        ["--events", "0"],
        ["--events", "-1"],
        ["--rps", "0"],
        ["--rps", "nan"],
        ["--rps", "inf"],
        ["--duplicate-rate", "-0.1"],
        ["--duplicate-rate", "1.1"],
        ["--duplicate-rate", "nan"],
    ],
)
def test_parse_args_rejects_invalid_values(arguments):
    with pytest.raises(SystemExit):
        parse_args(arguments)


@pytest.mark.asyncio
async def test_duplicate_requests_reuse_complete_event_and_record_actual_status():
    payloads = []
    statuses = iter((202, 202, 409))

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(next(statuses))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        summary = await generate_events(
            count=3,
            rps=10_000,
            duplicate_rate=1,
            client=client,
            rng=random.Random(42),
        )

    assert payloads[1] == payloads[0]
    assert payloads[2] == payloads[0]
    assert payloads[1]["event_id"] == payloads[0]["event_id"]
    assert payloads[1]["event_ts"] == payloads[0]["event_ts"]
    assert summary.requested == 3
    assert summary.successful == 2
    assert summary.failed == 1
    assert summary.duplicates == 2
    assert summary.status_codes == {202: 2, 409: 1}


@pytest.mark.asyncio
async def test_zero_duplicate_rate_generates_a_new_event_for_every_request():
    payloads = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(202)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        summary = await generate_events(
            count=4,
            rps=10_000,
            duplicate_rate=0,
            client=client,
            rng=random.Random(17),
        )

    event_ids = [event["event_id"] for event in payloads]
    assert len(set(event_ids)) == 4
    assert summary.duplicates == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "successful", "failed"),
    [
        (200, 0, 1),
        (202, 1, 0),
        (204, 0, 1),
        (400, 0, 1),
        (503, 0, 1),
        (409, 0, 1),
        (500, 0, 1),
        (418, 0, 1),
    ],
)
async def test_only_http_202_is_counted_as_success(status_code, successful, failed):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status_code))
    ) as client:
        summary = await generate_events(
            count=1,
            rps=10_000,
            duplicate_rate=0,
            client=client,
            rng=random.Random(2),
        )

    assert summary.successful == successful
    assert summary.failed == failed
    assert summary.status_codes == {status_code: 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
async def test_request_errors_are_counted_reported_and_later_requests_continue(error_type, caplog):
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            raise error_type("request failed", request=request)
        return httpx.Response(202)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with caplog.at_level(logging.WARNING):
            summary = await generate_events(
                count=2,
                rps=10_000,
                duplicate_rate=0,
                client=client,
                rng=random.Random(5),
            )

    assert summary.successful == 1
    assert summary.failed == 1
    assert summary.status_codes == {202: 1}
    assert "Request 1 failed" in caplog.text


@pytest.mark.asyncio
async def test_unexpected_request_failure_is_logged_and_counted(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("unexpected transport defect")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with caplog.at_level(logging.ERROR):
            summary = await generate_events(
                count=1,
                rps=10_000,
                client=client,
                rng=random.Random(5),
            )

    assert summary.failed == 1
    assert "Unexpected failure while sending request 1" in caplog.text
    assert summary.status_codes == {}


def test_unexpected_request_failure_makes_cli_exit_nonzero(monkeypatch, caplog):
    async_client_class = httpx.AsyncClient

    def client_with_failing_transport(*, timeout):
        def handler(request: httpx.Request) -> httpx.Response:
            raise RuntimeError("unexpected transport defect")

        return async_client_class(
            timeout=timeout,
            transport=httpx.MockTransport(handler),
        )

    monkeypatch.setattr(generator.httpx, "AsyncClient", client_with_failing_transport)

    with caplog.at_level(logging.ERROR):
        exit_code = generator.main(["--events", "1", "--rps", "10_000"])

    assert exit_code == 1
    assert "Unexpected failure while sending request 1" in caplog.text


@pytest.mark.asyncio
async def test_request_pacing_uses_monotonic_interval():
    current_time = 100.0
    start_times = []
    sleeps = []

    def clock() -> float:
        return current_time

    async def sleep(delay: float) -> None:
        nonlocal current_time
        sleeps.append(delay)
        current_time += delay

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current_time
        start_times.append(clock())
        current_time += 0.2
        return httpx.Response(202)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await generate_events(
            count=3,
            rps=2,
            client=client,
            rng=random.Random(3),
            clock=clock,
            sleep=sleep,
        )

    assert start_times == [100.0, 100.5, 101.0]
    assert sleeps == pytest.approx([0.3, 0.3])
