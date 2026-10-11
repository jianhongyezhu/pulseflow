"""Generate synthetic PulseFlow events and send them to the ingestion API."""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import random
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TypedDict

import httpx

DEFAULT_URL = "http://localhost:8080/v1/events"
DEFAULT_EVENTS = 1000
DEFAULT_RPS = 20.0
DEFAULT_DUPLICATE_RATE = 0.02
REQUEST_TIMEOUT_SECONDS = 5.0

EVENT_TYPES = (
    "PLAY",
    "PAUSE",
    "SKIP",
    "SEARCH",
    "LIKE",
    "ADD_TO_PLAYLIST",
    "PAGE_VIEW",
)
DEVICE_TYPES = ("mobile", "desktop", "tablet", "tv")
COUNTRY_CODES = ("GB", "US", "ES", "DE", "FR")
SOURCES = ("search", "playlist", "recommendation", "radio", "direct")
SUBSCRIPTION_TIERS = ("free", "premium", "family", "student")
CONTENT_IDS = (
    "track-midnight-drive",
    "track-summer-rain",
    "track-glass-harbour",
    "podcast-data-after-dark",
    "album-northern-lights",
)

logger = logging.getLogger(__name__)


class UserEvent(TypedDict):
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    content_id: str
    device_type: str
    country_code: str
    event_ts: str
    attributes: dict[str, str]


@dataclass
class RunSummary:
    requested: int
    successful: int = 0
    failed: int = 0
    duplicates: int = 0
    status_codes: Counter[int] = field(default_factory=Counter)


def make_event(rng: random.Random | None = None) -> UserEvent:
    """Create one synthetic user event conforming to the v1 event contract."""
    chooser = rng if rng is not None else random
    timestamp = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{chooser.randint(1, 10_000):05d}",
        "session_id": f"session-{uuid.uuid4()}",
        "event_type": chooser.choice(EVENT_TYPES),
        "content_id": chooser.choice(CONTENT_IDS),
        "device_type": chooser.choice(DEVICE_TYPES),
        "country_code": chooser.choice(COUNTRY_CODES),
        "event_ts": timestamp,
        "attributes": {
            "source": chooser.choice(SOURCES),
            "subscription_tier": chooser.choice(SUBSCRIPTION_TIERS),
        },
    }


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exception:
        raise argparse.ArgumentTypeError("must be a positive integer") from exception
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def positive_rps(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exception:
        raise argparse.ArgumentTypeError("must be a positive finite number") from exception
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def valid_duplicate_rate(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exception:
        raise argparse.ArgumentTypeError("must be between 0 and 1 inclusive") from exception
    if not math.isfinite(parsed) or not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1 inclusive")
    return parsed


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send synthetic PulseFlow events to the ingestion API."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="Event endpoint URL.")
    parser.add_argument(
        "--events",
        type=positive_int,
        default=DEFAULT_EVENTS,
        help="Number of requests.",
    )
    parser.add_argument(
        "--rps",
        type=positive_rps,
        default=DEFAULT_RPS,
        help="Target requests per second.",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=valid_duplicate_rate,
        default=DEFAULT_DUPLICATE_RATE,
        help="Probability of reusing a previously generated event (0 to 1).",
    )
    return parser.parse_args(args)


async def generate_events(
    url: str = DEFAULT_URL,
    count: int = DEFAULT_EVENTS,
    rps: float = DEFAULT_RPS,
    duplicate_rate: float = DEFAULT_DUPLICATE_RATE,
    *,
    client: httpx.AsyncClient | None = None,
    rng: random.Random | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RunSummary:
    """Generate and send events sequentially, optionally using an injected HTTP client."""
    if count <= 0:
        raise ValueError("count must be positive")
    if not math.isfinite(rps) or rps <= 0:
        raise ValueError("rps must be a positive finite number")
    if not math.isfinite(duplicate_rate) or not 0 <= duplicate_rate <= 1:
        raise ValueError("duplicate_rate must be between 0 and 1 inclusive")

    summary = RunSummary(requested=count)
    chooser = rng if rng is not None else random.Random()
    previous_events: list[UserEvent] = []
    next_request_at = clock()

    if client is None:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as owned_client:
            await _send_events(
                owned_client, url, count, rps, duplicate_rate,
                summary, previous_events, chooser, clock, sleep, next_request_at,
            )
    else:
        await _send_events(
            client, url, count, rps, duplicate_rate,
            summary, previous_events, chooser, clock, sleep, next_request_at,
        )

    return summary


async def _send_events(
    client: httpx.AsyncClient,
    url: str,
    count: int,
    rps: float,
    duplicate_rate: float,
    summary: RunSummary,
    previous_events: list[UserEvent],
    rng: random.Random,
    clock: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
    next_request_at: float,
) -> None:
    interval = 1.0 / rps

    for request_number in range(1, count + 1):
        delay = next_request_at - clock()
        if delay > 0:
            await sleep(delay)

        is_duplicate = bool(previous_events) and rng.random() < duplicate_rate
        if is_duplicate:
            event = rng.choice(previous_events)
            summary.duplicates += 1
        else:
            event = make_event(rng)
            previous_events.append(event)

        request_started_at = clock()
        try:
            response = await client.post(url, json=event)
        except httpx.RequestError as exception:
            summary.failed += 1
            logger.warning(
                "Request %d failed with %s: %s",
                request_number,
                type(exception).__name__,
                exception,
            )
        except Exception:
            summary.failed += 1
            logger.exception("Unexpected failure while sending request %d", request_number)
        else:
            summary.status_codes[response.status_code] += 1
            if response.status_code == 202:
                summary.successful += 1
            else:
                summary.failed += 1
                logger.warning(
                    "Request %d received HTTP %d",
                    request_number,
                    response.status_code,
                )

        next_request_at = request_started_at + interval


def print_summary(summary: RunSummary) -> None:
    print(f"Requests: {summary.requested}")
    print(f"Successful: {summary.successful}")
    print(f"Failed: {summary.failed}")
    print(f"Duplicate requests generated: {summary.duplicates}")
    if summary.status_codes:
        status_counts = ", ".join(
            f"{status}: {count}" for status, count in sorted(summary.status_codes.items())
        )
        print(f"HTTP status codes: {status_counts}")
    else:
        print("HTTP status codes: none")


def main(args: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    options = parse_args(args)
    try:
        summary = asyncio.run(
            generate_events(
                url=options.url,
                count=options.events,
                rps=options.rps,
                duplicate_rate=options.duplicate_rate,
            )
        )
    except KeyboardInterrupt:
        logger.error("Event generation interrupted")
        return 1
    print_summary(summary)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
