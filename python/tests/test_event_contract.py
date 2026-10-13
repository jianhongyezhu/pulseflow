import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "user-behaviour-v1.schema.json"
EVENT = {
    "event_id": "8b3caad0-b708-4cc4-9008-27862f8f8e5e",
    "user_id": "user-123",
    "session_id": "session-456",
    "event_type": "PLAY",
    "content_id": "content-789",
    "device_type": "mobile",
    "country_code": "US",
    "event_ts": "2025-05-04T12:30:00Z",
    "attributes": {"source": "search"},
}
with SCHEMA_PATH.open(encoding="utf-8") as schema_file:
    EVENT_SCHEMA = json.load(schema_file)
EVENT_VALIDATOR = Draft202012Validator(EVENT_SCHEMA, format_checker=FormatChecker())


def test_canonical_schema_accepts_representative_v1_event():
    EVENT_VALIDATOR.validate(EVENT)


@pytest.mark.parametrize(
    "updates",
    [
        {"event_id": "not-a-uuid"},
        {"event_type": "SHARE"},
        {"country_code": "gbr"},
        {"user_id": 123},
        {"attributes": {"rank": 5}},
    ],
)
def test_canonical_schema_rejects_invalid_event_values(updates):
    event = copy.deepcopy(EVENT)
    event.update(updates)

    assert list(EVENT_VALIDATOR.iter_errors(event))


def test_canonical_schema_rejects_unknown_top_level_properties():
    event = {**EVENT, "unexpected": "value"}

    assert list(EVENT_VALIDATOR.iter_errors(event))


def test_schema_min_length_allows_blank_user_id_while_runtime_layers_reject_it():
    event = {**EVENT, "user_id": " "}

    EVENT_VALIDATOR.validate(event)
