import json
import pytest
from flights.services.ObtenerVuelos import fetch_flights

class DummyResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200
        self.text = json.dumps(payload)

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload

class DummySession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.auth = None
        self.headers = {}
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return DummyResponse(self.responses.pop(0))


def test_fetch_flights_pagination(monkeypatch):
    payloads = [
        {"data": [{"id": 1}], "moreResultsAvailable": True},
        {"data": [{"id": 2}], "moreResultsAvailable": False},
    ]

    def session_factory():
        return DummySession(payloads.copy())

    monkeypatch.setattr(
        "flights.services.ObtenerVuelos.requests.Session",
        lambda: session_factory(),
    )

    query = {"start": "2024-06-01", "end": "2024-06-02"}
    cfg = {"api_key": "k", "base_url": "http://t", "endpoint": "/flights"}
    records, stats = fetch_flights(query, cfg)

    assert [r["id"] for r in records] == [1, 2]
    assert stats["total"] == 2
    assert stats["requested_range"] == ("2024-06-01", "2024-06-02")