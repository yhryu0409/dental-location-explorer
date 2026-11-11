from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dental_market.connectors import (  # noqa: E402
    BaseConnector,
    ConnectorError,
    ConnectorRunner,
    FixtureConnector,
    InMemoryWatermarkStore,
    Page,
    PaginationLoopError,
    RateLimiter,
    RawResponse,
    Request,
    RetryPolicy,
    collect,
    parse_retry_after,
)


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class ConnectorRunnerTests(unittest.TestCase):
    def test_blank_cursor_and_watermark_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "cursor"):
            Request(dataset="items", cursor=" ")
        with self.assertRaisesRegex(ValueError, "snapshot_token"):
            Request(dataset="items", snapshot_token=" ")
        with self.assertRaisesRegex(ValueError, "watermark"):
            Page(watermark="")
        with self.assertRaisesRegex(ValueError, "source_as_of"):
            Page(source_as_of=" ")
        with self.assertRaisesRegex(ValueError, "snapshot_fingerprint"):
            Page(snapshot_fingerprint="not-a-digest")
        with self.assertRaisesRegex(ValueError, "record_count"):
            Page(record_count=-1)
        with self.assertRaisesRegex(ValueError, "exceed"):
            Page(record_count=2, total_count=1)

    def test_request_params_are_detached_and_recursively_immutable(self) -> None:
        caller_params = {
            "region": "busan",
            "filters": {"types": ["family", "implant"]},
        }
        request = Request(dataset="items", params=caller_params)

        caller_params["region"] = "ulsan"
        caller_params["filters"]["types"].append("esthetic")

        self.assertEqual(request.params["region"], "busan")
        self.assertEqual(
            request.params["filters"]["types"], ("family", "implant")
        )
        with self.assertRaisesRegex(TypeError, "immutable"):
            request.params["region"] = "ulsan"
        with self.assertRaisesRegex(TypeError, "immutable"):
            request.params["filters"]["new"] = True

    def test_paginates_deduplicates_and_commits_watermark(self) -> None:
        store = InMemoryWatermarkStore()
        connector = FixtureConnector(
            {
                "clinics": [
                    {"id": "a", "name": "A"},
                    {"id": "b", "name": "B"},
                    {"id": "b", "name": "B"},
                    {"id": "c", "name": "C"},
                ]
            },
            default_page_size=2,
        )
        runner = ConnectorRunner(connector, watermark_store=store)

        result = runner.collect(Request(dataset="clinics"))

        self.assertEqual(result.pages_fetched, 2)
        self.assertEqual(len(result.raw_pages), 2)
        self.assertEqual(connector.requested_cursors, [None, "2"])
        self.assertIsNone(result.starting_watermark)
        self.assertTrue(result.ending_watermark.startswith("sha256:"))
        self.assertEqual(
            store.get_watermark("fixture", "clinics"), result.ending_watermark
        )

        second = runner.collect(Request(dataset="clinics"))
        self.assertEqual(second.pages_fetched, 1)
        self.assertEqual(second.starting_watermark, result.ending_watermark)

    def test_raw_is_observed_before_pagination_decode_error(self) -> None:
        observed = []

        class MalformedConnector(BaseConnector):
            source = "malformed"
            parser_id = "jsonl-records"
            parser_version = "1"

            def fetch_raw_page(self, request: Request) -> RawResponse:
                return RawResponse(body=b"{not-json")

            def decode_page(
                self, request: Request, response: RawResponse
            ) -> Page:
                self.assert_raw_was_observed(observed, response.body)
                raise ConnectorError("malformed pagination", retryable=False)

            @staticmethod
            def assert_raw_was_observed(captures, body):
                assert captures and captures[0].raw_response.body == body

        with self.assertRaisesRegex(ConnectorError, "malformed pagination"):
            ConnectorRunner(
                MalformedConnector(), page_observer=observed.append
            ).collect(Request(dataset="clinics"))
        self.assertEqual(len(observed), 1)

    def test_decoded_observer_runs_after_raw_and_keeps_resume_page_index(self) -> None:
        events = []
        connector = FixtureConnector(
            {"items": [{"id": "b"}, {"id": "c"}]},
            default_page_size=1,
        )

        def observe_raw(captured) -> None:
            events.append(("raw", captured.page_index, captured.request_cursor))

        def observe_decoded(captured, page) -> None:
            events.append(
                ("decoded", captured.page_index, captured.request_cursor)
            )
            self.assertTrue(events[-2][0] == "raw")
            self.assertIsInstance(page, Page)

        result = ConnectorRunner(
            connector, page_observer=observe_raw
        ).collect_from(
            Request(dataset="items", cursor="1", page_size=1),
            initial_page_index=4,
            decoded_page_observer=observe_decoded,
        )

        self.assertEqual(result.pages_fetched, 1)
        self.assertEqual(events, [("raw", 4, "1"), ("decoded", 4, "1")])

    def test_resume_page_index_must_be_nonnegative_integer(self) -> None:
        runner = ConnectorRunner(FixtureConnector({"items": []}))
        with self.assertRaisesRegex(ValueError, "initial_page_index"):
            runner.collect_from(
                Request(dataset="items"), initial_page_index=-1
            )

    def test_simple_fixture_constructor_and_collect_helper(self) -> None:
        connector = FixtureConnector(
            [{"id": "1"}, {"id": "2"}], 1, dataset="clinics"
        )

        result = collect(connector, Request(dataset="clinics"))

        self.assertEqual(result.pages_fetched, 2)

    def test_retries_with_exponential_backoff_and_retry_after(self) -> None:
        fake_time = FakeTime()

        class FlakyConnector(BaseConnector):
            source = "flaky"

            def __init__(self) -> None:
                self.calls = 0

            def fetch_raw_page(self, request: Request) -> RawResponse:
                self.calls += 1
                if self.calls == 1:
                    raise ConnectorError("temporary", status_code=503)
                if self.calls == 2:
                    raise ConnectorError(
                        "throttled", status_code=429, retry_after="3"
                    )
                return RawResponse(body=b'{"id":"1"}\n')

            def decode_page(
                self, request: Request, response: RawResponse
            ) -> Page:
                return Page(watermark="complete")

        connector = FlakyConnector()
        runner = ConnectorRunner(
            connector,
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay_seconds=0.5,
                max_delay_seconds=10,
            ),
            sleeper=fake_time.sleep,
        )
        result = runner.collect(Request(dataset="items"))

        self.assertEqual(connector.calls, 3)
        self.assertEqual(fake_time.sleeps, [0.5, 3.0])
        self.assertEqual(result.pages_fetched, 1)

    def test_does_not_retry_non_retryable_error(self) -> None:
        fake_time = FakeTime()
        connector = FixtureConnector({"items": []})
        runner = ConnectorRunner(connector, sleeper=fake_time.sleep)

        with self.assertRaises(ConnectorError):
            runner.collect(Request(dataset="missing"))

        self.assertEqual(fake_time.sleeps, [])

    def test_pagination_loop_does_not_advance_watermark(self) -> None:
        signed_cursor = "signed-cursor-TOPSECRET"

        class LoopingConnector(BaseConnector):
            source = "loop"

            def fetch_raw_page(self, request: Request) -> RawResponse:
                return RawResponse(body=b"{}")

            def decode_page(
                self, request: Request, response: RawResponse
            ) -> Page:
                return Page(next_cursor=signed_cursor, watermark="new")

        store = InMemoryWatermarkStore()
        store.set_watermark("loop", "items", "old")
        runner = ConnectorRunner(LoopingConnector(), watermark_store=store)

        with self.assertRaises(PaginationLoopError) as caught:
            runner.collect(Request(dataset="items"))

        self.assertNotIn(signed_cursor, str(caught.exception))
        self.assertIn("fingerprint=", str(caught.exception))
        self.assertEqual(store.get_watermark("loop", "items"), "old")


class RateLimiterTests(unittest.TestCase):
    def test_enforces_minimum_interval(self) -> None:
        fake_time = FakeTime()
        limiter = RateLimiter(
            requests_per_second=0.5,
            clock=fake_time.clock,
            sleeper=fake_time.sleep,
        )

        limiter.wait()
        limiter.wait()
        limiter.wait()

        self.assertEqual(fake_time.sleeps, [2.0, 2.0])


class RetryAfterTests(unittest.TestCase):
    def test_parses_seconds_and_http_date(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)

        self.assertEqual(parse_retry_after("2.5", now=now), 2.5)
        self.assertEqual(
            parse_retry_after("Thu, 01 Jan 2026 00:00:04 GMT", now=now),
            4.0,
        )
        self.assertIsNone(parse_retry_after("not-a-date", now=now))


if __name__ == "__main__":
    unittest.main()
