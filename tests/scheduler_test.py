import threading
import time as real_time
from email.utils import formatdate
from unittest import TestCase
from unittest.mock import Mock, patch

from gspread.exceptions import APIError
from gspread.http_client import HTTPClient
from gspread.scheduler import (
    BatchReadScheduler,
    ReadMethod,
    ReadTask,
    SchedulerConfig,
    _parse_retry_after,
)


class FakeClock:
    """Deterministic stand-in for the time module used by the scheduler."""

    def __init__(self, wall=1_700_000_000.0):
        self.now = 0.0
        self.wall = wall

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.now += delay

    def time(self):
        return self.wall


def make_api_error(status, headers=None, message="request failed"):
    response = Mock()
    response.ok = False
    response.status_code = status
    response.text = message
    response.headers = headers or {}
    response.json.return_value = {
        "error": {"code": status, "message": message, "status": "UNKNOWN"}
    }
    return APIError(response)


class RecordingClient:
    """Mimics the HTTPClient read methods.

    Records the (fake) time of every call and plays back a script of
    outcomes: a dict is returned, an exception is raised, a callable is
    called to produce the outcome.
    """

    def __init__(self, clock=None, script=()):
        self.clock = clock
        self.script = list(script)
        self.calls = []

    def _next(self, method, spreadsheet_id, ranges, params):
        now = self.clock.monotonic() if self.clock else real_time.monotonic()
        self.calls.append((method, now, spreadsheet_id, list(ranges), params))
        outcome = self.script.pop(0) if self.script else {"values": []}
        if callable(outcome):
            outcome = outcome()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def values_get(self, spreadsheet_id, range, params=None):
        return self._next("values_get", spreadsheet_id, [range], params)

    def values_batch_get(self, spreadsheet_id, ranges, params=None):
        return self._next("values_batch_get", spreadsheet_id, ranges, params)


def make_scheduler(clients, clock=None, **config_overrides):
    config = SchedulerConfig(**config_overrides)
    scheduler = BatchReadScheduler(clients=clients, config=config)
    if clock is not None:
        patcher = patch("gspread.scheduler.time", clock)
        patcher.start()
        scheduler._clock_patcher = patcher
    return scheduler


def task(user, index=0, **kwargs):
    kwargs.setdefault("spreadsheet_id", "spreadsheet-{}".format(index))
    kwargs.setdefault("ranges", ["Sheet1!A1:B2"])
    return ReadTask(user=user, **kwargs)


class ParseRetryAfterTest(TestCase):
    def test_delta_seconds(self):
        self.assertEqual(_parse_retry_after("30", now=1000.0), 30.0)
        self.assertEqual(_parse_retry_after("0", now=1000.0), 0.0)
        self.assertEqual(_parse_retry_after("  2.5 ", now=1000.0), 2.5)

    def test_negative_delta_seconds_clamps_to_zero(self):
        self.assertEqual(_parse_retry_after("-5", now=1000.0), 0.0)

    def test_http_date(self):
        date = formatdate(1005.0, usegmt=True)
        self.assertEqual(_parse_retry_after(date, now=1000.0), 5.0)

    def test_http_date_in_the_past_clamps_to_zero(self):
        date = formatdate(900.0, usegmt=True)
        self.assertEqual(_parse_retry_after(date, now=1000.0), 0.0)

    def test_unparseable_values(self):
        self.assertIsNone(_parse_retry_after(None, now=1000.0))
        self.assertIsNone(_parse_retry_after("", now=1000.0))
        self.assertIsNone(_parse_retry_after("not-a-date", now=1000.0))
        self.assertIsNone(_parse_retry_after("nan", now=1000.0))


class SchedulerConfigTest(TestCase):
    def test_defaults_are_valid(self):
        SchedulerConfig()

    def test_invalid_values_are_rejected(self):
        with self.assertRaises(ValueError):
            SchedulerConfig(max_concurrency=0)
        with self.assertRaises(ValueError):
            SchedulerConfig(project_max_requests=0)
        with self.assertRaises(ValueError):
            SchedulerConfig(user_max_requests=0)
        with self.assertRaises(ValueError):
            SchedulerConfig(window_seconds=0)
        with self.assertRaises(ValueError):
            SchedulerConfig(max_retries=-1)
        with self.assertRaises(ValueError):
            SchedulerConfig(backoff_multiplier=0.5)

    def test_backoff_delay_is_exponential_and_capped(self):
        config = SchedulerConfig(
            backoff_initial=2.0, backoff_multiplier=3.0, backoff_max=20.0
        )
        self.assertEqual(config.backoff_delay(0), 2.0)
        self.assertEqual(config.backoff_delay(1), 6.0)
        self.assertEqual(config.backoff_delay(2), 18.0)
        self.assertEqual(config.backoff_delay(3), 20.0)


class SchedulerValidationTest(TestCase):
    def test_requires_at_least_one_client(self):
        with self.assertRaises(ValueError):
            BatchReadScheduler(clients={})

    def test_unknown_user_is_rejected(self):
        scheduler = BatchReadScheduler(clients={"alice": RecordingClient()})
        with self.assertRaises(ValueError):
            scheduler.schedule([task("bob")])

    def test_values_get_requires_exactly_one_range(self):
        scheduler = BatchReadScheduler(clients={"alice": RecordingClient()})
        with self.assertRaises(ValueError):
            scheduler.schedule(
                [task("alice", ranges=["A1", "B2"], method=ReadMethod.VALUES_GET)]
            )

    def test_empty_ranges_are_rejected(self):
        scheduler = BatchReadScheduler(clients={"alice": RecordingClient()})
        with self.assertRaises(ValueError):
            scheduler.schedule([task("alice", ranges=[])])

    def test_only_read_methods_can_be_scheduled(self):
        scheduler = BatchReadScheduler(clients={"alice": RecordingClient()})
        with self.assertRaises(ValueError):
            scheduler.schedule([task("alice", method="values_update")])


class SchedulerConcurrencyTest(TestCase):
    def test_max_concurrency_is_respected(self):
        current = 0
        max_seen = 0
        lock = threading.Lock()

        def slow_read():
            nonlocal current, max_seen
            with lock:
                current += 1
                max_seen = max(max_seen, current)
            real_time.sleep(0.02)
            with lock:
                current -= 1
            return {"values": []}

        client = RecordingClient(script=[slow_read] * 6)
        scheduler = make_scheduler(
            {"alice": client},
            max_concurrency=2,
            project_max_requests=100,
            user_max_requests=100,
        )
        results = scheduler.schedule([task("alice", i) for i in range(6)])

        self.assertTrue(all(result.ok for result in results))
        self.assertEqual(max_seen, 2)

    def test_results_follow_submission_order(self):
        def slow():
            real_time.sleep(0.05)
            return {"user": "alice", "n": 0}

        alice = RecordingClient(script=[slow, {"user": "alice", "n": 2}])
        bob = RecordingClient(script=[{"user": "bob", "n": 1}, {"user": "bob", "n": 3}])
        scheduler = make_scheduler(
            {"alice": alice, "bob": bob},
            max_concurrency=4,
            project_max_requests=100,
            user_max_requests=100,
        )
        tasks = [task("alice", 0), task("bob", 1), task("alice", 2), task("bob", 3)]
        results = scheduler.schedule(tasks)

        self.assertEqual([result.index for result in results], [0, 1, 2, 3])
        self.assertEqual([result.task for result in results], tasks)
        self.assertEqual(
            [result.value for result in results],
            [
                {"user": "alice", "n": 0},
                {"user": "bob", "n": 1},
                {"user": "alice", "n": 2},
                {"user": "bob", "n": 3},
            ],
        )


class SchedulerQuotaTest(TestCase):
    def test_project_window_limits_total_requests(self):
        clock = FakeClock()
        client = RecordingClient(clock=clock)
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=4,
            project_max_requests=2,
            user_max_requests=100,
            window_seconds=10,
        )
        scheduler.schedule([task("alice", i) for i in range(3)])

        times = [call[1] for call in client.calls]
        self.assertEqual(times, [0.0, 0.0, 10.0])

    def test_user_window_limits_only_that_user(self):
        clock = FakeClock()
        alice = RecordingClient(clock=clock)
        bob = RecordingClient(clock=clock)
        scheduler = make_scheduler(
            {"alice": alice, "bob": bob},
            clock,
            max_concurrency=4,
            project_max_requests=100,
            user_max_requests=1,
            window_seconds=10,
        )
        scheduler.schedule([task("alice", 0), task("alice", 1), task("bob", 2)])

        # alice's second task waits for her window to free up...
        self.assertEqual([call[1] for call in alice.calls], [0.0, 10.0])
        # ...while bob's task goes through immediately
        self.assertEqual([call[1] for call in bob.calls], [0.0])

    def test_every_real_request_counts_toward_the_quota(self):
        # The failed first attempt consumes quota, so the retry cannot go
        # at its backoff time (t=1) and must wait for the window (t=10).
        # The retry itself consumes quota too, pushing the next task to
        # the next window (t=20).
        clock = FakeClock()
        client = RecordingClient(
            clock=clock, script=[make_api_error(500), {"values": [["ok"]]}]
        )
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=1,
            window_seconds=10,
            backoff_initial=1.0,
        )
        results = scheduler.schedule([task("alice", 0), task("alice", 1)])

        self.assertEqual([call[1] for call in client.calls], [0.0, 10.0, 20.0])
        self.assertTrue(all(result.ok for result in results))
        self.assertEqual(results[0].attempts, 2)

    def test_quota_accounting_persists_across_schedule_calls(self):
        clock = FakeClock()
        client = RecordingClient(clock=clock)
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=2,
            project_max_requests=2,
            user_max_requests=100,
            window_seconds=10,
        )
        scheduler.schedule([task("alice", 0), task("alice", 1)])
        scheduler.schedule([task("alice", 2)])

        self.assertEqual([call[1] for call in client.calls], [0.0, 0.0, 10.0])


class SchedulerRetryTest(TestCase):
    def test_retryable_5xx_follows_configured_backoff_and_limit(self):
        clock = FakeClock()
        client = RecordingClient(clock=clock, script=[make_api_error(503)] * 3)
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
            max_retries=2,
            backoff_initial=2.0,
            backoff_multiplier=2.0,
        )
        (result,) = scheduler.schedule([task("alice")])

        # attempts at t=0, t=2, t=6 then the retry budget is exhausted
        self.assertEqual([call[1] for call in client.calls], [0.0, 2.0, 6.0])
        self.assertFalse(result.ok)
        self.assertEqual(result.attempts, 3)
        self.assertIsInstance(result.error, APIError)
        self.assertEqual(result.error.code, 503)

    def test_5xx_retry_then_success(self):
        clock = FakeClock()
        client = RecordingClient(
            clock=clock, script=[make_api_error(500), {"values": [["ok"]]}]
        )
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
            backoff_initial=5.0,
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual([call[1] for call in client.calls], [0.0, 5.0])
        self.assertTrue(result.ok)
        self.assertEqual(result.value, {"values": [["ok"]]})
        self.assertEqual(result.attempts, 2)

    def test_429_cools_down_only_that_user_with_retry_after_seconds(self):
        clock = FakeClock()
        alice = RecordingClient(
            clock=clock,
            script=[
                make_api_error(429, headers={"Retry-After": "7"}),
                {"values": [["a1"]]},
                {"values": [["a2"]]},
            ],
        )
        bob = RecordingClient(clock=clock, script=[{"values": [["b"]]}])
        scheduler = make_scheduler(
            {"alice": alice, "bob": bob},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
        )
        results = scheduler.schedule(
            [task("alice", 0), task("alice", 1), task("bob", 2)]
        )

        # alice cools down for 7s after the 429; bob is not affected
        self.assertEqual([call[1] for call in alice.calls], [0.0, 7.0, 7.0])
        self.assertEqual([call[1] for call in bob.calls], [0.0])
        self.assertTrue(all(result.ok for result in results))
        self.assertEqual(results[0].attempts, 2)

    def test_429_supports_http_date_retry_after(self):
        clock = FakeClock(wall=1_700_000_000.0)
        retry_date = formatdate(clock.wall + 30.0, usegmt=True)
        client = RecordingClient(
            clock=clock,
            script=[
                make_api_error(429, headers={"Retry-After": retry_date}),
                {"values": [["ok"]]},
            ],
        )
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual([call[1] for call in client.calls], [0.0, 30.0])
        self.assertTrue(result.ok)

    def test_429_without_retry_after_uses_default_cooldown(self):
        clock = FakeClock()
        client = RecordingClient(
            clock=clock, script=[make_api_error(429), {"values": [["ok"]]}]
        )
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
            default_retry_after=12.0,
        )
        scheduler.schedule([task("alice")])

        self.assertEqual([call[1] for call in client.calls], [0.0, 12.0])

    def test_429_retries_are_limited(self):
        clock = FakeClock()
        client = RecordingClient(
            clock=clock,
            script=[make_api_error(429, headers={"Retry-After": "3"})] * 2,
        )
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
            max_retries=1,
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual([call[1] for call in client.calls], [0.0, 3.0])
        self.assertFalse(result.ok)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(result.error.code, 429)


class SchedulerFailureTest(TestCase):
    def test_permission_error_returns_immediately(self):
        clock = FakeClock()
        client = RecordingClient(clock=clock, script=[make_api_error(403)])
        scheduler = make_scheduler(
            {"alice": client},
            clock,
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual(len(client.calls), 1)
        self.assertFalse(result.ok)
        self.assertEqual(result.attempts, 1)
        self.assertIsInstance(result.error, APIError)
        self.assertEqual(result.error.code, 403)

    def test_other_client_errors_are_not_retried(self):
        client = RecordingClient(script=[make_api_error(404)])
        scheduler = make_scheduler(
            {"alice": client}, project_max_requests=100, user_max_requests=100
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(result.error.code, 404)

    def test_failure_keeps_concrete_reason_and_does_not_sink_the_batch(self):
        forbidden = make_api_error(403, message="The caller does not have permission")
        client = RecordingClient(
            script=[{"values": [["a"]]}, forbidden, {"values": [["c"]]}]
        )
        scheduler = make_scheduler(
            {"alice": client},
            max_concurrency=1,
            project_max_requests=100,
            user_max_requests=100,
        )
        results = scheduler.schedule([task("alice", i) for i in range(3)])

        self.assertEqual([result.ok for result in results], [True, False, True])
        self.assertIs(results[1].error, forbidden)
        self.assertEqual(results[1].error.code, 403)
        self.assertIn(
            "The caller does not have permission", results[1].error.error["message"]
        )
        self.assertEqual(results[0].value, {"values": [["a"]]})
        self.assertEqual(results[2].value, {"values": [["c"]]})

    def test_non_api_error_is_reported_without_retry(self):
        client = RecordingClient(script=[ConnectionError("connection reset")])
        scheduler = make_scheduler(
            {"alice": client}, project_max_requests=100, user_max_requests=100
        )
        (result,) = scheduler.schedule([task("alice")])

        self.assertEqual(len(client.calls), 1)
        self.assertIsInstance(result.error, ConnectionError)

    def test_unwrap_returns_value_or_raises_the_error(self):
        client = RecordingClient(script=[{"values": [["a"]]}, make_api_error(500)])
        scheduler = make_scheduler(
            {"alice": client},
            max_retries=0,
            project_max_requests=100,
            user_max_requests=100,
        )
        ok, failed = scheduler.schedule([task("alice", 0), task("alice", 1)])

        self.assertEqual(ok.unwrap(), {"values": [["a"]]})
        with self.assertRaises(APIError):
            failed.unwrap()


class SchedulerMethodTest(TestCase):
    def test_values_get_reads_a_single_range(self):
        client = RecordingClient()
        scheduler = make_scheduler(
            {"alice": client}, project_max_requests=100, user_max_requests=100
        )
        scheduler.schedule(
            [task("alice", ranges="Sheet1!A1", method=ReadMethod.VALUES_GET)]
        )

        method, _, spreadsheet_id, ranges, _ = client.calls[0]
        self.assertEqual(method, "values_get")
        self.assertEqual(ranges, ["Sheet1!A1"])

    def test_values_batch_get_is_the_default(self):
        client = RecordingClient()
        scheduler = make_scheduler(
            {"alice": client}, project_max_requests=100, user_max_requests=100
        )
        scheduler.schedule([task("alice", ranges=["A1", "B2"])])

        method, _, spreadsheet_id, ranges, _ = client.calls[0]
        self.assertEqual(method, "values_batch_get")
        self.assertEqual(ranges, ["A1", "B2"])

    def test_params_are_forwarded_and_not_mutated(self):
        session = Mock()
        response = Mock()
        response.ok = True
        response.json.return_value = {"valueRanges": []}
        session.request.return_value = response
        client = HTTPClient(auth=None, session=session)

        scheduler = BatchReadScheduler(clients={"alice": client})
        params = {"valueRenderOption": "FORMULA"}
        read_task = ReadTask(
            user="alice", spreadsheet_id="sid", ranges=["A1"], params=params
        )
        (result,) = scheduler.schedule([read_task])

        self.assertTrue(result.ok)
        self.assertEqual(result.value, {"valueRanges": []})
        # the API client injects "ranges" into the params it receives:
        _, kwargs = session.request.call_args
        self.assertEqual(kwargs["params"]["ranges"], ["A1"])
        self.assertEqual(kwargs["params"]["valueRenderOption"], "FORMULA")
        # ...but the task's own params must stay untouched
        self.assertEqual(params, {"valueRenderOption": "FORMULA"})
