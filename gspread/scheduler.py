"""
gspread.scheduler
~~~~~~~~~~~~~~~~~

Batch read scheduling for the Google Sheets API.

This module provides :class:`BatchReadScheduler`, an entry point to
schedule **read-only** requests on behalf of several authorized users of
the same Google Cloud project. It enforces:

* a maximum number of concurrent in-flight requests,
* a sliding window quota on the total number of requests of the project,
* a sliding window quota on the number of requests of each user,
* a per-user cool-down when the API answers ``429 Too Many Requests``,
  honoring the ``Retry-After`` header in both its delta-seconds and
  HTTP-date forms,
* bounded exponential backoff retries on retryable ``5xx`` errors.

Every real HTTP request — first tries and retries alike — is accounted
in both sliding windows. While a user waits for quota or cools down,
the tasks of the other eligible users keep flowing.
"""

import math
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from http import HTTPStatus
from threading import Lock
from typing import Any, Deque, Dict, FrozenSet, Iterable, List, Mapping, Optional, Union

from .exceptions import APIError
from .http_client import HTTPClient, ParamsType

#: How long to wait before re-evaluating the queue when no task is
#: eligible and no future event date is known. Should never happen, this
#: is only a safety net against busy-looping.
_IDLE_POLL_SECONDS = 0.01


@dataclass(frozen=True)
class SchedulerConfig:
    """Tuning knobs of :class:`BatchReadScheduler`.

    :param int max_concurrency: maximum number of requests in flight at
        any time, across all users.
    :param int project_max_requests: maximum number of requests allowed
        inside one sliding window for the whole project (all users
        combined).
    :param int user_max_requests: maximum number of requests allowed
        inside one sliding window for a single user.
    :param float window_seconds: length of the sliding window, in seconds.
    :param int max_retries: maximum number of retries of a single task.
        The budget is shared by every retryable outcome (``429`` and
        retryable ``5xx``). ``0`` disables retries.
    :param float backoff_initial: delay before the first retry after a
        retryable ``5xx`` error, in seconds.
    :param float backoff_multiplier: multiplier applied to the backoff
        delay after each retryable ``5xx`` error.
    :param float backoff_max: upper bound of the backoff delay, in seconds.
    :param retryable_statuses: HTTP status codes, other than ``429``,
        that are retried with backoff. Defaults to
        ``{500, 502, 503, 504}``.
    :param float default_retry_after: cool-down applied when a ``429``
        response carries no usable ``Retry-After`` header, in seconds.
    """

    max_concurrency: int = 4
    project_max_requests: int = 300
    user_max_requests: int = 60
    window_seconds: float = 60.0
    max_retries: int = 5
    backoff_initial: float = 1.0
    backoff_multiplier: float = 2.0
    backoff_max: float = 60.0
    retryable_statuses: FrozenSet[int] = frozenset(
        {
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        }
    )
    default_retry_after: float = 30.0

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if self.project_max_requests < 1:
            raise ValueError("project_max_requests must be >= 1")
        if self.user_max_requests < 1:
            raise ValueError("user_max_requests must be >= 1")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.backoff_initial < 0:
            raise ValueError("backoff_initial must be >= 0")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be >= 1")
        if self.backoff_max < 0:
            raise ValueError("backoff_max must be >= 0")
        if self.default_retry_after < 0:
            raise ValueError("default_retry_after must be >= 0")

    def backoff_delay(self, retry_number: int) -> float:
        """Returns the backoff delay before retry ``retry_number`` (0-based)."""
        delay = self.backoff_initial * (self.backoff_multiplier**retry_number)
        return min(delay, self.backoff_max)


class ReadMethod(Enum):
    """Read-only API methods that can be scheduled.

    Only read methods exist in this enum: the scheduler never issues
    write requests.
    """

    VALUES_GET = "values_get"
    VALUES_BATCH_GET = "values_batch_get"


@dataclass
class ReadTask:
    """A single read request to schedule.

    :param str user: key of the authorized user, as registered in the
        :class:`BatchReadScheduler`.
    :param str spreadsheet_id: ID of the spreadsheet to read from.
    :param ranges: range(s) in `A1 notation`_. A single range (or a
        one-element list) for :attr:`ReadMethod.VALUES_GET`, a list of
        ranges for :attr:`ReadMethod.VALUES_BATCH_GET`.
    :param ReadMethod method: read method to use, defaults to
        :attr:`ReadMethod.VALUES_BATCH_GET`.
    :param dict params: (optional) extra query parameters of the API
        endpoint (e.g. ``valueRenderOption``).

    .. _A1 notation: https://developers.google.com/sheets/api/guides/concepts#a1_notation
    """

    user: str
    spreadsheet_id: str
    ranges: Union[str, List[str]]
    method: ReadMethod = ReadMethod.VALUES_BATCH_GET
    params: Optional[ParamsType] = None

    def __post_init__(self) -> None:
        if isinstance(self.ranges, str):
            self.ranges = [self.ranges]


@dataclass
class ReadResult:
    """Outcome of a scheduled :class:`ReadTask`.

    :class:`BatchReadScheduler` returns one result per submitted task,
    in submission order, so ``results[i]`` is the outcome of
    ``tasks[i]`` whatever order the requests actually completed in.

    :param int index: position of the task in the submitted batch.
    :param ReadTask task: the task this result belongs to.
    :param int attempts: number of real HTTP requests made for this
        task (first try + retries).
    :param value: parsed response body on success, ``None`` otherwise.
    :param error: the concrete failure reason (e.g. an
        :class:`~gspread.exceptions.APIError` carrying its ``code`` and
        ``response``), ``None`` on success.
    """

    index: int
    task: ReadTask
    attempts: int
    value: Optional[Any] = None
    error: Optional[Exception] = None

    @property
    def ok(self) -> bool:
        """``True`` when the task succeeded."""
        return self.error is None

    def unwrap(self) -> Any:
        """Returns the value, raising the stored error on failure."""
        if self.error is not None:
            raise self.error
        return self.value


@dataclass
class _TaskState:
    """Mutable scheduling state of one submitted task."""

    index: int
    task: ReadTask
    retries_left: int
    attempts: int = 0
    not_before: float = 0.0


def _parse_retry_after(
    value: Optional[str], now: Optional[float] = None
) -> Optional[float]:
    """Parses a ``Retry-After`` header value into a delay in seconds.

    Supports both forms defined by RFC 9110: delta-seconds (``"120"``)
    and HTTP-date (``"Sun, 06 Nov 1994 08:49:37 GMT"``).

    :param value: the header value, may be ``None``.
    :param float now: (optional) current wall clock time, for testing.
    :returns: the delay in seconds (never negative), or ``None`` when
        the value is missing or cannot be parsed.
    """
    if not value or not isinstance(value, str):
        return None

    value = value.strip()

    try:
        delay = float(value)
        if math.isfinite(delay):
            return max(0.0, delay)
        return None
    except ValueError:
        pass

    try:
        date = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)

    if now is None:
        now = time.time()

    return max(0.0, date.timestamp() - now)


def _prune_window(window: Deque[float], horizon: float) -> None:
    """Drops every timestamp of ``window`` older than ``horizon``."""
    while window and window[0] <= horizon:
        window.popleft()


class BatchReadScheduler:
    """Schedules batch read requests for several authorized users.

    The scheduler only issues **read** requests
    (:class:`ReadMethod`). It limits the number of concurrent in-flight
    requests and throttles them with two sliding windows: one for the
    whole project, one per user. Every real HTTP request — including
    retries — is recorded in both windows.

    When the API answers ``429 Too Many Requests`` the user responsible
    is cooled down for the duration given by the ``Retry-After`` header
    (delta-seconds or HTTP-date), falling back to
    :attr:`SchedulerConfig.default_retry_after`. Retryable ``5xx``
    errors are retried with an exponential backoff, at most
    :attr:`SchedulerConfig.max_retries` times. Any other error —
    including permission errors — is returned immediately.

    While a user waits for quota, cools down, or a task backs off, the
    eligible tasks of the other users keep being sent.

    The scheduler instance keeps its quota accounting across
    :meth:`schedule` calls. Concurrent :meth:`schedule` calls are
    serialized.

    :param clients: mapping of user key to the
        :class:`~gspread.http_client.HTTPClient` authenticated as that
        user, e.g. ``{"alice": gc_alice.http_client}``.
    :param SchedulerConfig config: (optional) scheduling configuration.

    Example::

        scheduler = BatchReadScheduler(
            clients={
                "alice": gc_alice.http_client,
                "bob": gc_bob.http_client,
            },
            config=SchedulerConfig(max_concurrency=4, user_max_requests=60),
        )

        tasks = [
            ReadTask(user="alice", spreadsheet_id=sid, ranges=["Sheet1!A1:D10"]),
            ReadTask(
                user="bob",
                spreadsheet_id=sid,
                ranges="Sheet2!A1:A5",
                method=ReadMethod.VALUES_GET,
            ),
        ]

        for result in scheduler.schedule(tasks):
            if result.ok:
                print(result.value)
            else:
                print("read failed:", result.error)
    """

    def __init__(
        self,
        clients: Mapping[str, HTTPClient],
        config: Optional[SchedulerConfig] = None,
    ) -> None:
        if not clients:
            raise ValueError("at least one user client is required")
        self._clients: Dict[str, HTTPClient] = dict(clients)
        self._config = config if config is not None else SchedulerConfig()
        self._project_window: Deque[float] = deque()
        self._user_windows: Dict[str, Deque[float]] = {
            user: deque() for user in self._clients
        }
        self._cooldown_until: Dict[str, float] = {}
        self._schedule_lock = Lock()

    def schedule(self, tasks: Iterable[ReadTask]) -> List[ReadResult]:
        """Runs every given read task and returns their results.

        Results are ordered like the submitted tasks: ``results[i]`` is
        the outcome of ``tasks[i]``. A failed task does not fail the
        batch: its :class:`ReadResult` carries the concrete failure
        reason in :attr:`ReadResult.error`.

        :param tasks: the read tasks to schedule.
        :returns: one :class:`ReadResult` per submitted task, in
            submission order.
        """
        with self._schedule_lock:
            return self._schedule(list(tasks))

    def _schedule(self, tasks: List[ReadTask]) -> List[ReadResult]:
        for task in tasks:
            self._validate_task(task)

        config = self._config
        results: List[Optional[ReadResult]] = [None] * len(tasks)
        pending: Deque[_TaskState] = deque(
            _TaskState(index=index, task=task, retries_left=config.max_retries)
            for index, task in enumerate(tasks)
        )
        inflight: Dict[Future, _TaskState] = {}

        with ThreadPoolExecutor(
            max_workers=config.max_concurrency,
            thread_name_prefix="gspread-batch-read",
        ) as pool:
            while pending or inflight:
                now = time.monotonic()
                self._prune_windows(now)

                # Dispatch every eligible task, up to the concurrency
                # limit. Ineligible tasks are skipped but stay queued: a
                # user waiting for quota or cooling down never blocks
                # the tasks of the other users.
                scanned = len(pending)
                while scanned > 0 and len(inflight) < config.max_concurrency:
                    scanned -= 1
                    state = pending.popleft()
                    if self._is_eligible(state, now):
                        self._record_attempt(state.task.user, now)
                        state.attempts += 1
                        future = pool.submit(self._execute, state.task)
                        inflight[future] = state
                    else:
                        pending.append(state)

                if inflight:
                    timeout = self._next_event_delay(now, pending) if pending else None
                    done, _ = wait(
                        inflight, timeout=timeout, return_when=FIRST_COMPLETED
                    )
                    for future in done:
                        state = inflight.pop(future)
                        self._handle_outcome(state, future, results, pending)
                elif pending:
                    # Nothing can run right now: sleep until the next
                    # event that may make a task eligible.
                    delay = self._next_event_delay(now, pending)
                    time.sleep(delay if delay and delay > 0 else _IDLE_POLL_SECONDS)

        for index, result in enumerate(results):
            if result is None:  # pragma: no cover - defensive, cannot happen
                raise RuntimeError("task {} finished without a result".format(index))
        return [result for result in results if result is not None]

    def _validate_task(self, task: ReadTask) -> None:
        if task.user not in self._clients:
            raise ValueError("unknown user: {!r}".format(task.user))
        if not isinstance(task.method, ReadMethod):
            raise ValueError(
                "only read methods can be scheduled, got: {!r}".format(task.method)
            )
        if not task.ranges:
            raise ValueError("a task needs at least one range")
        if task.method is ReadMethod.VALUES_GET and len(task.ranges) != 1:
            raise ValueError("values_get reads exactly one range")

    def _execute(self, task: ReadTask) -> Any:
        client = self._clients[task.user]
        ranges = task.ranges if isinstance(task.ranges, list) else [task.ranges]
        # copy the params: the client methods may mutate them
        params = dict(task.params) if task.params is not None else None
        if task.method is ReadMethod.VALUES_GET:
            return client.values_get(task.spreadsheet_id, ranges[0], params=params)
        return client.values_batch_get(task.spreadsheet_id, ranges, params=params)

    def _handle_outcome(
        self,
        state: _TaskState,
        future: Future,
        results: List[Optional[ReadResult]],
        pending: Deque[_TaskState],
    ) -> None:
        try:
            value = future.result()
        except APIError as err:
            self._handle_api_error(state, err, results, pending)
        except Exception as err:
            # Not an API error (network failure, ...): do not retry,
            # keep the concrete reason.
            results[state.index] = ReadResult(
                index=state.index, task=state.task, attempts=state.attempts, error=err
            )
        else:
            results[state.index] = ReadResult(
                index=state.index, task=state.task, attempts=state.attempts, value=value
            )

    def _handle_api_error(
        self,
        state: _TaskState,
        err: APIError,
        results: List[Optional[ReadResult]],
        pending: Deque[_TaskState],
    ) -> None:
        config = self._config
        response = err.response
        status = response.status_code if response is not None else err.code

        if status == HTTPStatus.TOO_MANY_REQUESTS and state.retries_left > 0:
            # Cool the user down: none of its tasks may be sent until the
            # cool-down elapses, then this task is retried.
            header = (
                response.headers.get("Retry-After") if response is not None else None
            )
            delay = _parse_retry_after(header)
            if delay is None:
                delay = config.default_retry_after
            cooldown_end = time.monotonic() + delay
            user = state.task.user
            if cooldown_end > self._cooldown_until.get(user, 0.0):
                self._cooldown_until[user] = cooldown_end
            state.retries_left -= 1
            state.not_before = self._cooldown_until[user]
            pending.append(state)
            return

        if status in config.retryable_statuses and state.retries_left > 0:
            state.retries_left -= 1
            retry_number = config.max_retries - state.retries_left - 1
            state.not_before = time.monotonic() + config.backoff_delay(retry_number)
            pending.append(state)
            return

        # Permission errors (401/403), other client errors, exhausted
        # retries: return the concrete reason immediately.
        results[state.index] = ReadResult(
            index=state.index, task=state.task, attempts=state.attempts, error=err
        )

    def _is_eligible(self, state: _TaskState, now: float) -> bool:
        config = self._config
        if state.not_before > now:
            return False
        if self._cooldown_until.get(state.task.user, 0.0) > now:
            return False
        if len(self._project_window) >= config.project_max_requests:
            return False
        if len(self._user_windows[state.task.user]) >= config.user_max_requests:
            return False
        return True

    def _record_attempt(self, user: str, now: float) -> None:
        """Accounts one real HTTP request in both sliding windows."""
        self._project_window.append(now)
        self._user_windows[user].append(now)

    def _prune_windows(self, now: float) -> None:
        horizon = now - self._config.window_seconds
        _prune_window(self._project_window, horizon)
        for window in self._user_windows.values():
            _prune_window(window, horizon)
        self._cooldown_until = {
            user: until for user, until in self._cooldown_until.items() if until > now
        }

    def _next_event_delay(
        self, now: float, pending: Deque[_TaskState]
    ) -> Optional[float]:
        """Seconds until the next event that may unblock a task."""
        config = self._config
        events: List[float] = []

        if len(self._project_window) >= config.project_max_requests:
            events.append(self._project_window[0] + config.window_seconds)

        for window in self._user_windows.values():
            if len(window) >= config.user_max_requests:
                events.append(window[0] + config.window_seconds)

        events.extend(self._cooldown_until.values())
        events.extend(state.not_before for state in pending)

        future_events = [event for event in events if event > now]
        if not future_events:
            return None
        return min(future_events) - now
