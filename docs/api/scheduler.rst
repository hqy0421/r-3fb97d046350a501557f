Batch Read Scheduler
====================

.. note::

   The scheduler only issues **read** requests. It throttles them with
   a concurrency limit and two sliding window quotas (one for the whole
   project, one per user), cools a user down on ``429`` responses and
   retries retryable ``5xx`` errors with a bounded exponential backoff.

.. autoclass:: gspread.BatchReadScheduler
   :members:

.. autoclass:: gspread.SchedulerConfig
   :members:

.. autoclass:: gspread.ReadTask
   :members:

.. autoclass:: gspread.ReadResult
   :members:

.. autoclass:: gspread.ReadMethod
   :members:
