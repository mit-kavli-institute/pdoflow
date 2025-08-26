============
Architecture
============

This document describes the internal architecture of PDOFlow.

System Overview
--------------

PDOFlow is built around PostgreSQL as the central coordination point:

.. code-block:: text

   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
   │   Client    │     │   Client    │     │   Client    │
   │  (Python)   │     │   (CLI)     │     │  (Python)   │
   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
          │                    │                    │
          │         Post Jobs  │                    │
          └────────────┬───────┴────────────────────┘
                       │
                       ▼
   ┌─────────────────────────────────────────────────┐
   │              PostgreSQL Database                 │
   │                                                 │
   │  ┌─────────────┐  ┌─────────────┐             │
   │  │ job_posting │  │ job_record  │             │
   │  └─────────────┘  └─────────────┘             │
   │                                                 │
   │  ┌─────────────┐  ┌─────────────┐             │
   │  │ job_profile │  │  function   │             │
   │  └─────────────┘  └─────────────┘             │
   └────────────────────┬────────────────────────────┘
                        │
                        │ Pull Jobs (SKIP LOCKED)
                        │
          ┌─────────────┴──────────────┐
          │                            │
          ▼                            ▼
   ┌─────────────┐             ┌─────────────┐
   │   Worker    │             │   Worker    │
   │  Process 1  │             │  Process N  │
   └─────────────┘             └─────────────┘

Core Components
--------------

Models Layer
~~~~~~~~~~~

The models layer defines the database schema using SQLAlchemy ORM:

**Base Classes**:

- ``Base``: SQLAlchemy declarative base with UUID primary keys
- ``CreatedOnMixin``: Adds created_on timestamp to models

**Job Models**:

- ``JobPosting``: Groups related jobs with metadata
- ``JobRecord``: Individual job with arguments and status

**Profiling Models**:

- ``JobProfile``: Profiling summary for executed jobs
- ``Function``: Unique functions in profiles
- ``FunctionStat``: Per-function statistics
- ``FunctionCallMap``: Caller-callee relationships

Registry System
~~~~~~~~~~~~~~

The Registry provides a global singleton for job management:

.. code-block:: python

   # Internal structure
   class _Registry:
       def __init__(self):
           self._jobs: Dict[str, _RegisteredJob] = {}
           self._lock = threading.Lock()

       def register(self, name: str, func: Callable):
           with self._lock:
               self._jobs[name] = _RegisteredJob(name, func)

The ``@job`` decorator automatically registers functions:

.. code-block:: python

   @job(name="my_task")
   def my_task(x: int):
       # Automatically added to Registry
       pass

Worker Architecture
~~~~~~~~~~~~~~~~~

**ClusterProcess**:

Each worker process:

1. Connects to database independently
2. Pulls jobs using ``SKIP LOCKED``
3. Executes jobs in transaction
4. Tracks failures per posting
5. Profiles 10% of executions

**ClusterPool**:

Manages worker lifecycle:

- Spawns initial workers
- Monitors process health
- Replaces dead workers
- Handles graceful shutdown

Database Design
--------------

Schema
~~~~~~

Key tables and relationships:

.. code-block:: sql

   -- Job posting (batch of related jobs)
   CREATE TABLE job_posting (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       poster VARCHAR NOT NULL,
       status VARCHAR NOT NULL,
       target_function VARCHAR NOT NULL,
       entry_point VARCHAR NOT NULL,
       created_on TIMESTAMP NOT NULL DEFAULT NOW()
   );

   -- Individual jobs
   CREATE TABLE job_record (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       posting_id UUID REFERENCES job_posting(id),
       priority INTEGER DEFAULT 0,
       positional_arguments JSONB NOT NULL,
       keyword_arguments JSONB NOT NULL,
       tries_remaining INTEGER DEFAULT 3,
       status VARCHAR NOT NULL,
       created_on TIMESTAMP NOT NULL DEFAULT NOW(),
       updated_on TIMESTAMP
   );

   -- Indexes for performance
   CREATE INDEX idx_job_waiting_priority
   ON job_record(priority DESC, created_on ASC)
   WHERE status = 'waiting';

   CREATE INDEX idx_job_posting_status
   ON job_record(posting_id, status);

Locking Strategy
~~~~~~~~~~~~~~~

PDOFlow uses PostgreSQL's ``SKIP LOCKED`` to prevent contention:

.. code-block:: sql

   BEGIN;

   SELECT * FROM job_record
   WHERE status = 'waiting'
     AND posting_id IN (
       SELECT id FROM job_posting
       WHERE status = 'executing'
     )
   ORDER BY priority DESC, created_on ASC
   LIMIT 10
   FOR UPDATE SKIP LOCKED;

   -- Update selected jobs
   UPDATE job_record
   SET status = 'executing'
   WHERE id = ANY(selected_ids);

   COMMIT;

This ensures:

- No blocking between workers
- Jobs processed in priority order
- Automatic failure recovery

Job Execution Flow
-----------------

1. **Job Submission**:

   .. code-block:: python

      Registry["my_job"].post_work([(1,), (2,)])
      # → Creates JobPosting
      # → Creates JobRecords
      # → Sets status='waiting'

2. **Worker Selection**:

   .. code-block:: python

      # In ClusterProcess.run()
      jobs = fetch_jobs(batch_size=10)
      # → Query with SKIP LOCKED
      # → Update status='executing'

3. **Execution**:

   .. code-block:: python

      for job in jobs:
          if random() < 0.1:  # 10% profiling
              stats = traced_execution(job)
              reflect_cProfile(db, job_profile, stats)
          else:
              nominal_execution(job)
      # → Update status='done' or 'errored_out'

4. **Error Handling**:

   .. code-block:: python

      try:
          job.execute()
      except Exception:
          job.tries_remaining -= 1
          if job.tries_remaining > 0:
              job.status = JobStatus.waiting
          else:
              job.status = JobStatus.errored_out

Profiling System
---------------

Profile Collection
~~~~~~~~~~~~~~~~~

10% of jobs are profiled using cProfile:

.. code-block:: python

   def traced_execution(self, job: JobRecord):
       pr = cProfile.Profile()
       pr.enable()
       job.execute()
       pr.disable()
       pr.create_stats()
       return pr.stats

Profile Storage
~~~~~~~~~~~~~~

Profile data is normalized and stored:

1. ``JobProfile``: Summary statistics
2. ``Function``: Unique function entries
3. ``FunctionStat``: Per-function metrics
4. ``FunctionCallMap``: Call relationships

This allows SQL queries for performance analysis.

Extension Points
---------------

Custom Workers
~~~~~~~~~~~~~

Extend ``ClusterProcess`` for custom behavior:

.. code-block:: python

   class CustomWorker(ClusterProcess):
       def _pre_run_init(self):
           super()._pre_run_init()
           # Custom initialization

       def process_job(self, job: JobRecord):
           # Custom processing logic
           super().process_job(job)

Custom Job Types
~~~~~~~~~~~~~~~

Implement specialized job handling:

.. code-block:: python

   @job(name="streaming_job")
   def streaming_job(stream_id: str):
       # Can post follow-up jobs
       for chunk in get_stream_chunks(stream_id):
           Registry["process_chunk"].post_work([(chunk,)])

Monitoring Hooks
~~~~~~~~~~~~~~~

Add monitoring at key points:

.. code-block:: python

   class MonitoredPool(ClusterPool):
       def upkeep(self):
           super().upkeep()
           # Send metrics
           send_metric("workers.active", len(self.workers))
           send_metric("workers.failed", self.failure_count)

Performance Considerations
------------------------

Query Optimization
~~~~~~~~~~~~~~~~~

- Indexes on ``(priority DESC, created_on ASC)``
- Partial indexes for ``status='waiting'``
- JSONB indexes for argument queries

Connection Pooling
~~~~~~~~~~~~~~~~~

Each worker maintains one connection:

- No connection pool overhead
- Predictable resource usage
- Easy to scale horizontally

Batch Processing
~~~~~~~~~~~~~~~

Workers fetch jobs in batches:

- Reduces round trips
- Amortizes transaction overhead
- Configurable batch size

Memory Management
~~~~~~~~~~~~~~~

- Jobs processed one at a time
- Profile data flushed after each job
- No unbounded growth

Security Model
-------------

Process Isolation
~~~~~~~~~~~~~~~

- Workers run as separate processes
- No shared memory between workers
- Failures isolated to single process

Database Security
~~~~~~~~~~~~~~~

- Workers need minimal privileges
- Read/write to job tables only
- No DDL permissions required

Input Validation
~~~~~~~~~~~~~~

- JSON serialization enforces types
- No code execution from database
- Function paths validated on load

Future Enhancements
------------------

Planned improvements:

1. **Distributed Workers**: Support for remote workers
2. **Job Dependencies**: DAG-based job scheduling
3. **Result Storage**: Optional result persistence
4. **Monitoring API**: Real-time metrics endpoint
5. **Admin UI**: Web interface for management

See Also
--------

- :doc:`contributing` - How to contribute
- :doc:`testing` - Testing architecture
- :doc:`../api/index` - API documentation
