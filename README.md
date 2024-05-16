# PDOFlow
A Python project which manages job queues using PostgreSQL as the source of truth.

# Usage
There are two entrypoints required to run ``PDOFlow``. The first
requires decorating a function which work will be assigned to various
computing clusters. The other is actually running and maintaining an
active worker pool for each machine in the cluster.

## Decorating Functions
The use case is very simple. Decorate a function and push work to that
function.

```python
from pdoflow import cluster


# File mymodule/*/foo.py

@cluster.job()
def some_unit_of_work(a: int, b: float, c: str) -> str:
    result = a * b
    with open(c, "wt") as fout:
        fout.write(f"{result:.06f}")

# File mymodule/*/workload.py | doesn't necessarily need to be in another
# file

from pdoflow.registry import Registry
from mymodule.foo import some_unit_of_work


Registry[some_unit_of_work].post_work(
    (
        [1, 2.0, "just_twos.txt"],
        [10, -100, "big_numbers.txt"],
    ),
    []  # Work is being done through positional arguments, no keyword
        # parameters are being used.
)
# Done from the client's point of view.
```

## Running clusters
You may instantiate clusters using the provided CLI.

```bash
pdoflow pool --max-workers 16
```

This will create a pool of a maximum of 16 concurrent python processes
which will execute posted workloads.

Or you may run pool in a Python program:
```python

from pdoflow import cluster
from time import sleep


worker_pool = cluster.ClusterPool(max_workers=16)

with worker_pool:
    while True:
        worker_pool.upkeep()
        sleep(1.0)
```

# Drawbacks
Work is posted to a PostgreSQL database utilizing JSON serializable
fields for work parameters. The biggest draw back currently is the
inability to pass ``NaN`` and ``inf`` values.

While functions are executed, any return values are dropped and ignored.
So these functions should be impure and have some non-volatile
side-effect.

# Security Implications
This package runs arbitrary code passed from clients. Some mitigations
have been made. For example, all posted parameters must be JSON
serializable and the code must be importable in some fashion.

However, anyone can point to a file on disk. So the user owning the
cluster worker pool should be given the smallest amount of privileges.
And the users authorized to SELECT, UPDATE, and INSERT into the
job queue should be limited.
