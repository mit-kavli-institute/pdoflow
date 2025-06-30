#!/usr/bin/env python
"""
Example demonstrating PDOFlow's priority queue feature.

This script shows how to:
1. Submit jobs with different priorities
2. View priority statistics
3. Observe how jobs are executed in priority order
"""

import time

from pdoflow import cluster
from pdoflow.registry import Registry


@cluster.job()
def process_task(task_id: int, task_name: str, duration: float) -> None:
    """Simple task that logs its execution and sleeps."""
    start_time = time.strftime("%H:%M:%S")
    print(f"[{start_time}] Starting {task_name} (ID: {task_id})")
    time.sleep(duration)
    end_time = time.strftime("%H:%M:%S")
    print(f"[{end_time}] Completed {task_name} (ID: {task_id})")


def main():
    # Clear any existing registrations
    Registry.clear_registry()

    # Register our task function
    cluster.job()(process_task)

    print("Submitting jobs with different priorities...")
    print("-" * 50)

    # Submit urgent tasks (high priority)
    urgent_tasks = [
        (101, "URGENT: Server restart", 1.0),
        (102, "URGENT: Database backup", 1.5),
    ]
    posting1_id, _ = Registry[process_task].post_work(
        posargs=urgent_tasks, kwargs=[], priority=100  # High priority
    )
    print(f"Submitted urgent tasks (priority=100): {posting1_id}")

    # Submit normal tasks (default priority)
    normal_tasks = [
        (201, "Normal: Generate report", 2.0),
        (202, "Normal: Send emails", 1.0),
        (203, "Normal: Update cache", 1.5),
    ]
    posting2_id, _ = Registry[process_task].post_work(
        posargs=normal_tasks, kwargs=[], priority=0  # Default priority
    )
    print(f"Submitted normal tasks (priority=0): {posting2_id}")

    # Submit background tasks (low priority)
    background_tasks = [
        (301, "Background: Clean logs", 0.5),
        (302, "Background: Optimize images", 3.0),
    ]
    posting3_id, _ = Registry[process_task].post_work(
        posargs=background_tasks, kwargs=[], priority=-50  # Low priority
    )
    print(f"Submitted background tasks (priority=-50): {posting3_id}")

    # Submit tasks with varying priorities
    varied_tasks = [
        (401, "Critical fix", 0.5),
        (402, "Important update", 1.0),
        (403, "Minor adjustment", 0.5),
    ]
    priorities = [150, 50, -25]  # Different priority for each task
    posting4_id, _ = Registry[process_task].post_work(
        posargs=varied_tasks, kwargs=[], priority=priorities
    )
    print(f"Submitted varied priority tasks: {posting4_id}")

    print("\nAll jobs submitted!")
    print("Workers will process jobs in this order:")
    print("1. Critical fix (priority=150)")
    print("2. URGENT tasks (priority=100)")
    print("3. Important update (priority=50)")
    print("4. Normal tasks (priority=0)")
    print("5. Minor adjustment (priority=-25)")
    print("6. Background tasks (priority=-50)")
    print("\nWithin same priority level, jobs are processed FIFO.")
    print("\nRun 'pdoflow priority-stats' to see the queue status.")
    print("Run 'pdoflow pool --max-workers 2' to start processing.")


if __name__ == "__main__":
    main()
