"""
Example demonstrating JobPosting shared variables.

This example shows how to use shared variables to coordinate state
across multiple job postings. Since the current PDOFlow architecture
doesn't pass session/posting_id to job functions, this demonstrates
external coordination using the shared variables API.
"""


from pdoflow.io import Session
from pdoflow.models import JobPosting
from pdoflow.shared_vars import (
    get_shared_variable,
    list_shared_variables,
    set_shared_variable,
    update_shared_variable,
)


def demonstrate_shared_variables():
    """Demonstrate shared variable operations."""

    # Create a test job posting
    with Session() as session:
        posting = JobPosting(
            target_function="example_function",
            entry_point="examples.example_package:example_function",
        )
        session.add(posting)
        session.commit()
        posting_id = posting.id
        print(f"Created JobPosting: {posting_id}")

    # Demonstrate basic set/get operations
    with Session() as session:
        # Set initial configuration
        set_shared_variable(
            session, posting_id, "config", {"max_retries": 3, "timeout": 30}
        )

        # Set a counter
        set_shared_variable(session, posting_id, "counter", 0)

        session.commit()
        print("Set initial shared variables")

    # Demonstrate atomic increment
    print("\nDemonstrating atomic increments:")
    for i in range(5):
        with Session() as session:
            new_count = update_shared_variable(
                session, posting_id, "counter", lambda x: x + 1
            )
            session.commit()
            print(f"  Increment {i+1}: counter = {new_count}")

    # Demonstrate accumulator pattern
    print("\nDemonstrating accumulator pattern:")
    values = [10, 20, 30, 40, 50]
    for val in values:
        with Session() as session:
            new_total = update_shared_variable(
                session,
                posting_id,
                "total",
                lambda x: (x or 0) + val,
                default=0,
            )
            session.commit()
            print(f"  Added {val}: total = {new_total}")

    # Demonstrate list operations
    print("\nDemonstrating list operations:")
    with Session() as session:
        # Initialize empty list
        set_shared_variable(session, posting_id, "results", [])
        session.commit()

    # Append items atomically
    items = ["task_1", "task_2", "task_3"]
    for item in items:
        with Session() as session:

            def append_item(lst):
                lst.append(item)
                return lst

            new_list = update_shared_variable(
                session, posting_id, "results", append_item
            )
            session.commit()
            print(f"  Appended '{item}': {new_list}")

    # Read all variables
    print("\nFinal state of all shared variables:")
    with Session() as session:
        variables = list_shared_variables(session, posting_id)
        for key, value in sorted(variables.items()):
            print(f"  {key}: {value}")

    # Cleanup
    with Session() as session:
        posting = session.get(JobPosting, posting_id)
        session.delete(posting)
        session.commit()
        print("\nDeleted JobPosting and all associated shared variables")


def demonstrate_concurrent_access():
    """
    Demonstrate how shared variables handle concurrent access.

    In a real cluster environment, multiple workers would be accessing
    these variables simultaneously.
    """
    print("\n" + "=" * 60)
    print("Demonstrating concurrent access patterns")
    print("=" * 60)

    with Session() as session:
        posting = JobPosting(
            target_function="example_function",
            entry_point="examples.example_package:example_function",
        )
        session.add(posting)
        session.commit()
        posting_id = posting.id

    # Simulate worker coordination
    print("\nSimulating worker coordination:")

    # Worker 1: Claims a task
    with Session() as session:
        # Use lock=True to ensure exclusive access
        tasks = get_shared_variable(
            session,
            posting_id,
            "available_tasks",
            default=["task_a", "task_b", "task_c"],
            lock=True,
        )

        if tasks:
            claimed_task = tasks.pop(0)
            set_shared_variable(session, posting_id, "available_tasks", tasks)
            set_shared_variable(
                session, posting_id, "worker_1_task", claimed_task
            )
            session.commit()
            print(f"  Worker 1 claimed: {claimed_task}")

    # Worker 2: Claims next task
    with Session() as session:
        tasks = get_shared_variable(
            session, posting_id, "available_tasks", lock=True
        )

        if tasks:
            claimed_task = tasks.pop(0)
            set_shared_variable(session, posting_id, "available_tasks", tasks)
            set_shared_variable(
                session, posting_id, "worker_2_task", claimed_task
            )
            session.commit()
            print(f"  Worker 2 claimed: {claimed_task}")

    # Check remaining tasks
    with Session() as session:
        remaining = get_shared_variable(
            session, posting_id, "available_tasks", default=[]
        )
        print(f"  Remaining tasks: {remaining}")

    # Cleanup
    with Session() as session:
        posting = session.get(JobPosting, posting_id)
        session.delete(posting)
        session.commit()


if __name__ == "__main__":
    print("PDOFlow Shared Variables Example")
    print("================================\n")

    demonstrate_shared_variables()
    demonstrate_concurrent_access()

    print("\nExample complete!")
