def foo(a: int, b: float) -> float:
    """
    Sample docstring.
    """
    return a * b


def failure(fail_arg: int) -> bool:
    """
    A test function to deterministically fail with even integers.
    """
    if fail_arg % 2 == 0:
        raise ValueError(
            "This function cannot possibly work with even numbers."
        )
    return True


# Test helper functions for CLI tests
_execution_tracker = []


def track_execution(x):
    """Track execution calls for testing."""
    _execution_tracker.append(x)
    return x


def get_execution_tracker():
    """Get the execution tracker list."""
    return _execution_tracker


def clear_execution_tracker():
    """Clear the execution tracker."""
    _execution_tracker.clear()


def failing_func(x):
    """A function that always fails with a specific error."""
    raise ValueError("Test error")
