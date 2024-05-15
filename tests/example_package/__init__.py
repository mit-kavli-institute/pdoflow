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
