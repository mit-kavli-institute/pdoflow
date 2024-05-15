from hypothesis import strategies as st


def foo_workload():
    return st.lists(
        st.tuples(
            st.integers(), st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1,
    )
