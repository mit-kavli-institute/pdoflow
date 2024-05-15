from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cli, cluster, registry, status
from pdoflow.utils import load_function


@given(
    st.lists(
        st.tuples(
            st.integers(), st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1,
    ),
    st.just("tests.example_package.foo"),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_posting_status_cli(db_session, workload, path):
    registry.Registry.clear_registry()
    function = load_function(path)
    cluster.job()(function)

    posting_id, _ = registry.Registry[function].post_work(workload, [])

    runner = CliRunner()
    result = runner.invoke(cli.posting_status, (str(posting_id),))

    assert result.exit_code == 0
    assert str(posting_id) in result.output
    assert str(status.PostingStatus.executing) in result.output
