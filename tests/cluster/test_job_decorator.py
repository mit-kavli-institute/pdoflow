"""Tests for the job decorator."""

import pytest

from pdoflow import cluster, registry


class TestJobDecorator:
    """Test cases for the @job decorator."""

    def test_job_decorator_default_name(self, example_function):
        """Test that decorator uses function __name__ by default."""
        # Apply decorator
        decorated = cluster.job()(example_function)

        # Check function is preserved
        assert decorated is example_function

        # Check it was registered with function name
        assert "test_func" in registry.Registry
        assert registry.Registry["test_func"].target is example_function

    def test_job_decorator_custom_name(self, example_function):
        """Test that decorator accepts custom name."""
        custom_name = "my_custom_job"

        # Apply decorator with custom name
        decorated = cluster.job(name=custom_name)(example_function)

        # Check function is preserved
        assert decorated is example_function

        # Check it was registered with custom name
        assert custom_name in registry.Registry
        assert registry.Registry[custom_name].target is example_function

        # Original name should not be registered
        assert "test_func" not in registry.Registry

    def test_job_decorator_custom_registry(
        self, example_function, cluster_test_registry
    ):
        """Test that decorator can use a custom registry."""
        # Apply decorator with custom registry
        decorated = cluster.job(registry=cluster_test_registry)(
            example_function
        )

        # Check function is preserved
        assert decorated is example_function

        # Check it was registered in custom registry
        assert "test_func" in cluster_test_registry
        assert cluster_test_registry["test_func"].target is example_function

        # Should not be in global registry
        assert "test_func" not in registry.Registry

    def test_job_decorator_function_preservation(self):
        """Test that decorated function maintains its behavior."""

        @cluster.job()
        def add_numbers(a, b):
            """Add two numbers."""
            return a + b

        # Test function still works
        assert add_numbers(5, 3) == 8
        assert add_numbers(10, -5) == 5

        # Test function attributes preserved
        assert add_numbers.__name__ == "add_numbers"
        assert add_numbers.__doc__ == "Add two numbers."

    def test_job_decorator_with_various_functions(self, cluster_test_registry):
        """Test decorator with different function signatures."""

        @cluster.job(name="no_args", registry=cluster_test_registry)
        def func_no_args():
            return 42

        @cluster.job(name="args_only", registry=cluster_test_registry)
        def func_args_only(x, y, z):
            return x + y + z

        @cluster.job(name="kwargs_only", registry=cluster_test_registry)
        def func_kwargs_only(**kwargs):
            return kwargs

        @cluster.job(name="mixed_args", registry=cluster_test_registry)
        def func_mixed(a, b=10, *args, **kwargs):
            return (a, b, args, kwargs)

        # Verify all are registered
        assert cluster_test_registry["no_args"].target is func_no_args
        assert cluster_test_registry["args_only"].target is func_args_only
        assert cluster_test_registry["kwargs_only"].target is func_kwargs_only
        assert cluster_test_registry["mixed_args"].target is func_mixed

        # Verify they still work
        assert func_no_args() == 42
        assert func_args_only(1, 2, 3) == 6
        assert func_kwargs_only(a=1, b=2) == {"a": 1, "b": 2}
        assert func_mixed(5) == (5, 10, (), {})

    def test_job_decorator_registration_verification(self):
        """Test that decorator properly calls registry.add_job."""
        from unittest.mock import Mock

        mock_registry = Mock(spec=registry.JobRegistry)
        mock_registry.add_job = Mock()

        @cluster.job(name="test_job", registry=mock_registry)
        def test_function():
            pass

        # Verify add_job was called correctly
        mock_registry.add_job.assert_called_once_with(
            test_function, "test_job"
        )  # noqa: E501

    def test_job_decorator_multiple_decorations_same_function(self):
        """Test registering same function with different names."""

        def shared_function(x):
            return x * 2

        # Register with different names
        cluster.job(name="double")(shared_function)
        cluster.job(name="times_two")(shared_function)

        # Both names should work
        assert registry.Registry["double"].target is shared_function
        assert registry.Registry["times_two"].target is shared_function
        assert (
            registry.Registry["double"].target
            is registry.Registry["times_two"].target
        )

    def test_job_decorator_overwrite_existing_name(self):
        """Test that decorator raises error when trying to reuse name."""

        @cluster.job(name="reused_name")
        def first_function():
            return 1

        # Trying to register with the same name should raise ValueError
        with pytest.raises(ValueError) as exc_info:

            @cluster.job(name="reused_name")
            def second_function():
                return 2

        assert "already defined in registery" in str(exc_info.value)

    def test_job_decorator_none_name_uses_function_name(self):
        """Test that passing None as name uses function __name__."""

        @cluster.job(name=None)
        def my_function():
            return "test"

        assert "my_function" in registry.Registry
        assert registry.Registry["my_function"].target() == "test"
