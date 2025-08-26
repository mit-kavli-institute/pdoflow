"""Tests for the _FailureCache class."""

import uuid

from pdoflow.cluster import _FailureCache


class TestFailureCache:
    """Test cases for _FailureCache functionality."""

    def test_init_default_value(self):
        """Test that initialization stores the default value correctly."""
        default_value = 42
        cache = _FailureCache(default_value)
        assert cache._default_value == default_value
        assert cache._cache == {}

    def test_getitem_new_key_returns_default(self):
        """Test that accessing a new key returns and stores default value."""
        default_value = 10
        cache = _FailureCache(default_value)
        test_uuid = uuid.uuid4()

        # First access should return default value
        result = cache[test_uuid]
        assert result == default_value

        # Verify it was stored in the cache
        assert test_uuid in cache._cache
        assert cache._cache[test_uuid] == default_value

    def test_getitem_existing_key(self):
        """Test that accessing an existing key returns the cached value."""
        cache = _FailureCache(10)
        test_uuid = uuid.uuid4()

        # Set a custom value
        cache[test_uuid] = 5

        # Getting should return the custom value, not default
        result = cache[test_uuid]
        assert result == 5

    def test_setitem_updates_cache(self):
        """Test that setitem properly updates the cache."""
        cache = _FailureCache(10)
        test_uuid = uuid.uuid4()

        # Set a value
        cache[test_uuid] = 7
        assert cache._cache[test_uuid] == 7

        # Update the value
        cache[test_uuid] = 3
        assert cache._cache[test_uuid] == 3

    def test_multiple_posting_tracking(self):
        """Test tracking failures for multiple postings."""
        cache = _FailureCache(5)
        posting1 = uuid.uuid4()
        posting2 = uuid.uuid4()
        posting3 = uuid.uuid4()

        # Access different postings
        assert cache[posting1] == 5
        assert cache[posting2] == 5
        assert cache[posting3] == 5

        # Modify each independently
        cache[posting1] = 4
        cache[posting2] = 2
        cache[posting3] = 0

        # Verify each maintains its own value
        assert cache[posting1] == 4
        assert cache[posting2] == 2
        assert cache[posting3] == 0

    def test_cache_persistence_across_operations(self):
        """Test that cache values persist across multiple operations."""
        cache = _FailureCache(10)
        test_uuid = uuid.uuid4()

        # Initial access
        initial = cache[test_uuid]
        assert initial == 10

        # Decrement in a loop (simulating failure tracking)
        for expected in range(9, -1, -1):
            cache[test_uuid] = expected
            assert cache[test_uuid] == expected

        # Verify final state
        assert cache[test_uuid] == 0

    def test_cache_with_zero_default(self):
        """Test cache behavior with zero as default value."""
        cache = _FailureCache(0)
        test_uuid = uuid.uuid4()

        assert cache[test_uuid] == 0
        cache[test_uuid] = -1
        assert cache[test_uuid] == -1

    def test_cache_uuid_type_safety(self):
        """Test that cache works correctly with UUID objects."""
        cache = _FailureCache(100)

        # Create UUIDs in different ways
        uuid1 = uuid.uuid4()
        uuid2 = uuid.UUID("12345678-1234-5678-1234-567812345678")
        uuid3 = uuid.uuid4()

        # Each should maintain separate values
        cache[uuid1] = 90
        cache[uuid2] = 80
        assert cache[uuid3] == 100  # Should get default

        assert cache[uuid1] == 90
        assert cache[uuid2] == 80
        assert cache[uuid3] == 100
