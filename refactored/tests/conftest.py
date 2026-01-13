"""
Test configuration and fixtures for CDC Audit System tests.

This module provides:
- Test database setup and teardown
- Kafka test cluster management
- Test fixtures for common objects
- Test configuration overrides
- Async test utilities
"""

import asyncio
import os
import tempfile
import pytest
import pytest_asyncio
from typing import Dict, Any, AsyncGenerator, Generator
from pathlib import Path

# Test imports
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool

# Application imports
from src.config import AppConfig, DatabaseConfig, KafkaConfig, Environment
from src.database import Base, get_database_manager
from src.monitoring import MonitoringService


# Test configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_config() -> AppConfig:
    """Create test configuration."""
    # Override configuration for testing
    os.environ.update({
        "ENVIRONMENT": "testing",
        "DEBUG": "true",
        "LOG_LEVEL": "WARNING",  # Reduce log noise during tests
        "LOG_STRUCTURED": "false",
        "MONITORING_ENABLED": "false",  # Disable monitoring in tests
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",  # Test Kafka port
        "SINK_DB_HOST": "localhost",
        "SINK_DB_PORT": "5433",  # Test database port
        "SINK_DB_USERNAME": "test_admin",
        "SINK_DB_PASSWORD": "test_admin",
        "SINK_DB_NAME": "test_sink_db",
    })

    config = AppConfig.from_env()
    return config


@pytest.fixture(scope="session")
async def test_database_engine(test_config):
    """Create test database engine with in-memory SQLite."""
    # Use SQLite for fast testing instead of PostgreSQL
    database_url = "sqlite+aiosqlite:///:memory:"

    engine = create_async_engine(
        database_url,
        poolclass=StaticPool,
        echo=False,
        future=True,
    )

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()


@pytest.fixture
async def test_db_session(test_database_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session."""
    session_factory = async_sessionmaker(
        test_database_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_factory() as session:
        yield session
        await session.rollback()  # Rollback changes after each test


@pytest.fixture
async def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_debezium_message() -> Dict[str, Any]:
    """Sample Debezium CDC message for testing."""
    return {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "struct",
                    "fields": [
                        {"type": "int32", "optional": False, "field": "id"},
                        {"type": "string", "optional": False, "field": "name"},
                        {"type": "string", "optional": True, "field": "email"}
                    ],
                    "optional": True,
                    "name": "source_db.public.people.Value",
                    "field": "before"
                },
                {
                    "type": "struct",
                    "fields": [
                        {"type": "int32", "optional": False, "field": "id"},
                        {"type": "string", "optional": False, "field": "name"},
                        {"type": "string", "optional": True, "field": "email"}
                    ],
                    "optional": True,
                    "name": "source_db.public.people.Value",
                    "field": "after"
                },
                {
                    "type": "struct",
                    "fields": [
                        {"type": "string", "optional": False, "field": "version"},
                        {"type": "string", "optional": False, "field": "connector"},
                        {"type": "string", "optional": False, "field": "name"},
                        {"type": "int64", "optional": False, "field": "ts_ms"},
                        {"type": "string", "optional": True, "field": "snapshot"},
                        {"type": "string", "optional": False, "field": "db"},
                        {"type": "string", "optional": True, "field": "sequence"},
                        {"type": "string", "optional": False, "field": "schema"},
                        {"type": "string", "optional": False, "field": "table"},
                        {"type": "int64", "optional": True, "field": "txId"},
                        {"type": "int64", "optional": True, "field": "lsn"},
                        {"type": "boolean", "optional": True, "field": "xmin"}
                    ],
                    "optional": False,
                    "name": "io.debezium.connector.postgresql.Source",
                    "field": "source"
                },
                {"type": "string", "optional": False, "field": "op"},
                {"type": "int64", "optional": True, "field": "ts_ms"},
                {"type": "struct", "optional": True, "field": "transaction"}
            ],
            "optional": False,
            "name": "source_db.public.people.Envelope"
        },
        "payload": {
            "before": None,
            "after": {
                "id": 1,
                "name": "John Doe",
                "email": "john.doe@example.com"
            },
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": "source_db",
                "ts_ms": 1703123456789,
                "snapshot": "false",
                "db": "source_db",
                "sequence": "[\"123456789\"]",
                "schema": "public",
                "table": "people",
                "txId": 12345,
                "lsn": "0/12345678",
                "xmin": None
            },
            "op": "c",
            "ts_ms": 1703123456789,
            "transaction": None
        }
    }


@pytest.fixture
def sample_update_message() -> Dict[str, Any]:
    """Sample Debezium UPDATE message."""
    message = {
        "schema": {},  # Simplified for test
        "payload": {
            "before": {
                "id": 1,
                "name": "John Doe",
                "email": "john.doe@example.com"
            },
            "after": {
                "id": 1,
                "name": "John Smith",
                "email": "john.smith@example.com"
            },
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": "source_db",
                "ts_ms": 1703123456789,
                "snapshot": "false",
                "db": "source_db",
                "schema": "public",
                "table": "people",
                "txId": 12346,
                "lsn": "0/12345679"
            },
            "op": "u",
            "ts_ms": 1703123456789
        }
    }
    return message


@pytest.fixture
def sample_delete_message() -> Dict[str, Any]:
    """Sample Debezium DELETE message."""
    message = {
        "schema": {},  # Simplified for test
        "payload": {
            "before": {
                "id": 1,
                "name": "John Smith",
                "email": "john.smith@example.com"
            },
            "after": None,
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": "source_db",
                "ts_ms": 1703123456789,
                "snapshot": "false",
                "db": "source_db",
                "schema": "public",
                "table": "people",
                "txId": 12347,
                "lsn": "0/1234567A"
            },
            "op": "d",
            "ts_ms": 1703123456789
        }
    }
    return message


@pytest.fixture
async def mock_kafka_message(sample_debezium_message):
    """Create a mock Kafka message for testing."""
    class MockMessage:
        def __init__(self, value, topic="source_db.public.people", partition=0, offset=0, key=None):
            self._value = value
            self._topic = topic
            self._partition = partition
            self._offset = offset
            self._key = key

        def value(self):
            return self._value

        def topic(self):
            return self._topic

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

        def key(self):
            return self._key

        def error(self):
            return None

    return MockMessage


@pytest.fixture
async def mock_monitoring_service(test_config):
    """Create mock monitoring service for testing."""
    # Create a mock database service
    class MockDatabaseService:
        async def get_system_health(self):
            return {"status": "healthy", "database": "mock"}

    monitoring = MonitoringService(
        test_config.monitoring,
        MockDatabaseService()
    )

    # Don't start the actual service
    return monitoring


# Test utilities
class AsyncTestCase:
    """Base class for async test cases."""

    def run_async(self, coro):
        """Run async coroutine in test."""
        return asyncio.run(coro)


def assert_dict_contains(actual: Dict[str, Any], expected: Dict[str, Any]):
    """Assert that actual dict contains all expected key-value pairs."""
    for key, value in expected.items():
        assert key in actual, f"Key '{key}' not found in dict"
        assert actual[key] == value, f"Value for key '{key}' mismatch: expected {value}, got {actual[key]}"


def wait_for_condition(condition_func, timeout: float = 5.0, interval: float = 0.1):
    """Wait for a condition to become true."""
    import time

    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        time.sleep(interval)

    raise AssertionError(f"Condition not met within {timeout} seconds")


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


# Test data factories
def create_test_audit_log(**overrides):
    """Factory for creating test audit log data."""
    from datetime import datetime

    defaults = {
        "source_table": "people",
        "operation_type": "INSERT",
        "change_timestamp": datetime.utcnow(),
        "old_data": None,
        "new_data": {"id": 1, "name": "Test User"},
        "change_user": None,
        "transaction_id": "test-tx-123",
        "lsn": "0/12345678",
        "source_metadata": {"connector": "postgresql"}
    }

    defaults.update(overrides)
    return defaults


def create_test_offset_record(**overrides):
    """Factory for creating test Kafka offset data."""
    defaults = {
        "topic": "source_db.public.people",
        "partition": 0,
        "offset": 100,
        "consumer_group": "test-group",
        "processing_status": "completed"
    }

    defaults.update(overrides)
    return defaults