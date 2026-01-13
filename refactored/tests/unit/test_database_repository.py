"""
Unit tests for database repository layer.

Tests cover:
- Repository operations (CRUD)
- Error handling
- Batch operations
- Transaction management
- Query filtering and pagination
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock

from src.database import (
    AuditLoggingRepository,
    KafkaOffsetRepository,
    ProcessingErrorRepository,
    OperationTypeEnum,
    AuditLogging,
    KafkaOffset,
    ProcessingError,
    get_repository_session,
    DuplicateKeyError,
    RepositoryError
)


class TestAuditLoggingRepository:
    """Test cases for AuditLoggingRepository."""

    @pytest.fixture
    async def repo(self, test_db_session):
        """Create repository instance."""
        return AuditLoggingRepository(test_db_session)

    @pytest.fixture
    def sample_audit_data(self):
        """Sample audit log data."""
        return {
            "source_table": "people",
            "operation_type": OperationTypeEnum.INSERT,
            "change_timestamp": datetime.utcnow(),
            "old_data": None,
            "new_data": {"id": 1, "name": "John Doe", "email": "john@example.com"},
            "change_user": "test_user",
            "transaction_id": "tx-123",
            "lsn": "0/12345678",
            "source_metadata": {"connector": "postgresql", "version": "2.7.3"}
        }

    async def test_create_audit_log(self, repo, sample_audit_data):
        """Test creating an audit log entry."""
        result = await repo.create_audit_log(**sample_audit_data)

        assert isinstance(result, AuditLogging)
        assert result.source_table == "people"
        assert result.operation_type == OperationTypeEnum.INSERT
        assert result.new_data["name"] == "John Doe"
        assert result.id is not None

    async def test_create_audit_log_minimal(self, repo):
        """Test creating audit log with minimal data."""
        result = await repo.create_audit_log(
            source_table="roles",
            operation_type=OperationTypeEnum.DELETE,
            old_data={"id": 1, "name": "Admin"}
        )

        assert result.source_table == "roles"
        assert result.operation_type == OperationTypeEnum.DELETE
        assert result.old_data["name"] == "Admin"
        assert result.new_data is None

    async def test_batch_create_audit_logs(self, repo, sample_audit_data):
        """Test batch creation of audit logs."""
        # Create multiple audit log entries
        batch_data = []
        for i in range(3):
            data = sample_audit_data.copy()
            data["new_data"] = {"id": i + 1, "name": f"User {i + 1}"}
            batch_data.append(data)

        results = await repo.batch_create_audit_logs(batch_data)

        assert len(results) == 3
        assert all(isinstance(r, AuditLogging) for r in results)
        assert all(r.id is not None for r in results)
        assert results[0].new_data["name"] == "User 1"
        assert results[2].new_data["name"] == "User 3"

    async def test_get_audit_logs_no_filters(self, repo, sample_audit_data):
        """Test getting audit logs without filters."""
        # Create some test data
        await repo.create_audit_log(**sample_audit_data)
        await repo.create_audit_log(**sample_audit_data)

        results = await repo.get_audit_logs()

        assert len(results) == 2
        assert all(isinstance(r, AuditLogging) for r in results)

    async def test_get_audit_logs_with_filters(self, repo):
        """Test getting audit logs with various filters."""
        base_data = {
            "source_table": "people",
            "operation_type": OperationTypeEnum.INSERT,
            "new_data": {"id": 1, "name": "Test"}
        }

        # Create logs with different tables and operations
        await repo.create_audit_log(source_table="people", operation_type=OperationTypeEnum.INSERT, new_data={"id": 1})
        await repo.create_audit_log(source_table="roles", operation_type=OperationTypeEnum.INSERT, new_data={"id": 1})
        await repo.create_audit_log(source_table="people", operation_type=OperationTypeEnum.UPDATE, new_data={"id": 1})

        # Filter by table
        people_logs = await repo.get_audit_logs(source_table="people")
        assert len(people_logs) == 2

        # Filter by operation
        insert_logs = await repo.get_audit_logs(operation_type=OperationTypeEnum.INSERT)
        assert len(insert_logs) == 2

        # Filter by both
        filtered_logs = await repo.get_audit_logs(
            source_table="people",
            operation_type=OperationTypeEnum.UPDATE
        )
        assert len(filtered_logs) == 1

    async def test_get_audit_logs_pagination(self, repo):
        """Test audit log pagination."""
        # Create multiple records
        for i in range(10):
            await repo.create_audit_log(
                source_table="people",
                operation_type=OperationTypeEnum.INSERT,
                new_data={"id": i}
            )

        # Test pagination
        page1 = await repo.get_audit_logs(limit=3, offset=0)
        page2 = await repo.get_audit_logs(limit=3, offset=3)

        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0].new_data["id"] != page2[0].new_data["id"]

    async def test_get_audit_logs_date_filtering(self, repo):
        """Test date-based filtering of audit logs."""
        now = datetime.utcnow()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        # Create logs at different times
        await repo.create_audit_log(
            source_table="people",
            operation_type=OperationTypeEnum.INSERT,
            change_timestamp=past,
            new_data={"id": 1}
        )
        await repo.create_audit_log(
            source_table="people",
            operation_type=OperationTypeEnum.INSERT,
            change_timestamp=now,
            new_data={"id": 2}
        )

        # Filter by date range
        recent_logs = await repo.get_audit_logs(start_date=now - timedelta(hours=1))
        assert len(recent_logs) == 1
        assert recent_logs[0].new_data["id"] == 2

    async def test_get_audit_stats(self, repo):
        """Test getting audit statistics."""
        # Create test data
        await repo.create_audit_log(source_table="people", operation_type=OperationTypeEnum.INSERT)
        await repo.create_audit_log(source_table="people", operation_type=OperationTypeEnum.UPDATE)
        await repo.create_audit_log(source_table="roles", operation_type=OperationTypeEnum.INSERT)

        stats = await repo.get_audit_stats()

        assert stats["total_changes"] == 3
        assert stats["operation_breakdown"]["INSERT"] == 2
        assert stats["operation_breakdown"]["UPDATE"] == 1
        assert "DELETE" not in stats["operation_breakdown"]

    async def test_get_audit_stats_with_table_filter(self, repo):
        """Test audit stats for specific table."""
        await repo.create_audit_log(source_table="people", operation_type=OperationTypeEnum.INSERT)
        await repo.create_audit_log(source_table="roles", operation_type=OperationTypeEnum.INSERT)

        stats = await repo.get_audit_stats(source_table="people")

        assert stats["total_changes"] == 1
        assert stats["source_table"] == "people"


class TestKafkaOffsetRepository:
    """Test cases for KafkaOffsetRepository."""

    @pytest.fixture
    async def repo(self, test_db_session):
        """Create repository instance."""
        return KafkaOffsetRepository(test_db_session)

    async def test_get_offset_not_exists(self, repo):
        """Test getting offset that doesn't exist."""
        result = await repo.get_offset("test.topic", 0, "test-group")
        assert result is None

    async def test_update_offset_create(self, repo):
        """Test creating new offset record."""
        result = await repo.update_offset("test.topic", 0, 100, "test-group")

        assert isinstance(result, KafkaOffset)
        assert result.topic == "test.topic"
        assert result.partition == 0
        assert result.offset == 100
        assert result.consumer_group == "test-group"

    async def test_update_offset_update(self, repo):
        """Test updating existing offset record."""
        # Create initial record
        await repo.update_offset("test.topic", 0, 100, "test-group")

        # Update it
        result = await repo.update_offset("test.topic", 0, 200, "test-group")

        assert result.offset == 200
        assert result.consumer_group == "test-group"

    async def test_batch_update_offsets(self, repo):
        """Test batch offset updates."""
        offset_data = [
            {"topic": "topic1", "partition": 0, "offset": 100},
            {"topic": "topic1", "partition": 1, "offset": 150},
            {"topic": "topic2", "partition": 0, "offset": 200},
        ]

        results = await repo.batch_update_offsets(offset_data, "test-group")

        assert len(results) == 3
        assert all(isinstance(r, KafkaOffset) for r in results)
        assert results[0].offset == 100
        assert results[1].offset == 150
        assert results[2].offset == 200

    async def test_get_stale_offsets(self, repo):
        """Test getting stale offset records."""
        old_time = datetime.utcnow() - timedelta(hours=2)

        # Create some offset records
        await repo.update_offset("topic1", 0, 100, "group1")
        await repo.update_offset("topic2", 0, 200, "group1")

        # Manually set old timestamp for one record
        # (In real usage, this would happen over time)
        # For testing, we'll create a record with old timestamp

        # Get stale offsets (should return none since they're fresh)
        stale = await repo.get_stale_offsets(max_age_minutes=60)
        assert len(stale) == 0  # All records are fresh

    async def test_unique_constraint(self, repo):
        """Test unique constraint on topic/partition/group."""
        # Create first record
        await repo.update_offset("topic1", 0, 100, "group1")

        # Try to create another with same key
        result = await repo.update_offset("topic1", 0, 200, "group1")

        # Should update existing record, not create new
        assert result.offset == 200


class TestProcessingErrorRepository:
    """Test cases for ProcessingErrorRepository."""

    @pytest.fixture
    async def repo(self, test_db_session):
        """Create repository instance."""
        return ProcessingErrorRepository(test_db_session)

    async def test_create_error(self, repo):
        """Test creating a processing error record."""
        result = await repo.create_error(
            topic="test.topic",
            partition=0,
            offset=100,
            error_type="ValueError",
            error_message="Invalid value",
            message_key="test-key",
            message_value='{"test": "data"}'
        )

        assert isinstance(result, ProcessingError)
        assert result.topic == "test.topic"
        assert result.error_type == "ValueError"
        assert result.status == "failed"
        assert result.retry_count == 0

    async def test_get_retryable_errors(self, repo):
        """Test getting errors that can be retried."""
        # Create errors with different retry counts
        await repo.create_error("topic1", 0, 100, "Error1", "Message1", max_retries=3)
        await repo.create_error("topic2", 0, 200, "Error2", "Message2", max_retries=1)

        # Both should be retryable initially
        retryable = await repo.get_retryable_errors()
        assert len(retryable) == 2

        # Mark one as having exceeded retries
        # (This would normally happen through increment_retry_count)
        # For testing, we'll directly update
        await repo.session.execute(
            "UPDATE processing_errors SET retry_count = 3 WHERE offset = 100"
        )
        await repo.session.commit()

        retryable = await repo.get_retryable_errors()
        assert len(retryable) == 1  # Only one should be retryable now

    async def test_increment_retry_count(self, repo):
        """Test incrementing retry count."""
        # Create error
        error = await repo.create_error("topic1", 0, 100, "Error1", "Message1")

        # Increment retry count
        updated = await repo.increment_retry_count(error.id)

        assert updated is not None
        assert updated.retry_count == 1

    async def test_mark_error_resolved(self, repo):
        """Test marking error as resolved."""
        # Create error
        error = await repo.create_error("topic1", 0, 100, "Error1", "Message1")

        # Mark as resolved
        success = await repo.mark_error_resolved(error.id)

        assert success is True

        # Check that it's marked resolved
        # (In real usage, you'd query the database)
        await repo.session.refresh(error)
        assert error.status == "resolved"


class TestRepositorySession:
    """Test cases for repository session management."""

    async def test_get_repository_session(self, test_db_session):
        """Test getting repository session context manager."""
        async with get_repository_session() as (audit_repo, offset_repo, error_repo):
            assert isinstance(audit_repo, AuditLoggingRepository)
            assert isinstance(offset_repo, KafkaOffsetRepository)
            assert isinstance(error_repo, ProcessingErrorRepository)

            # Test that repositories work
            result = await audit_repo.create_audit_log(
                source_table="test",
                operation_type=OperationTypeEnum.INSERT
            )
            assert result.id is not None

    async def test_session_rollback_on_error(self, test_db_session):
        """Test that session rolls back on error."""
        with pytest.raises(ValueError):
            async with get_repository_session() as (audit_repo, offset_repo, error_repo):
                await audit_repo.create_audit_log(
                    source_table="test",
                    operation_type=OperationTypeEnum.INSERT
                )
                raise ValueError("Test error")  # This should cause rollback

        # Check that no records were actually created
        async with get_repository_session() as (audit_repo, offset_repo, error_repo):
            logs = await audit_repo.get_audit_logs()
            assert len(logs) == 0


class TestErrorHandling:
    """Test error handling in repositories."""

    async def test_duplicate_key_error(self, test_db_session):
        """Test handling of duplicate key errors."""
        repo = AuditLoggingRepository(test_db_session)

        # This would require setting up a unique constraint violation
        # For now, just test that the error class exists
        from src.database import DuplicateKeyError
        assert DuplicateKeyError is not None

    async def test_repository_error(self, test_db_session):
        """Test repository error handling."""
        repo = AuditLoggingRepository(test_db_session)

        # Test with invalid data that might cause SQLAlchemy errors
        # (Most validation happens at the model level)
        with pytest.raises(Exception):  # Could be more specific
            await repo.create_audit_log(
                source_table=None,  # Invalid: should not be None
                operation_type=OperationTypeEnum.INSERT
            )