"""
Unit tests for message transformer component.

Tests cover:
- Message parsing and validation
- Data transformation logic
- Error handling
- Edge cases
"""

import pytest
import json
from datetime import datetime
from unittest.mock import Mock

from src.consumer.transformer import (
    MessageTransformer,
    TransformedMessage,
    TransformationError,
    ValidationError,
    transform_cdc_message
)
from src.database import OperationTypeEnum


class TestMessageTransformer:
    """Test cases for MessageTransformer class."""

    @pytest.fixture
    def transformer(self):
        """Create transformer instance for testing."""
        return MessageTransformer(validate_schema=True, sanitize_data=True)

    @pytest.fixture
    def mock_message(self, sample_debezium_message):
        """Create mock Kafka message."""
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(sample_debezium_message).encode('utf-8')
        mock_msg.topic.return_value = "source_db.public.people"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.key.return_value = b"user-1"
        mock_msg.error.return_value = None
        return mock_msg

    def test_successful_transformation(self, transformer, mock_message):
        """Test successful message transformation."""
        result = transformer.transform_message(
            mock_message, "source_db.public.people", 0, 100
        )

        assert isinstance(result, TransformedMessage)
        assert result.source_table == "people"
        assert result.operation_type == OperationTypeEnum.INSERT
        assert result.new_data["name"] == "John Doe"
        assert result.new_data["email"] == "john.doe@example.com"
        assert isinstance(result.change_timestamp, datetime)
        assert result.message_topic == "source_db.public.people"
        assert result.message_partition == 0
        assert result.message_offset == 100

    def test_update_message_transformation(self, transformer, sample_update_message):
        """Test UPDATE message transformation."""
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(sample_update_message).encode('utf-8')
        mock_msg.topic.return_value = "source_db.public.people"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 200
        mock_msg.key.return_value = None
        mock_msg.error.return_value = None

        result = transformer.transform_message(mock_msg, "source_db.public.people", 0, 200)

        assert result.operation_type == OperationTypeEnum.UPDATE
        assert result.old_data["name"] == "John Doe"
        assert result.new_data["name"] == "John Smith"
        assert result.old_data is not None
        assert result.new_data is not None

    def test_delete_message_transformation(self, transformer, sample_delete_message):
        """Test DELETE message transformation."""
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(sample_delete_message).encode('utf-8')
        mock_msg.topic.return_value = "source_db.public.people"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 300
        mock_msg.key.return_value = None
        mock_msg.error.return_value = None

        result = transformer.transform_message(mock_msg, "source_db.public.people", 0, 300)

        assert result.operation_type == OperationTypeEnum.DELETE
        assert result.old_data["name"] == "John Smith"
        assert result.new_data is None

    def test_invalid_message_value(self, transformer):
        """Test handling of invalid message value."""
        mock_msg = Mock()
        mock_msg.value.return_value = None  # Invalid value
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0

        with pytest.raises(TransformationError):
            transformer.transform_message(mock_msg, "test.topic", 0, 0)

    def test_malformed_json(self, transformer):
        """Test handling of malformed JSON."""
        mock_msg = Mock()
        mock_msg.value.return_value = b"invalid json"
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0

        with pytest.raises(TransformationError):
            transformer.transform_message(mock_msg, "test.topic", 0, 0)

    def test_missing_payload(self, transformer):
        """Test handling of message missing payload."""
        invalid_message = {"schema": {}}
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(invalid_message).encode('utf-8')
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0

        with pytest.raises(ValidationError):
            transformer.transform_message(mock_msg, "test.topic", 0, 0)

    def test_invalid_operation_type(self, transformer, sample_debezium_message):
        """Test handling of invalid operation type."""
        # Modify message to have invalid operation
        sample_debezium_message["payload"]["op"] = "x"  # Invalid operation

        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(sample_debezium_message).encode('utf-8')
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0

        # Should still work but default to INSERT
        result = transformer.transform_message(mock_msg, "test.topic", 0, 0)
        assert result.operation_type == OperationTypeEnum.INSERT

    def test_timestamp_conversion(self, transformer, sample_debezium_message):
        """Test timestamp conversion."""
        # Test with valid timestamp
        result = transformer.transform_message(
            self._create_mock_message(sample_debezium_message),
            "test.topic", 0, 0
        )
        assert isinstance(result.change_timestamp, datetime)

        # Test with invalid timestamp
        sample_debezium_message["payload"]["ts_ms"] = "invalid"
        result = transformer.transform_message(
            self._create_mock_message(sample_debezium_message),
            "test.topic", 0, 0
        )
        assert isinstance(result.change_timestamp, datetime)  # Should use current time

    def test_data_sanitization(self, transformer):
        """Test data sanitization functionality."""
        # Create message with bytes and datetime data
        test_data = {
            "schema": {"type": "struct"},
            "payload": {
                "op": "c",
                "ts_ms": 1703123456789,
                "source": {
                    "table": "test_table",
                    "connector": "postgresql",
                    "name": "test",
                    "db": "test",
                    "schema": "public"
                },
                "after": {
                    "id": 1,
                    "name": "Test User",
                    "binary_data": b"some_bytes",
                    "timestamp": datetime(2023, 1, 1, 12, 0, 0),
                    "nested": {"key": "value"}
                }
            }
        }

        mock_msg = self._create_mock_message(test_data)
        result = transformer.transform_message(mock_msg, "test.topic", 0, 0)

        # Check that binary data is converted to string representation
        assert "binary_data" in result.new_data
        assert isinstance(result.new_data["binary_data"], str)

        # Check that datetime is converted to ISO string
        assert "timestamp" in result.new_data
        assert isinstance(result.new_data["timestamp"], str)

        # Check that nested dict is preserved
        assert result.new_data["nested"]["key"] == "value"

    def test_validation_disabled(self):
        """Test transformer with validation disabled."""
        transformer = MessageTransformer(validate_schema=False)

        # Create invalid message (missing required fields)
        invalid_message = {"payload": {"op": "c"}}

        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(invalid_message).encode('utf-8')
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0
        mock_msg.key.return_value = None
        mock_msg.error.return_value = None

        # Should work without validation
        result = transformer.transform_message(mock_msg, "test.topic", 0, 0)
        assert isinstance(result, TransformedMessage)

    def test_batch_transformation(self, transformer, sample_debezium_message):
        """Test batch message transformation."""
        messages = []
        for i in range(3):
            mock_msg = Mock()
            mock_msg.value.return_value = json.dumps(sample_debezium_message).encode('utf-8')
            mock_msg.topic.return_value = f"topic{i}"
            mock_msg.partition.return_value = i
            mock_msg.offset.return_value = i * 100
            mock_msg.key.return_value = f"key{i}".encode()
            mock_msg.error.return_value = None
            messages.append((mock_msg, f"topic{i}", i, i * 100))

        results = transformer.transform_batch(messages)

        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.message_topic == f"topic{i}"
            assert result.message_partition == i
            assert result.message_offset == i * 100

    def test_batch_transformation_with_errors(self, transformer, sample_debezium_message):
        """Test batch transformation with some invalid messages."""
        # Create mix of valid and invalid messages
        messages = []

        # Valid message
        valid_msg = Mock()
        valid_msg.value.return_value = json.dumps(sample_debezium_message).encode('utf-8')
        valid_msg.topic.return_value = "valid.topic"
        valid_msg.partition.return_value = 0
        valid_msg.offset.return_value = 0
        valid_msg.key.return_value = None
        valid_msg.error.return_value = None
        messages.append((valid_msg, "valid.topic", 0, 0))

        # Invalid message
        invalid_msg = Mock()
        invalid_msg.value.return_value = b"invalid json"
        invalid_msg.topic.return_value = "invalid.topic"
        invalid_msg.partition.return_value = 0
        invalid_msg.offset.return_value = 1
        messages.append((invalid_msg, "invalid.topic", 0, 1))

        # Another valid message
        messages.append((valid_msg, "valid.topic", 0, 2))

        results = transformer.transform_batch(messages)

        # Should only return valid messages
        assert len(results) == 2
        assert all(isinstance(r, TransformedMessage) for r in results)

    def _create_mock_message(self, message_data):
        """Helper to create mock message."""
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(message_data).encode('utf-8')
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0
        mock_msg.key.return_value = None
        mock_msg.error.return_value = None
        return mock_msg


class TestTransformedMessage:
    """Test cases for TransformedMessage class."""

    def test_to_dict(self):
        """Test conversion to dictionary."""
        from datetime import datetime

        msg = TransformedMessage(
            source_table="people",
            operation_type=OperationTypeEnum.INSERT,
            change_timestamp=datetime(2023, 1, 1, 12, 0, 0),
            old_data=None,
            new_data={"id": 1, "name": "Test"},
            change_user="test_user",
            transaction_id="tx-123",
            lsn="0/12345678",
            source_metadata={"connector": "postgresql"},
            raw_message={"test": "data"},
            message_key="test-key",
            message_offset=100,
            message_partition=0,
            message_topic="test.topic"
        )

        result = msg.to_dict()

        assert result["source_table"] == "people"
        assert result["operation_type"] == "INSERT"
        assert "change_timestamp" in result
        assert result["old_data"] is None
        assert result["new_data"]["name"] == "Test"
        assert result["change_user"] == "test_user"
        assert result["transaction_id"] == "tx-123"
        assert result["lsn"] == "0/12345678"


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_transform_cdc_message(self, sample_debezium_message):
        """Test the convenience transform function."""
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps(sample_debezium_message).encode('utf-8')
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 0
        mock_msg.key.return_value = None
        mock_msg.error.return_value = None

        result = transform_cdc_message(mock_msg, "test.topic", 0, 0)

        assert isinstance(result, TransformedMessage)
        assert result.source_table == "people"
        assert result.operation_type == OperationTypeEnum.INSERT