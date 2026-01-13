"""
Message transformer for CDC Audit System.

This module provides:
- Debezium message parsing and transformation
- Data validation and sanitization
- Type conversion and normalization
- Error handling for malformed messages

Key improvements over original:
- Comprehensive message validation
- Better error handling and logging
- Type safety and data sanitization
- Support for different message formats
- Configurable transformation rules
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from json import JSONDecodeError

from ..config import OperationType
from ..database import OperationTypeEnum

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Raised when message transformation fails."""
    pass


class ValidationError(TransformationError):
    """Raised when message validation fails."""
    pass


@dataclass
class TransformedMessage:
    """Represents a successfully transformed CDC message."""
    source_table: str
    operation_type: OperationTypeEnum
    change_timestamp: datetime
    old_data: Optional[Dict[str, Any]]
    new_data: Optional[Dict[str, Any]]
    change_user: Optional[str]
    transaction_id: Optional[str]
    lsn: Optional[str]
    source_metadata: Optional[Dict[str, Any]]

    # Raw message data for debugging
    raw_message: Dict[str, Any]
    message_key: Optional[str]
    message_offset: int
    message_partition: int
    message_topic: str


class MessageTransformer:
    """
    Transformer for Debezium CDC messages.

    Handles parsing, validation, and transformation of Kafka messages
    from Debezium into audit log records.
    """

    def __init__(self, validate_schema: bool = True, sanitize_data: bool = True):
        """
        Initialize the message transformer.

        Args:
            validate_schema: Whether to validate message schema
            sanitize_data: Whether to sanitize/transform data values
        """
        self.validate_schema = validate_schema
        self.sanitize_data = sanitize_data

        # Required fields for Debezium messages
        self.required_fields = ["payload", "schema"]
        self.required_payload_fields = ["op", "ts_ms", "source"]

    def transform_message(
        self,
        message,
        topic: str,
        partition: int,
        offset: int
    ) -> TransformedMessage:
        """
        Transform a Kafka message into audit log data.

        Args:
            message: Kafka message object
            topic: Topic name
            partition: Partition number
            offset: Message offset

        Returns:
            TransformedMessage instance

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse message value
            raw_data = self._parse_message_value(message.value())
            message_key = self._parse_message_key(message.key())

            # Validate message structure
            if self.validate_schema:
                self._validate_message_structure(raw_data)

            # Extract payload
            payload = raw_data.get("payload", {})

            # Transform the payload
            transformed = self._transform_payload(payload, raw_data)

            return TransformedMessage(
                source_table=transformed["source_table"],
                operation_type=transformed["operation_type"],
                change_timestamp=transformed["change_timestamp"],
                old_data=transformed["old_data"],
                new_data=transformed["new_data"],
                change_user=transformed["change_user"],
                transaction_id=transformed["transaction_id"],
                lsn=transformed["lsn"],
                source_metadata=transformed["source_metadata"],
                raw_message=raw_data,
                message_key=message_key,
                message_offset=offset,
                message_partition=partition,
                message_topic=topic,
            )

        except Exception as e:
            logger.error(f"Failed to transform message from {topic}:{partition}:{offset}: {e}")
            raise TransformationError(f"Message transformation failed: {e}") from e

    def _parse_message_value(self, message_value: bytes) -> Dict[str, Any]:
        """Parse the message value from bytes to dict."""
        if message_value is None:
            raise ValidationError("Message value is None")

        try:
            decoded = message_value.decode('utf-8')
            return json.loads(decoded)
        except (UnicodeDecodeError, JSONDecodeError) as e:
            raise ValidationError(f"Failed to parse message value: {e}") from e

    def _parse_message_key(self, message_key: Optional[bytes]) -> Optional[str]:
        """Parse the message key from bytes to string."""
        if message_key is None:
            return None

        try:
            return message_key.decode('utf-8')
        except UnicodeDecodeError:
            # Return hex representation if not valid UTF-8
            return message_key.hex()

    def _validate_message_structure(self, data: Dict[str, Any]) -> None:
        """Validate the basic structure of a Debezium message."""
        # Check required top-level fields
        for field in self.required_fields:
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")

        payload = data.get("payload", {})

        # Check required payload fields
        for field in self.required_payload_fields:
            if field not in payload:
                raise ValidationError(f"Missing required payload field: {field}")

        # Validate operation type
        op = payload.get("op")
        if op not in ["c", "u", "d"]:
            raise ValidationError(f"Invalid operation type: {op}")

        # Validate source information
        source = payload.get("source", {})
        if not source.get("table"):
            raise ValidationError("Missing source table information")

    def _transform_payload(
        self,
        payload: Dict[str, Any],
        raw_message: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform the Debezium payload into audit log format."""
        # Extract operation type
        operation_type = self._convert_operation_type(payload.get("op"))

        # Extract source information
        source = payload.get("source", {})
        source_table = source.get("table", "unknown_table")

        # Convert timestamp
        change_timestamp = self._convert_timestamp(payload.get("ts_ms"))

        # Extract data
        old_data = payload.get("before")
        new_data = payload.get("after")

        # Sanitize data if requested
        if self.sanitize_data:
            old_data = self._sanitize_data(old_data) if old_data else None
            new_data = self._sanitize_data(new_data) if new_data else None

        # Extract additional metadata
        change_user = self._extract_change_user(payload)
        transaction_id = payload.get("transaction", {}).get("id")
        lsn = source.get("lsn")

        # Build source metadata
        source_metadata = {
            "connector": source.get("connector"),
            "name": source.get("name"),
            "db": source.get("db"),
            "schema": source.get("schema"),
            "table": source_table,
            "txId": payload.get("transaction", {}).get("id"),
            "lsn": lsn,
            "xmin": source.get("xmin"),
            "version": raw_message.get("schema", {}).get("version"),
        }

        # Remove None values from metadata
        source_metadata = {k: v for k, v in source_metadata.items() if v is not None}

        return {
            "source_table": source_table,
            "operation_type": operation_type,
            "change_timestamp": change_timestamp,
            "old_data": old_data,
            "new_data": new_data,
            "change_user": change_user,
            "transaction_id": transaction_id,
            "lsn": lsn,
            "source_metadata": source_metadata,
        }

    def _convert_operation_type(self, op: str) -> OperationTypeEnum:
        """Convert Debezium operation code to enum."""
        mapping = {
            "c": OperationTypeEnum.INSERT,
            "u": OperationTypeEnum.UPDATE,
            "d": OperationTypeEnum.DELETE,
        }
        return mapping.get(op, OperationTypeEnum.INSERT)

    def _convert_timestamp(self, ts_ms: Optional[Union[int, str]]) -> datetime:
        """Convert millisecond timestamp to datetime."""
        if ts_ms is None:
            return datetime.utcnow()

        try:
            # Handle both int and string timestamps
            if isinstance(ts_ms, str):
                ts_ms = int(ts_ms)
            return datetime.utcfromtimestamp(ts_ms / 1000.0)
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"Invalid timestamp {ts_ms}, using current time: {e}")
            return datetime.utcnow()

    def _sanitize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize data values for storage.

        This can include:
        - Converting unsupported types
        - Truncating large values
        - Removing sensitive information
        - Normalizing formats
        """
        if not isinstance(data, dict):
            return data

        sanitized = {}

        for key, value in data.items():
            # Handle different data types
            if isinstance(value, datetime):
                # Convert datetime objects to ISO format strings
                sanitized[key] = value.isoformat()
            elif isinstance(value, bytes):
                # Convert bytes to string representation
                try:
                    sanitized[key] = value.decode('utf-8')
                except UnicodeDecodeError:
                    sanitized[key] = f"<bytes:{len(value)}>"
            elif isinstance(value, (int, float, str, bool, type(None))):
                # Keep primitive types as-is
                sanitized[key] = value
            elif isinstance(value, (list, dict)):
                # Recursively sanitize nested structures
                sanitized[key] = self._sanitize_data(value) if isinstance(value, dict) else value
            else:
                # Convert other types to string representation
                sanitized[key] = str(value)

        return sanitized

    def _extract_change_user(self, payload: Dict[str, Any]) -> Optional[str]:
        """Extract change user from payload if available."""
        # Try different possible locations for user information
        user_sources = [
            payload.get("user"),
            payload.get("source", {}).get("user"),
            payload.get("transaction", {}).get("user"),
        ]

        for user in user_sources:
            if user:
                return str(user)

        return None

    def transform_batch(
        self,
        messages: List[tuple]
    ) -> List[TransformedMessage]:
        """
        Transform a batch of messages.

        Args:
            messages: List of (message, topic, partition, offset) tuples

        Returns:
            List of transformed messages
        """
        transformed = []

        for message, topic, partition, offset in messages:
            try:
                transformed_msg = self.transform_message(message, topic, partition, offset)
                transformed.append(transformed_msg)
            except TransformationError as e:
                logger.error(f"Skipping malformed message {topic}:{partition}:{offset}: {e}")
                continue

        return transformed


class MessageValidator:
    """
    Additional message validation utilities.

    Provides business logic validation beyond basic structure checks.
    """

    def __init__(self, allowed_tables: Optional[List[str]] = None):
        """
        Initialize the message validator.

        Args:
            allowed_tables: List of allowed table names, None for no restriction
        """
        self.allowed_tables = set(allowed_tables) if allowed_tables else None

    def validate_business_rules(self, transformed: TransformedMessage) -> None:
        """
        Validate business rules for the transformed message.

        Args:
            transformed: Transformed message to validate

        Raises:
            ValidationError: If business rules are violated
        """
        # Check table restrictions
        if self.allowed_tables and transformed.source_table not in self.allowed_tables:
            raise ValidationError(
                f"Table '{transformed.source_table}' is not in allowed tables list"
            )

        # Validate data consistency
        if transformed.operation_type == OperationTypeEnum.INSERT:
            if transformed.new_data is None:
                raise ValidationError("INSERT operation must have new_data")
        elif transformed.operation_type == OperationTypeEnum.UPDATE:
            if transformed.new_data is None:
                raise ValidationError("UPDATE operation must have new_data")
        elif transformed.operation_type == OperationTypeEnum.DELETE:
            if transformed.old_data is None:
                raise ValidationError("DELETE operation must have old_data")

        # Validate timestamp is reasonable (not in far future)
        now = datetime.utcnow()
        if transformed.change_timestamp > now.replace(year=now.year + 1):
            raise ValidationError("Change timestamp is unreasonably far in the future")

        # Validate data size (prevent extremely large records)
        max_data_size = 10 * 1024 * 1024  # 10MB
        old_data_size = len(json.dumps(transformed.old_data or {}).encode('utf-8'))
        new_data_size = len(json.dumps(transformed.new_data or {}).encode('utf-8'))

        if old_data_size > max_data_size or new_data_size > max_data_size:
            raise ValidationError(f"Data size exceeds maximum allowed size ({max_data_size} bytes)")


# Global transformer instances
default_transformer = MessageTransformer()
default_validator = MessageValidator()


def transform_cdc_message(
    message,
    topic: str,
    partition: int,
    offset: int,
    validate_business_rules: bool = True
) -> TransformedMessage:
    """
    Convenience function to transform a CDC message.

    Args:
        message: Kafka message
        topic: Topic name
        partition: Partition number
        offset: Message offset
        validate_business_rules: Whether to validate business rules

    Returns:
        Transformed message
    """
    transformed = default_transformer.transform_message(message, topic, partition, offset)

    if validate_business_rules:
        default_validator.validate_business_rules(transformed)

    return transformed