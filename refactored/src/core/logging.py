"""
Advanced logging configuration for CDC Audit System.

This module provides:
- Structured logging with JSON output
- Multiple log handlers (console, file, external)
- Log filtering and formatting
- Performance monitoring
- Log correlation IDs
- Configurable log levels per component

Key improvements over original:
- Structured logging for better observability
- Multiple output destinations
- Log rotation and management
- Performance logging
- Request/transaction correlation
- Environment-specific logging configuration
"""

import os
import sys
import json
import logging
import logging.handlers
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pathlib import Path
from contextvars import ContextVar
from pythonjsonlogger import jsonlogger

from ..config import LoggingConfig, Environment


# Context variables for log correlation
correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)
transaction_id: ContextVar[Optional[str]] = ContextVar('transaction_id', default=None)
user_id: ContextVar[Optional[str]] = ContextVar('user_id', default=None)


class StructuredFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional fields."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_time_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)

        # Add timestamp in ISO format
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'

        # Add correlation IDs if available
        if correlation_id.get():
            log_record['correlation_id'] = correlation_id.get()
        if transaction_id.get():
            log_record['transaction_id'] = transaction_id.get()
        if user_id.get():
            log_record['user_id'] = user_id.get()

        # Add service information
        log_record['service'] = 'cdc-audit-consumer'
        log_record['version'] = os.getenv('APP_VERSION', '1.0.0')
        log_record['environment'] = os.getenv('ENVIRONMENT', 'development')

        # Add performance data if available
        if hasattr(record, 'duration_ms'):
            log_record['duration_ms'] = record.duration_ms
        if hasattr(record, 'operation'):
            log_record['operation'] = record.operation


class PerformanceFilter(logging.Filter):
    """Filter for adding performance context to log records."""

    def filter(self, record):
        # Add performance data to record
        if hasattr(record, 'extra') and record.extra:
            for key, value in record.extra.items():
                if key.startswith('perf_'):
                    setattr(record, key, value)

        return True


class ComponentFilter(logging.Filter):
    """Filter logs by component/module."""

    def __init__(self, allowed_components: Optional[list] = None):
        super().__init__()
        self.allowed_components = allowed_components or []

    def filter(self, record):
        if not self.allowed_components:
            return True

        # Extract component from logger name (e.g., 'cdc.consumer' -> 'consumer')
        logger_parts = record.name.split('.')
        component = logger_parts[-1] if len(logger_parts) > 1 else record.name

        return component in self.allowed_components


class LogContextManager:
    """Context manager for log correlation."""

    def __init__(self, corr_id: Optional[str] = None, tx_id: Optional[str] = None, user: Optional[str] = None):
        self.corr_id = corr_id or correlation_id.get()
        self.tx_id = tx_id or transaction_id.get()
        self.user = user or user_id.get()
        self.token_corr = None
        self.token_tx = None
        self.token_user = None

    def __enter__(self):
        self.token_corr = correlation_id.set(self.corr_id)
        self.token_tx = transaction_id.set(self.tx_id)
        self.token_user = user_id.set(self.user)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        correlation_id.reset(self.token_corr)
        if self.token_tx:
            transaction_id.reset(self.token_tx)
        if self.token_user:
            user_id.reset(self.token_user)


def setup_logging(config: LoggingConfig, environment: Environment) -> None:
    """
    Set up comprehensive logging configuration.

    Args:
        config: Logging configuration
        environment: Deployment environment
    """
    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set root logger level
    root_logger.setLevel(getattr(logging, config.level))

    # Create formatters
    if config.structured:
        formatter = StructuredFormatter(
            fmt='%(timestamp)s %(level)s %(name)s %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%SZ'
        )
    else:
        formatter = logging.Formatter(
            fmt=config.format,
            datefmt=config.date_format
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.level))
    console_handler.setFormatter(formatter)
    console_handler.addFilter(PerformanceFilter())

    # Add console handler to root logger
    root_logger.addHandler(console_handler)

    # File handler (if configured)
    if config.log_to_file and config.log_file_path:
        log_path = Path(config.log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            filename=str(log_path),
            maxBytes=config.max_file_size,
            backupCount=config.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, config.level))
        file_handler.setFormatter(formatter)
        file_handler.addFilter(PerformanceFilter())

        root_logger.addHandler(file_handler)

    # Environment-specific adjustments
    if environment == Environment.PRODUCTION:
        # In production, reduce noise from third-party libraries
        logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
        logging.getLogger('kafka').setLevel(logging.WARNING)
        logging.getLogger('confluent_kafka').setLevel(logging.WARNING)
    elif environment == Environment.DEVELOPMENT:
        # In development, enable more detailed logging
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    # Set up specific loggers for different components
    _setup_component_loggers(config, environment, formatter)


def _setup_component_loggers(
    config: LoggingConfig,
    environment: Environment,
    formatter: logging.Formatter
) -> None:
    """Set up component-specific loggers."""

    # Database logger
    db_logger = logging.getLogger('cdc.database')
    if environment == Environment.DEVELOPMENT:
        db_logger.setLevel(logging.DEBUG)
    else:
        db_logger.setLevel(logging.INFO)

    # Consumer logger
    consumer_logger = logging.getLogger('cdc.consumer')
    consumer_logger.setLevel(logging.INFO)

    # Kafka logger
    kafka_logger = logging.getLogger('cdc.kafka')
    if environment == Environment.DEVELOPMENT:
        kafka_logger.setLevel(logging.DEBUG)
    else:
        kafka_logger.setLevel(logging.WARNING)

    # Monitoring logger
    monitoring_logger = logging.getLogger('cdc.monitoring')
    monitoring_logger.setLevel(logging.INFO)


class PerformanceLogger:
    """Logger for performance monitoring."""

    def __init__(self, logger_name: str = 'cdc.performance'):
        self.logger = logging.getLogger(logger_name)

    def log_operation(
        self,
        operation: str,
        duration_ms: float,
        success: bool = True,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log operation performance."""
        level = logging.INFO if success else logging.WARNING

        extra_data = extra or {}
        extra_data.update({
            'perf_operation': operation,
            'perf_duration_ms': duration_ms,
            'perf_success': success,
        })

        self.logger.log(
            level,
            f"Operation {operation} completed in {duration_ms:.2f}ms",
            extra=extra_data
        )

    def log_batch(
        self,
        batch_size: int,
        processing_time_ms: float,
        success: bool = True,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log batch processing performance."""
        level = logging.INFO if success else logging.ERROR

        throughput = batch_size / (processing_time_ms / 1000) if processing_time_ms > 0 else 0

        extra_data = extra or {}
        extra_data.update({
            'perf_batch_size': batch_size,
            'perf_processing_time_ms': processing_time_ms,
            'perf_throughput_msg_per_sec': throughput,
            'perf_success': success,
        })

        self.logger.log(
            level,
            f"Processed batch of {batch_size} messages in {processing_time_ms:.2f}ms "
            ".2f",
            extra=extra_data
        )

    def log_database_operation(
        self,
        operation: str,
        table: str,
        record_count: int,
        duration_ms: float,
        success: bool = True,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log database operation performance."""
        level = logging.INFO if success else logging.ERROR

        records_per_sec = record_count / (duration_ms / 1000) if duration_ms > 0 else 0

        extra_data = extra or {}
        extra_data.update({
            'perf_operation': operation,
            'perf_table': table,
            'perf_record_count': record_count,
            'perf_duration_ms': duration_ms,
            'perf_records_per_sec': records_per_sec,
            'perf_success': success,
        })

        self.logger.log(
            level,
            f"DB {operation} on {table}: {record_count} records in {duration_ms:.2f}ms "
            ".2f",
            extra=extra_data
        )


class AuditLogger:
    """Specialized logger for audit events."""

    def __init__(self, logger_name: str = 'cdc.audit'):
        self.logger = logging.getLogger(logger_name)

    def log_change(
        self,
        table: str,
        operation: str,
        record_id: Optional[Union[int, str]] = None,
        user: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an audit change event."""
        message = f"AUDIT: {operation} on {table}"
        if record_id:
            message += f" (ID: {record_id})"
        if user:
            message += f" by {user}"

        extra_data = extra or {}
        extra_data.update({
            'audit_table': table,
            'audit_operation': operation,
            'audit_record_id': record_id,
            'audit_user': user,
            'audit_old_values': old_values,
            'audit_new_values': new_values,
        })

        self.logger.info(message, extra=extra_data)

    def log_security_event(
        self,
        event: str,
        severity: str,
        user: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a security-related event."""
        level_map = {
            'low': logging.INFO,
            'medium': logging.WARNING,
            'high': logging.ERROR,
            'critical': logging.CRITICAL,
        }

        level = level_map.get(severity.lower(), logging.INFO)

        message = f"SECURITY: {event}"
        if user:
            message += f" (User: {user})"

        extra_data = extra or {}
        extra_data.update({
            'security_event': event,
            'security_severity': severity,
            'security_user': user,
            'security_details': details,
        })

        self.logger.log(level, message, extra=extra_data)


# Global logger instances
performance_logger = PerformanceLogger()
audit_logger = AuditLogger()


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID."""
    return correlation_id.get()


def set_correlation_id(corr_id: str) -> None:
    """Set correlation ID for current context."""
    correlation_id.set(corr_id)


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    import uuid
    return str(uuid.uuid4())


def with_correlation_id(corr_id: Optional[str] = None):
    """Decorator to set correlation ID for function execution."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            cid = corr_id or generate_correlation_id()
            with LogContextManager(corr_id=cid):
                return await func(*args, **kwargs)

        def sync_wrapper(*args, **kwargs):
            cid = corr_id or generate_correlation_id()
            with LogContextManager(corr_id=cid):
                return func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# Convenience functions
def log_performance(operation: str, duration_ms: float, **extra):
    """Convenience function for performance logging."""
    performance_logger.log_operation(operation, duration_ms, extra=extra)


def log_audit_change(table: str, operation: str, **extra):
    """Convenience function for audit logging."""
    audit_logger.log_change(table, operation, **extra)


def create_log_context(**kwargs):
    """Create a log context manager."""
    return LogContextManager(**kwargs)