"""
Monitoring package for CDC Audit System.

This package provides:
- Health check endpoints and monitoring
- Prometheus metrics collection and exposure
- Performance monitoring and timing
- System resource monitoring
- Alerting and notification hooks

Key improvements over original:
- Comprehensive health checks with detailed status
- Rich Prometheus metrics with proper labeling
- Automatic performance monitoring
- System resource tracking
- HTTP endpoints for monitoring data
- Configurable alerting thresholds
"""

from .service import (
    HealthStatus,
    MetricsCollector,
    HealthChecker,
    MonitoringService,
    get_monitoring_service,
    set_monitoring_service,
    create_monitoring_service,
)

from .middleware import (
    database_operation_timer,
    consumer_operation_timer,
    time_database_operation,
    time_consumer_operation,
    time_batch_operation,
    MetricsMiddleware,
    record_message_consumed,
    record_message_processed,
    record_message_failed,
    monitor_performance,
)

__all__ = [
    # Service classes
    "HealthStatus",
    "MetricsCollector",
    "HealthChecker",
    "MonitoringService",

    # Service functions
    "get_monitoring_service",
    "set_monitoring_service",
    "create_monitoring_service",

    # Middleware classes
    "MetricsMiddleware",

    # Middleware decorators
    "database_operation_timer",
    "consumer_operation_timer",
    "monitor_performance",

    # Middleware context managers
    "time_database_operation",
    "time_consumer_operation",
    "time_batch_operation",

    # Manual metrics recording
    "record_message_consumed",
    "record_message_processed",
    "record_message_failed",
]