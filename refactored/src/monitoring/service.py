"""
Monitoring service for CDC Audit System.

This module provides:
- Health check endpoints
- Metrics collection and exposure
- Prometheus integration
- System monitoring
- Alerting hooks

Key improvements over original:
- Comprehensive health checks
- Prometheus metrics with rich metadata
- System resource monitoring
- Configurable alerting
- REST API for monitoring data
- Performance metrics collection
"""

import asyncio
import time
import psutil
import platform
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from aiohttp import web, ClientError
from prometheus_client import (
    Counter, Gauge, Histogram, CollectorRegistry,
    generate_latest, CONTENT_TYPE_LATEST
)

from ..config import MonitoringConfig
from ..database import DatabaseService
from ..core.logging import logger


@dataclass
class HealthStatus:
    """Health check status container."""
    status: str  # "healthy", "unhealthy", "degraded"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    checks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status,
            "timestamp": self.timestamp.isoformat() + "Z",
            "checks": self.checks,
            "message": self.message,
        }


class MetricsCollector:
    """Prometheus metrics collector."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()

        # Message processing metrics
        self.messages_consumed = Counter(
            'cdc_messages_consumed_total',
            'Total number of messages consumed',
            ['topic', 'partition'],
            registry=self.registry
        )

        self.messages_processed = Counter(
            'cdc_messages_processed_total',
            'Total number of messages successfully processed',
            ['topic', 'operation_type'],
            registry=self.registry
        )

        self.messages_failed = Counter(
            'cdc_messages_failed_total',
            'Total number of messages that failed processing',
            ['topic', 'error_type'],
            registry=self.registry
        )

        # Batch processing metrics
        self.batches_processed = Counter(
            'cdc_batches_processed_total',
            'Total number of batches processed',
            registry=self.registry
        )

        self.batch_processing_time = Histogram(
            'cdc_batch_processing_duration_seconds',
            'Time spent processing batches',
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
            registry=self.registry
        )

        # Database metrics
        self.db_connections_active = Gauge(
            'cdc_db_connections_active',
            'Number of active database connections',
            registry=self.registry
        )

        self.db_operation_duration = Histogram(
            'cdc_db_operation_duration_seconds',
            'Time spent on database operations',
            ['operation', 'table'],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
            registry=self.registry
        )

        # Consumer lag metrics
        self.consumer_lag = Gauge(
            'cdc_consumer_lag',
            'Consumer lag in messages',
            ['topic', 'partition'],
            registry=self.registry
        )

        # System metrics
        self.system_cpu_usage = Gauge(
            'cdc_system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self.registry
        )

        self.system_memory_usage = Gauge(
            'cdc_system_memory_usage_bytes',
            'System memory usage in bytes',
            registry=self.registry
        )

        self.system_disk_usage = Gauge(
            'cdc_system_disk_usage_bytes',
            'System disk usage in bytes',
            ['mount_point'],
            registry=self.registry
        )

        # Application metrics
        self.app_uptime = Gauge(
            'cdc_app_uptime_seconds',
            'Application uptime in seconds',
            registry=self.registry
        )

        self.app_start_time = time.time()

    def record_message_consumed(self, topic: str, partition: int) -> None:
        """Record a consumed message."""
        self.messages_consumed.labels(topic=topic, partition=partition).inc()

    def record_message_processed(self, topic: str, operation_type: str) -> None:
        """Record a successfully processed message."""
        self.messages_processed.labels(topic=topic, operation_type=operation_type).inc()

    def record_message_failed(self, topic: str, error_type: str) -> None:
        """Record a failed message."""
        self.messages_failed.labels(topic=topic, error_type=error_type).inc()

    def record_batch_processed(self, duration_seconds: float) -> None:
        """Record batch processing metrics."""
        self.batches_processed.inc()
        self.batch_processing_time.observe(duration_seconds)

    def update_db_connections(self, active_connections: int) -> None:
        """Update database connection count."""
        self.db_connections_active.set(active_connections)

    def record_db_operation(self, operation: str, table: str, duration_seconds: float) -> None:
        """Record database operation duration."""
        self.db_operation_duration.labels(operation=operation, table=table).observe(duration_seconds)

    def update_consumer_lag(self, topic: str, partition: int, lag: int) -> None:
        """Update consumer lag."""
        self.consumer_lag.labels(topic=topic, partition=partition).set(lag)

    def collect_system_metrics(self) -> None:
        """Collect current system metrics."""
        # CPU usage
        self.system_cpu_usage.set(psutil.cpu_percent(interval=None))

        # Memory usage
        memory = psutil.virtual_memory()
        self.system_memory_usage.set(memory.used)

        # Disk usage
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                self.system_disk_usage.labels(mount_point=partition.mountpoint).set(usage.used)
            except (OSError, PermissionError):
                # Skip partitions we can't access
                continue

        # Application uptime
        self.app_uptime.set(time.time() - self.app_start_time)

    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format."""
        return generate_latest(self.registry).decode('utf-8')


class HealthChecker:
    """Health check manager."""

    def __init__(self, database_service: DatabaseService):
        self.database_service = database_service
        self.checks: List[Callable] = []
        self._last_check_time = None
        self._last_status = None
        self._check_interval = 30  # seconds

    def add_check(self, check_func: Callable) -> None:
        """Add a health check function."""
        self.checks.append(check_func)

    async def run_health_checks(self) -> HealthStatus:
        """Run all health checks."""
        now = datetime.utcnow()

        # Return cached result if recent
        if (self._last_check_time and
            self._last_status and
            (now - self._last_check_time).total_seconds() < self._check_interval):
            return self._last_status

        checks_results = {}
        overall_status = "healthy"

        # Run database health check
        try:
            db_health = await self.database_service.get_system_health()
            checks_results["database"] = db_health
            if db_health.get("overall_status") != "healthy":
                overall_status = "unhealthy"
        except Exception as e:
            checks_results["database"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            overall_status = "unhealthy"

        # Run custom checks
        for check_func in self.checks:
            try:
                check_name = getattr(check_func, '__name__', 'unknown_check')
                result = await check_func()
                checks_results[check_name] = result

                if result.get("status") != "healthy":
                    overall_status = "unhealthy"

            except Exception as e:
                checks_results[getattr(check_func, '__name__', 'unknown_check')] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                overall_status = "unhealthy"

        # Add system information
        checks_results["system"] = {
            "status": "healthy",
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count": psutil.cpu_count(),
            "memory_total": psutil.virtual_memory().total,
        }

        status = HealthStatus(
            status=overall_status,
            checks=checks_results
        )

        self._last_check_time = now
        self._last_status = status

        return status


class MonitoringService:
    """Main monitoring service."""

    def __init__(
        self,
        config: MonitoringConfig,
        database_service: DatabaseService,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.database_service = database_service
        self.metrics = metrics_collector or MetricsCollector()
        self.health_checker = HealthChecker(database_service)

        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the monitoring service."""
        logger.info("Starting monitoring service...")

        if not self.config.enabled:
            logger.info("Monitoring disabled in configuration")
            return

        # Set up web application
        self.app = web.Application()

        # Add routes
        self.app.router.add_get('/health', self.health_check_handler)
        self.app.router.add_get('/ready', self.readiness_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)

        # Start web server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(
            self.runner,
            '0.0.0.0',
            self.config.health_check_port
        )
        await self.site.start()

        logger.info(f"Health check server started on port {self.config.health_check_port}")

        # Start background monitoring
        if self.config.collect_system_metrics:
            self._monitoring_task = asyncio.create_task(self._background_monitoring())

        logger.info("Monitoring service started")

    async def stop(self) -> None:
        """Stop the monitoring service."""
        logger.info("Stopping monitoring service...")

        # Stop background monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        # Stop web server
        if self.site:
            await self.site.stop()

        if self.runner:
            await self.runner.cleanup()

        logger.info("Monitoring service stopped")

    async def health_check_handler(self, request: web.Request) -> web.Response:
        """Health check endpoint handler."""
        try:
            health_status = await self.health_checker.run_health_checks()

            status_code = 200 if health_status.status == "healthy" else 503

            return web.json_response(
                health_status.to_dict(),
                status=status_code
            )

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return web.json_response(
                {
                    "status": "unhealthy",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                },
                status=503
            )

    async def readiness_handler(self, request: web.Request) -> web.Response:
        """Readiness check endpoint handler."""
        # For now, readiness is the same as health
        # In production, you might want different logic
        return await self.health_check_handler(request)

    async def metrics_handler(self, request: web.Request) -> web.Response:
        """Metrics endpoint handler."""
        if not self.config.prometheus_enabled:
            return web.Response(status=404, text="Metrics not enabled")

        try:
            # Collect current system metrics
            self.metrics.collect_system_metrics()

            metrics_text = self.metrics.get_metrics_text()

            return web.Response(
                text=metrics_text,
                content_type=CONTENT_TYPE_LATEST
            )

        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            return web.Response(status=500, text=f"Metrics error: {e}")

    async def _background_monitoring(self) -> None:
        """Background monitoring task."""
        while True:
            try:
                # Collect system metrics
                self.metrics.collect_system_metrics()

                # Update database connection count (if available)
                # This would need access to the database manager

                await asyncio.sleep(60)  # Collect every minute

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background monitoring error: {e}")
                await asyncio.sleep(60)

    def record_message_consumed(self, topic: str, partition: int) -> None:
        """Record message consumption."""
        if self.config.collect_consumer_metrics:
            self.metrics.record_message_consumed(topic, partition)

    def record_message_processed(self, topic: str, operation_type: str) -> None:
        """Record successful message processing."""
        if self.config.collect_consumer_metrics:
            self.metrics.record_message_processed(topic, operation_type)

    def record_message_failed(self, topic: str, error_type: str) -> None:
        """Record failed message processing."""
        if self.config.collect_consumer_metrics:
            self.metrics.record_message_failed(topic, error_type)

    def record_batch_processed(self, duration_seconds: float) -> None:
        """Record batch processing."""
        if self.config.collect_consumer_metrics:
            self.metrics.record_batch_processed(duration_seconds)

    def record_db_operation(
        self,
        operation: str,
        table: str,
        duration_seconds: float
    ) -> None:
        """Record database operation."""
        if self.config.collect_database_metrics:
            self.metrics.record_db_operation(operation, table, duration_seconds)


# Global monitoring service instance
_monitoring_service: Optional[MonitoringService] = None


def get_monitoring_service() -> Optional[MonitoringService]:
    """Get the global monitoring service instance."""
    return _monitoring_service


def set_monitoring_service(service: MonitoringService) -> None:
    """Set the global monitoring service instance."""
    global _monitoring_service
    _monitoring_service = service


@asynccontextmanager
async def create_monitoring_service(
    config: MonitoringConfig,
    database_service: DatabaseService
) -> MonitoringService:
    """
    Context manager for monitoring service lifecycle.

    Usage:
        async with create_monitoring_service(config, db_service) as monitoring:
            # Use monitoring service
            pass
    """
    service = MonitoringService(config, database_service)
    set_monitoring_service(service)

    try:
        yield service
    finally:
        set_monitoring_service(None)