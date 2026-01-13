# CDC Audit System API Reference

This document provides comprehensive API documentation for the CDC Audit Consumer.

## Table of Contents

- [Configuration API](#configuration-api)
- [Consumer API](#consumer-api)
- [Database API](#database-api)
- [Monitoring API](#monitoring-api)
- [REST API](#rest-api)

## Configuration API

### AppConfig

Main application configuration class.

```python
from src.config import AppConfig, app_config

# Get global configuration instance
config = app_config

# Create custom configuration
config = AppConfig.from_env()
config.validate()
```

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `environment` | `Environment` | Deployment environment |
| `debug` | `bool` | Debug mode flag |
| `app_name` | `str` | Application name |
| `version` | `str` | Application version |
| `kafka` | `KafkaConfig` | Kafka configuration |
| `sink_database` | `DatabaseConfig` | Sink database configuration |
| `source_database` | `DatabaseConfig` | Source database configuration |
| `logging` | `LoggingConfig` | Logging configuration |
| `monitoring` | `MonitoringConfig` | Monitoring configuration |
| `consumer` | `ConsumerConfig` | Consumer configuration |

#### Methods

##### `from_env() -> AppConfig`
Create configuration from environment variables.

##### `validate() -> None`
Validate configuration values.

##### `to_dict() -> Dict[str, Any]`
Convert configuration to dictionary.

### KafkaConfig

Kafka consumer configuration.

```python
from src.config import KafkaConfig

config = KafkaConfig(
    bootstrap_servers=["localhost:29092"],
    group_id="my-consumer",
    auto_offset_reset="earliest"
)
```

#### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `bootstrap_servers` | `List[str]` | `["localhost:29092"]` | Kafka broker addresses |
| `group_id` | `str` | `"cdc_audit_consumer"` | Consumer group ID |
| `auto_offset_reset` | `str` | `"earliest"` | Offset reset strategy |
| `enable_auto_commit` | `bool` | `False` | Auto commit offsets |
| `session_timeout_ms` | `int` | `30000` | Session timeout |
| `heartbeat_interval_ms` | `int` | `3000` | Heartbeat interval |
| `max_poll_records` | `int` | `500` | Max records per poll |
| `consumer_timeout_ms` | `int` | `5000` | Consumer timeout |

### DatabaseConfig

Database configuration.

```python
from src.config import DatabaseConfig

config = DatabaseConfig(
    host="localhost",
    port=5432,
    username="admin",
    password="admin",
    database="audit_db"
)
```

#### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | `str` | `"localhost"` | Database host |
| `port` | `int` | `5432` | Database port |
| `username` | `str` | `"admin"` | Database username |
| `password` | `str` | `"admin"` | Database password |
| `database` | `str` | `"sink_db"` | Database name |
| `schema` | `str` | `"public"` | Database schema |
| `pool_size` | `int` | `10` | Connection pool size |
| `max_overflow` | `int` | `20` | Max overflow connections |
| `pool_timeout` | `int` | `30` | Pool timeout |
| `pool_recycle` | `int` | `3600` | Pool recycle time |

#### Properties

##### `connection_string: str`
Generate SQLAlchemy connection string.

## Consumer API

### CDCConsumerService

Main consumer service for processing CDC messages.

```python
from src.consumer import CDCConsumerService

consumer = CDCConsumerService(
    kafka_config=kafka_config,
    consumer_config=consumer_config,
    database_service=db_service
)

await consumer.start()
```

#### Methods

##### `start() -> None`
Start the consumer service.

##### `stop() -> None`
Stop the consumer service gracefully.

##### `get_metrics() -> Dict[str, Any]`
Get consumer performance metrics.

##### `get_health_status() -> Dict[str, Any]`
Get consumer health status.

### MessageTransformer

Transforms Debezium messages into audit log format.

```python
from src.consumer import MessageTransformer

transformer = MessageTransformer(validate_schema=True)

transformed = transformer.transform_message(message, topic, partition, offset)
```

#### Methods

##### `transform_message(message, topic: str, partition: int, offset: int) -> TransformedMessage`
Transform a single Kafka message.

##### `transform_batch(messages: List[tuple]) -> List[TransformedMessage]`
Transform a batch of messages.

### TransformedMessage

Represents a transformed CDC message.

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `source_table` | `str` | Source table name |
| `operation_type` | `OperationTypeEnum` | Operation type (INSERT/UPDATE/DELETE) |
| `change_timestamp` | `datetime` | Change timestamp |
| `old_data` | `Optional[Dict]` | Data before change |
| `new_data` | `Optional[Dict]` | Data after change |
| `change_user` | `Optional[str]` | User who made change |
| `transaction_id` | `Optional[str]` | Transaction ID |
| `lsn` | `Optional[str]` | PostgreSQL LSN |
| `source_metadata` | `Optional[Dict]` | Additional metadata |

#### Methods

##### `to_dict() -> Dict[str, Any]`
Convert to dictionary representation.

## Database API

### Repository Classes

#### AuditLoggingRepository

Repository for audit logging operations.

```python
from src.database import get_repository_session

async with get_repository_session() as (audit_repo, offset_repo, error_repo):
    # Create audit log
    log = await audit_repo.create_audit_log(
        source_table="people",
        operation_type=OperationTypeEnum.INSERT,
        new_data={"id": 1, "name": "John"}
    )

    # Query audit logs
    logs = await audit_repo.get_audit_logs(
        source_table="people",
        limit=100
    )

    # Get statistics
    stats = await audit_repo.get_audit_stats(days=7)
```

##### Methods

###### `create_audit_log(...) -> AuditLogging`
Create a new audit log entry.

###### `batch_create_audit_logs(audit_logs: List[Dict]) -> List[AuditLogging]`
Create multiple audit log entries.

###### `get_audit_logs(...) -> List[AuditLogging]`
Query audit logs with optional filters.

###### `get_audit_stats(source_table=None, days=30) -> Dict[str, Any]`
Get audit statistics.

#### KafkaOffsetRepository

Repository for Kafka offset tracking.

```python
# Get current offset
offset = await offset_repo.get_offset("topic", 0, "group")

# Update offset
await offset_repo.update_offset("topic", 0, 100, "group")

# Batch update offsets
await offset_repo.batch_update_offsets(offsets, "group")
```

##### Methods

###### `get_offset(topic: str, partition: int, consumer_group: str) -> Optional[KafkaOffset]`
Get last processed offset.

###### `update_offset(...) -> KafkaOffset`
Update or create offset record.

###### `batch_update_offsets(offsets: List[Dict], consumer_group: str) -> List[KafkaOffset]`
Batch update multiple offsets.

#### ProcessingErrorRepository

Repository for processing error tracking.

```python
# Create error record
error = await error_repo.create_error(
    topic="topic",
    partition=0,
    offset=100,
    error_type="ValueError",
    error_message="Invalid data"
)

# Get retryable errors
errors = await error_repo.get_retryable_errors(limit=10)

# Mark error resolved
await error_repo.mark_error_resolved(error.id)
```

##### Methods

###### `create_error(...) -> ProcessingError`
Create a processing error record.

###### `get_retryable_errors(limit=100) -> List[ProcessingError]`
Get errors that can be retried.

###### `mark_error_resolved(error_id: int, resolution_note=None) -> bool`
Mark an error as resolved.

### Database Models

#### AuditLogging

Audit logging database model.

```python
from src.database import AuditLogging, OperationTypeEnum

log = AuditLogging(
    source_table="people",
    operation_type=OperationTypeEnum.INSERT,
    new_data={"id": 1, "name": "John"}
)
```

#### KafkaOffset

Kafka offset tracking model.

```python
from src.database import KafkaOffset

offset = KafkaOffset(
    topic="topic",
    partition=0,
    offset=100,
    consumer_group="group"
)
```

#### ProcessingError

Processing error tracking model.

```python
from src.database import ProcessingError

error = ProcessingError(
    topic="topic",
    partition=0,
    offset=100,
    error_type="ValueError",
    error_message="Invalid data"
)
```

### Database Utilities

#### `get_session() -> AsyncSession`
Get a database session.

#### `get_repository_session() -> ContextManager`
Get repository instances with session management.

#### `create_tables() -> None`
Create all database tables.

#### `drop_tables() -> None`
Drop all database tables.

## Monitoring API

### MonitoringService

Main monitoring service.

```python
from src.monitoring import MonitoringService

monitoring = MonitoringService(config, db_service)
await monitoring.start()

# Get health status
health = await monitoring.health_checker.run_health_checks()

# Get metrics
metrics_text = monitoring.metrics.get_metrics_text()
```

#### Methods

##### `start() -> None`
Start monitoring services.

##### `stop() -> None`
Stop monitoring services.

##### `record_message_consumed(topic: str, partition: int) -> None`
Record message consumption.

##### `record_message_processed(topic: str, operation_type: str) -> None`
Record successful message processing.

##### `record_message_failed(topic: str, error_type: str) -> None`
Record failed message processing.

##### `record_batch_processed(duration_seconds: float) -> None`
Record batch processing metrics.

### MetricsCollector

Prometheus metrics collector.

```python
from src.monitoring import MetricsCollector

metrics = MetricsCollector()

# Record metrics
metrics.record_message_processed("topic", "INSERT")
metrics.record_batch_processed(1.25)

# Get metrics
metrics_text = metrics.get_metrics_text()
```

### HealthChecker

Health check manager.

```python
from src.monitoring import HealthChecker

checker = HealthChecker(db_service)

# Run health checks
health = await checker.run_health_checks()

# Add custom check
checker.add_check(my_health_check_function)
```

## REST API

### Health Endpoints

#### `GET /health`

Get system health status.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2023-12-01T10:00:00Z",
  "checks": {
    "database": {
      "status": "healthy",
      "connection_pool": {
        "size": 10,
        "checked_in": 8,
        "checked_out": 2
      }
    }
  }
}
```

#### `GET /ready`

Get system readiness status.

**Response:** Same as `/health`

### Metrics Endpoint

#### `GET /metrics`

Get Prometheus metrics.

**Response:** Prometheus text format metrics.

### Error Responses

All endpoints return appropriate HTTP status codes:

- `200`: Success
- `503`: Service unhealthy
- `500`: Internal server error

## Error Handling

### Exceptions

#### `TransformationError`
Raised when message transformation fails.

#### `ValidationError`
Raised when message validation fails.

#### `RepositoryError`
Base class for repository errors.

#### `DuplicateKeyError`
Raised when unique constraints are violated.

#### `ConnectionError`
Raised when database connections fail.

### Error Handling Patterns

```python
from src.database import RepositoryError, DuplicateKeyError

try:
    await audit_repo.create_audit_log(...)
except DuplicateKeyError:
    # Handle duplicate key violation
    pass
except RepositoryError as e:
    # Handle general repository errors
    logger.error(f"Repository error: {e}")
    raise
```

## Type Definitions

### OperationTypeEnum

```python
from src.database import OperationTypeEnum

OperationTypeEnum.INSERT
OperationTypeEnum.UPDATE
OperationTypeEnum.DELETE
```

### Environment

```python
from src.config import Environment

Environment.DEVELOPMENT
Environment.STAGING
Environment.PRODUCTION
```

## Constants

### Default Values

```python
# Consumer defaults
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_BATCH_TIMEOUT = 5.0

# Database defaults
DEFAULT_POOL_SIZE = 10
DEFAULT_MAX_OVERFLOW = 20

# Monitoring defaults
DEFAULT_METRICS_PORT = 8000
DEFAULT_HEALTH_CHECK_PORT = 8001
```

## Examples

### Complete Consumer Setup

```python
from src.config import app_config
from src.database import DatabaseService, get_database_manager
from src.consumer import CDCConsumerService
from src.monitoring import MonitoringService

# Initialize services
db_manager = await get_database_manager(app_config.sink_database)
db_service = DatabaseService(db_manager, app_config.consumer)

monitoring = MonitoringService(app_config.monitoring, db_service)
await monitoring.start()

# Create and start consumer
consumer = CDCConsumerService(
    kafka_config=app_config.kafka,
    consumer_config=app_config.consumer,
    database_service=db_service
)

await consumer.start()
```

### Custom Message Processing

```python
from src.consumer import MessageTransformer, transform_cdc_message

# Create custom transformer
transformer = MessageTransformer(
    validate_schema=True,
    sanitize_data=True
)

# Process single message
transformed = transform_cdc_message(message, topic, partition, offset)

# Process batch
messages = [(msg, topic, part, off) for msg, topic, part, off in batch]
transformed_batch = transformer.transform_batch(messages)
```

### Repository Usage

```python
from src.database import get_repository_session, OperationTypeEnum

async with get_repository_session() as (audit_repo, offset_repo, error_repo):
    # Create audit log
    audit_log = await audit_repo.create_audit_log(
        source_table="users",
        operation_type=OperationTypeEnum.INSERT,
        new_data={"id": 1, "email": "user@example.com"}
    )

    # Query with filters
    recent_changes = await audit_repo.get_audit_logs(
        source_table="users",
        start_date=datetime.utcnow() - timedelta(days=1),
        limit=50
    )

    # Get statistics
    stats = await audit_repo.get_audit_stats(source_table="users", days=7)
    print(f"Total changes: {stats['total_changes']}")
```