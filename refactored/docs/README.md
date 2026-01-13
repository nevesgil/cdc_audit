# CDC Audit System - Refactored

A comprehensive Change Data Capture (CDC) audit logging system built with Python, Debezium, Kafka, and PostgreSQL. This refactored version includes significant improvements in architecture, performance, monitoring, and maintainability.

## 🚀 Key Improvements

### Architecture & Design
- **Async Processing**: Full async/await support for better performance and scalability
- **Repository Pattern**: Clean separation of data access logic
- **Dependency Injection**: Modular components with clear interfaces
- **Configuration Management**: Centralized, type-safe configuration with validation

### Performance & Reliability
- **Batch Processing**: Configurable batch sizes for optimal throughput
- **Connection Pooling**: Efficient database connection management
- **Error Handling**: Comprehensive error handling with retry logic and dead letter queues
- **Health Monitoring**: Built-in health checks and metrics collection

### Monitoring & Observability
- **Structured Logging**: JSON logging with correlation IDs
- **Prometheus Metrics**: Rich metrics for monitoring and alerting
- **Performance Monitoring**: Automatic timing and performance tracking
- **Health Endpoints**: REST API for health status and diagnostics

### Developer Experience
- **Type Safety**: Full type hints throughout the codebase
- **Comprehensive Testing**: Unit and integration tests with high coverage
- **Docker Support**: Complete containerization with multi-stage builds
- **Documentation**: Extensive documentation and examples

## 📋 Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.12+ (for local development)
- 4GB RAM available
- 10GB disk space

### Start the System

```bash
# Clone and enter the refactored directory
cd refactored

# Copy environment configuration
cp docker/env.example docker/.env

# Start all services
docker-compose up -d

# Check health status
curl http://localhost:8001/health
```

### Monitor the System

```bash
# View logs
docker-compose logs -f cdc-consumer

# Access monitoring
open http://localhost:8000/metrics  # Prometheus metrics
open http://localhost:8080          # Kafka UI
open http://localhost:5050          # pgAdmin
```

## 🏗️ Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │     Kafka       │    │   Consumer      │
│   (Source DB)   │───▶│   (Message Bus) │───▶│   (Processor)   │
│                 │    │                 │    │                 │
│ • people table  │    │ • Debezium      │    │ • Transform     │
│ • roles table   │    │ • Topics        │    │ • Batch Proc.   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Monitoring    │    │   Metrics       │
│   (Sink DB)     │    │   & Health      │    │   & Logs        │
│                 │◀───│                 │    │                 │
│ • audit logs    │    │ • Health checks │    │ • Prometheus    │
│ • offsets       │    │ • REST API      │    │ • Structured    │
│ • errors        │    │                 │    │ • Correlation   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Change Capture**: Debezium monitors PostgreSQL WAL for changes
2. **Message Streaming**: Changes are published to Kafka topics
3. **Message Processing**: Consumer transforms and batches messages
4. **Data Persistence**: Audit logs are stored in the sink database
5. **Monitoring**: Health checks and metrics are collected continuously

### Key Design Patterns

- **Repository Pattern**: Data access layer abstraction
- **Observer Pattern**: Event-driven processing
- **Factory Pattern**: Component instantiation
- **Strategy Pattern**: Configurable processing strategies
- **Decorator Pattern**: Cross-cutting concerns (logging, metrics)

## ⚙️ Configuration

### Environment Variables

The system uses environment variables for configuration with sensible defaults:

```bash
# Application
ENVIRONMENT=production
DEBUG=false
APP_NAME=cdc-audit-consumer

# Database
SINK_DB_HOST=postgres
SINK_DB_PORT=5432
SINK_DB_USERNAME=admin
SINK_DB_PASSWORD=admin
SINK_DB_NAME=sink_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_GROUP_ID=cdc_audit_consumer
KAFKA_AUTO_OFFSET_RESET=earliest

# Consumer
CDC_TOPICS=source_db.public.people,source_db.public.roles
CDC_BATCH_SIZE=100
CDC_MAX_RETRIES=3

# Monitoring
MONITORING_ENABLED=true
METRICS_PORT=8000
HEALTH_CHECK_PORT=8001
```

### Configuration Validation

The system validates configuration at startup:

```python
from src.config import app_config, validate_configuration

# Configuration is automatically validated on import
config = app_config
validation_result = await validate_configuration(config)
```

## 📚 API Reference

### Core Classes

#### Consumer Service

```python
from src.consumer import CDCConsumerService

# Create consumer service
consumer = CDCConsumerService(
    kafka_config=kafka_config,
    consumer_config=consumer_config,
    database_service=db_service
)

# Start processing
await consumer.start()

# Get metrics
metrics = consumer.get_metrics()
```

#### Database Repository

```python
from src.database import get_repository_session

async with get_repository_session() as (audit_repo, offset_repo, error_repo):
    # Create audit log
    audit_log = await audit_repo.create_audit_log(
        source_table="people",
        operation_type=OperationTypeEnum.INSERT,
        new_data={"id": 1, "name": "John Doe"}
    )

    # Get audit logs with filtering
    logs = await audit_repo.get_audit_logs(
        source_table="people",
        limit=100
    )
```

#### Monitoring Service

```python
from src.monitoring import get_monitoring_service

monitoring = get_monitoring_service()

# Record metrics
monitoring.record_message_processed("topic", "INSERT")

# Get health status
health = await monitoring.health_checker.run_health_checks()
```

### REST API Endpoints

#### Health Check
```bash
GET /health
# Returns system health status

GET /ready
# Returns readiness status
```

#### Metrics
```bash
GET /metrics
# Returns Prometheus metrics
```

## 📊 Monitoring

### Health Checks

The system provides comprehensive health monitoring:

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
    },
    "kafka": {
      "status": "healthy",
      "topics_count": 2
    }
  }
}
```

### Metrics

Key metrics exposed via Prometheus:

- **Message Processing**: `cdc_messages_consumed_total`, `cdc_messages_processed_total`
- **Batch Performance**: `cdc_batch_processing_duration_seconds`
- **Database**: `cdc_db_connections_active`, `cdc_db_operation_duration_seconds`
- **System**: `cdc_system_cpu_usage_percent`, `cdc_system_memory_usage_bytes`

### Logging

Structured logging with correlation IDs:

```json
{
  "timestamp": "2023-12-01T10:00:00.123Z",
  "level": "INFO",
  "name": "cdc.consumer",
  "message": "Processed batch of 50 messages",
  "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
  "batch_size": 50,
  "processing_time_ms": 1250.5
}
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
python tests/run_tests.py all

# Run unit tests only
python tests/run_tests.py unit

# Run integration tests
python tests/run_tests.py integration

# Run with coverage
python tests/run_tests.py all --coverage

# Run specific test
python tests/run_tests.py specific tests/unit/test_transformer.py
```

### Test Categories

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **End-to-End Tests**: Test complete pipelines (requires Docker)

### Test Configuration

Tests use pytest with fixtures for:

- Database sessions (SQLite for speed)
- Mock Kafka messages
- Test configurations
- Temporary directories

## 🚀 Deployment

### Docker Deployment

```bash
# Production deployment
docker-compose up -d

# Development with hot reload
docker-compose --profile dev up -d

# Testing environment
docker-compose -f docker-compose.test.yml up -d
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cdc-consumer
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: consumer
        image: cdc-consumer:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster:9092"
        ports:
        - containerPort: 8001
        livenessProbe:
          httpGet:
            path: /health
            port: 8001
          initialDelaySeconds: 30
          periodSeconds: 10
```

### Environment-Specific Configuration

```bash
# Development
export ENVIRONMENT=development
export DEBUG=true
export LOG_LEVEL=DEBUG

# Staging
export ENVIRONMENT=staging
export MONITORING_ENABLED=true
export CDC_BATCH_SIZE=50

# Production
export ENVIRONMENT=production
export DEBUG=false
export LOG_LEVEL=INFO
export CDC_BATCH_SIZE=200
```

## 🔧 Troubleshooting

### Common Issues

#### Consumer Not Processing Messages

```bash
# Check consumer health
curl http://localhost:8001/health

# Check Kafka connectivity
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic source_db.public.people \
  --from-beginning \
  --max-messages 5
```

#### High Memory Usage

```bash
# Adjust batch size
export CDC_BATCH_SIZE=50

# Monitor memory usage
curl http://localhost:8000/metrics | grep memory
```

#### Database Connection Issues

```bash
# Check database connectivity
docker-compose exec postgres pg_isready -U admin -d database

# View connection pool status
curl http://localhost:8001/health | jq '.checks.database'
```

### Debugging

#### Enable Debug Logging

```bash
export LOG_LEVEL=DEBUG
export DEBUG=true
docker-compose up -d cdc-consumer
```

#### View Structured Logs

```bash
docker-compose logs cdc-consumer | jq '.'
```

#### Performance Profiling

```bash
# Enable performance logging
export LOG_LEVEL=DEBUG

# View performance metrics
curl http://localhost:8000/metrics | grep cdc_batch
```

## 🤝 Contributing

### Development Setup

```bash
# Clone repository
git clone <repository-url>
cd cdc-audit/refactored

# Set up virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r tests/requirements.txt

# Run tests
python tests/run_tests.py all --coverage

# Start development environment
docker-compose --profile dev up -d
```

### Code Quality

```bash
# Run code quality checks
python tests/run_tests.py quality

# Format code
black src/ tests/

# Type checking
mypy src/
```

### Adding New Features

1. Create feature branch
2. Add tests for new functionality
3. Implement feature with proper error handling
4. Update documentation
5. Ensure all tests pass
6. Submit pull request

### Commit Guidelines

```bash
# Format: type(scope): description
feat(consumer): add batch processing support
fix(database): resolve connection pool issue
docs(api): update configuration guide
test(transformer): add validation tests
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Debezium for change data capture
- Apache Kafka for messaging
- PostgreSQL for database
- FastAPI for async web framework
- SQLAlchemy for database ORM
- Prometheus for metrics collection

---

For more detailed information, see the individual component documentation in the `docs/` directory.