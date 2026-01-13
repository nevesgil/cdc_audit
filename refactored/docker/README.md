# CDC Audit System - Docker Setup

This directory contains Docker configurations for running the CDC Audit System in various environments.

## Quick Start

### Development Environment

1. **Copy environment file:**
   ```bash
   cp env.example .env
   # Edit .env with your preferred settings
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

3. **Start development consumer:**
   ```bash
   docker-compose --profile dev up -d cdc-consumer
   ```

### Production Environment

1. **Set environment variables:**
   ```bash
   export POSTGRES_USER=your_user
   export POSTGRES_PASSWORD=your_password
   # ... other variables
   ```

2. **Start services:**
   ```bash
   docker-compose up -d
   ```

## Available Services

### Core Services (always running)

- **zookeeper**: Apache ZooKeeper for Kafka coordination
- **kafka**: Apache Kafka message broker
- **kafka-ui**: Web interface for Kafka management (http://localhost:8080)
- **connect**: Debezium Connect for CDC connectors
- **postgres**: PostgreSQL database (source and sink)
- **pgadmin**: PostgreSQL administration interface (http://localhost:5050)
- **cdc-consumer**: CDC Audit Consumer application

### Development Services (`--profile dev`)

- **cdc-consumer**: Development version with hot reload
- **postgres-dev**: Additional PostgreSQL instance for development
- **prometheus**: Metrics collection (http://localhost:9090)
- **grafana**: Monitoring dashboards (http://localhost:3000)

### Test Services (`-f docker-compose.test.yml`)

- **postgres-test**: Isolated test database
- **kafka-test**: Isolated test Kafka cluster
- **cdc-consumer-test**: Test consumer instance

## Configuration

### Environment Variables

The system supports extensive configuration via environment variables. See `env.example` for all available options.

Key configuration areas:

- **Application**: `ENVIRONMENT`, `DEBUG`, `APP_NAME`
- **Database**: `POSTGRES_*`, `SINK_DB_*`, `SOURCE_DB_*`
- **Kafka**: `KAFKA_*` settings
- **Logging**: `LOG_*` settings
- **Monitoring**: `MONITORING_*`, `METRICS_*` settings
- **Consumer**: `CDC_*` settings

### Docker Compose Files

- `docker-compose.yml`: Base configuration for all environments
- `docker-compose.override.yml`: Development overrides (auto-loaded)
- `docker-compose.test.yml`: Test environment configuration

## Database Setup

The database is automatically initialized with:

- **source_db**: Contains `people` and `roles` tables with sample data
- **sink_db**: Contains audit logging tables (`audit_logging`, `kafka_offsets`, `processing_errors`)

Tables are created and populated via `init-db/01-init-databases.sql`.

## Monitoring and Health Checks

### Health Endpoints

- **Consumer Health**: http://localhost:8001/health
- **Consumer Readiness**: http://localhost:8001/ready
- **Consumer Metrics**: http://localhost:8000/metrics

### Service Health Checks

All services include Docker health checks that monitor:

- **zookeeper**: Port availability
- **kafka**: Broker API connectivity
- **connect**: HTTP endpoint availability
- **postgres**: Database connectivity
- **cdc-consumer**: Application health endpoint

## Usage Examples

### View Logs

```bash
# View consumer logs
docker-compose logs -f cdc-consumer

# View all service logs
docker-compose logs -f

# View specific service logs with timestamps
docker-compose logs -f --timestamps kafka
```

### Database Access

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U admin -d database

# Access pgAdmin
# Open http://localhost:5050
# Username: admin@pgadmin.com
# Password: admin
```

### Kafka Management

```bash
# Access Kafka UI
# Open http://localhost:8080

# List topics
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a topic manually
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic test-topic --partitions 3 --replication-factor 1
```

### Monitoring

```bash
# Access Prometheus (development only)
# Open http://localhost:9090

# Access Grafana (development only)
# Open http://localhost:3000
# Username: admin
# Password: admin
```

## Development Workflow

### Code Changes

1. **Mount source code** (already configured in dev profile)
2. **Enable hot reload** by setting `HOT_RELOAD=true`
3. **Restart consumer** when needed:
   ```bash
   docker-compose up -d cdc-consumer
   ```

### Testing

```bash
# Run tests in container
docker-compose -f docker-compose.test.yml run --rm cdc-consumer-test \
    python -m pytest tests/

# Run specific test
docker-compose -f docker-compose.test.yml run --rm cdc-consumer-test \
    python -m pytest tests/test_consumer.py -v
```

### Debugging

```bash
# Access running consumer container
docker-compose exec cdc-consumer bash

# View consumer configuration
docker-compose exec cdc-consumer python -c "from src.config import app_config; print(app_config)"

# Check consumer health
curl http://localhost:8001/health | jq
```

## Production Deployment

### Security Considerations

1. **Change default passwords** in `.env`
2. **Use secrets management** instead of environment variables
3. **Enable SSL/TLS** for database and Kafka connections
4. **Configure network isolation** between services
5. **Set up proper logging** aggregation

### Scaling

- **Kafka**: Add more brokers by copying kafka service configuration
- **Consumer**: Run multiple consumer instances with different group IDs
- **Database**: Use connection pooling and consider read replicas

### Backup and Recovery

```bash
# Backup database
docker-compose exec postgres pg_dump -U admin database > backup.sql

# Backup Kafka data
docker-compose exec kafka kafka-dump-log.sh --files /bitnami/kafka/data/*.log > kafka_backup.txt

# Restore database
docker-compose exec -T postgres psql -U admin database < backup.sql
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Change ports in `docker-compose.yml`
2. **Memory issues**: Increase Docker memory limits
3. **Slow startup**: Check service dependencies and health checks
4. **Connection refused**: Wait for services to be healthy

### Logs and Debugging

```bash
# View service health
docker-compose ps

# Check service health status
docker-compose exec cdc-consumer wget -qO- http://localhost:8001/health

# Debug Kafka connectivity
docker-compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic source_db.public.people \
    --from-beginning
```

### Performance Tuning

- Adjust `CDC_BATCH_SIZE` based on throughput requirements
- Configure `DB_POOL_SIZE` based on concurrent operations
- Monitor metrics endpoints for bottlenecks
- Use Grafana dashboards for visualization