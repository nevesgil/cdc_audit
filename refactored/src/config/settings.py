"""
Centralized configuration management for CDC Audit System.

This module provides:
- Environment variable validation and parsing
- Type-safe configuration classes
- Default value management
- Configuration validation
- Support for different deployment environments

Improvements over original:
- Single source of truth for all configuration
- Type safety and validation
- Environment-specific settings
- Proper defaults and documentation
- No hardcoded values in application code
"""

import os
import json
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class Environment(str, Enum):
    """Deployment environment enumeration."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class OperationType(str, Enum):
    """Database operation types."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class KafkaConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:29092"])
    group_id: str = "cdc_audit_consumer"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    max_poll_records: int = 500
    fetch_min_bytes: int = 1024
    fetch_max_wait_ms: int = 500

    # Consumer-specific settings
    consumer_timeout_ms: int = 5000
    max_empty_polls: int = 5
    idle_threshold_seconds: int = 30

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Create Kafka config from environment variables."""
        return cls(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
            ).split(","),
            group_id=os.getenv("KAFKA_GROUP_ID", "cdc_audit_consumer"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true",
            session_timeout_ms=int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000")),
            heartbeat_interval_ms=int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "3000")),
            max_poll_records=int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500")),
            fetch_min_bytes=int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1024")),
            fetch_max_wait_ms=int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "500")),
            consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "5000")),
            max_empty_polls=int(os.getenv("KAFKA_MAX_EMPTY_POLLS", "5")),
            idle_threshold_seconds=int(os.getenv("KAFKA_IDLE_THRESHOLD_SECONDS", "30")),
        )


@dataclass
class DatabaseConfig:
    """Database configuration for both source and sink databases."""
    host: str = "localhost"
    port: int = 5432
    username: str = "admin"
    password: str = "admin"
    database: str = "sink_db"
    schema: str = "public"

    # Connection pool settings
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600

    # SSL settings
    ssl_mode: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_root_cert: Optional[str] = None

    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        base_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        params = []
        if self.ssl_mode:
            params.append(f"sslmode={self.ssl_mode}")
        if self.ssl_cert:
            params.append(f"sslcert={self.ssl_cert}")
        if self.ssl_key:
            params.append(f"sslkey={self.ssl_key}")
        if self.ssl_root_cert:
            params.append(f"sslrootcert={self.ssl_root_cert}")

        if params:
            return f"{base_url}?{'&'.join(params)}"
        return base_url

    @classmethod
    def sink_from_env(cls) -> "DatabaseConfig":
        """Create sink database config from environment variables."""
        return cls(
            host=os.getenv("SINK_DB_HOST", "localhost"),
            port=int(os.getenv("SINK_DB_PORT", "5432")),
            username=os.getenv("SINK_DB_USERNAME", "admin"),
            password=os.getenv("SINK_DB_PASSWORD", "admin"),
            database=os.getenv("SINK_DB_NAME", "sink_db"),
            schema=os.getenv("SINK_DB_SCHEMA", "public"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "3600")),
            ssl_mode=os.getenv("DB_SSL_MODE"),
            ssl_cert=os.getenv("DB_SSL_CERT"),
            ssl_key=os.getenv("DB_SSL_KEY"),
            ssl_root_cert=os.getenv("DB_SSL_ROOT_CERT"),
        )

    @classmethod
    def source_from_env(cls) -> "DatabaseConfig":
        """Create source database config from environment variables."""
        return cls(
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "5432")),
            username=os.getenv("SOURCE_DB_USERNAME", "admin"),
            password=os.getenv("SOURCE_DB_PASSWORD", "admin"),
            database=os.getenv("SOURCE_DB_NAME", "source_db"),
            schema=os.getenv("SOURCE_DB_SCHEMA", "public"),
        )


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"

    # File logging
    log_to_file: bool = False
    log_file_path: Optional[str] = None
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

    # Structured logging (JSON)
    structured: bool = False

    @classmethod
    def from_env(cls) -> "LoggingConfig":
        """Create logging config from environment variables."""
        return cls(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            date_format=os.getenv("LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S"),
            log_to_file=os.getenv("LOG_TO_FILE", "false").lower() == "true",
            log_file_path=os.getenv("LOG_FILE_PATH"),
            max_file_size=int(os.getenv("LOG_MAX_FILE_SIZE", str(10 * 1024 * 1024))),
            backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
            structured=os.getenv("LOG_STRUCTURED", "false").lower() == "true",
        )


@dataclass
class MonitoringConfig:
    """Monitoring and metrics configuration."""
    enabled: bool = True
    metrics_port: int = 8000
    health_check_port: int = 8001

    # Prometheus metrics
    prometheus_enabled: bool = True
    prometheus_path: str = "/metrics"

    # Custom metrics
    collect_consumer_metrics: bool = True
    collect_database_metrics: bool = True
    collect_system_metrics: bool = True

    @classmethod
    def from_env(cls) -> "MonitoringConfig":
        """Create monitoring config from environment variables."""
        return cls(
            enabled=os.getenv("MONITORING_ENABLED", "true").lower() == "true",
            metrics_port=int(os.getenv("METRICS_PORT", "8000")),
            health_check_port=int(os.getenv("HEALTH_CHECK_PORT", "8001")),
            prometheus_enabled=os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true",
            prometheus_path=os.getenv("PROMETHEUS_PATH", "/metrics"),
            collect_consumer_metrics=os.getenv("COLLECT_CONSUMER_METRICS", "true").lower() == "true",
            collect_database_metrics=os.getenv("COLLECT_DATABASE_METRICS", "true").lower() == "true",
            collect_system_metrics=os.getenv("COLLECT_SYSTEM_METRICS", "true").lower() == "true",
        )


@dataclass
class ConsumerConfig:
    """CDC consumer specific configuration."""
    topics: List[str] = field(default_factory=lambda: ["source_db.public.people", "source_db.public.roles"])

    # Batch processing
    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0

    # Processing settings
    enable_dlq: bool = True
    dlq_topic: str = "cdc_audit_dlq"
    max_processing_time_seconds: int = 300

    @classmethod
    def from_env(cls) -> "ConsumerConfig":
        """Create consumer config from environment variables."""
        topics_str = os.getenv("CDC_TOPICS", "source_db.public.people,source_db.public.roles")
        topics = [topic.strip() for topic in topics_str.split(",") if topic.strip()]

        return cls(
            topics=topics,
            batch_size=int(os.getenv("CDC_BATCH_SIZE", "100")),
            batch_timeout_seconds=float(os.getenv("CDC_BATCH_TIMEOUT", "5.0")),
            max_retries=int(os.getenv("CDC_MAX_RETRIES", "3")),
            retry_delay_seconds=float(os.getenv("CDC_RETRY_DELAY", "1.0")),
            enable_dlq=os.getenv("CDC_ENABLE_DLQ", "true").lower() == "true",
            dlq_topic=os.getenv("CDC_DLQ_TOPIC", "cdc_audit_dlq"),
            max_processing_time_seconds=int(os.getenv("CDC_MAX_PROCESSING_TIME", "300")),
        )


@dataclass
class AppConfig:
    """Main application configuration."""
    environment: Environment = Environment.DEVELOPMENT
    debug: bool = False

    # Component configs
    kafka: KafkaConfig = field(default_factory=KafkaConfig.from_env)
    sink_database: DatabaseConfig = field(default_factory=DatabaseConfig.sink_from_env)
    source_database: DatabaseConfig = field(default_factory=DatabaseConfig.source_from_env)
    logging: LoggingConfig = field(default_factory=LoggingConfig.from_env)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig.from_env)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig.from_env)

    # Application settings
    app_name: str = "cdc-audit-consumer"
    version: str = "1.0.0"

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create application config from environment variables."""
        env_str = os.getenv("ENVIRONMENT", "development").lower()
        try:
            environment = Environment(env_str)
        except ValueError:
            environment = Environment.DEVELOPMENT

        return cls(
            environment=environment,
            debug=os.getenv("DEBUG", "false").lower() == "true",
            app_name=os.getenv("APP_NAME", "cdc-audit-consumer"),
            version=os.getenv("APP_VERSION", "1.0.0"),
        )

    def validate(self) -> None:
        """Validate configuration values."""
        if not self.kafka.bootstrap_servers:
            raise ValueError("At least one Kafka bootstrap server must be configured")

        if not self.sink_database.username or not self.sink_database.password:
            raise ValueError("Sink database credentials must be provided")

        if self.consumer.batch_size <= 0:
            raise ValueError("Consumer batch size must be positive")

        if self.consumer.max_retries < 0:
            raise ValueError("Max retries cannot be negative")

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging/serialization."""
        return {
            "environment": self.environment.value,
            "debug": self.debug,
            "app_name": self.app_name,
            "version": self.version,
            "kafka": {
                "bootstrap_servers": self.kafka.bootstrap_servers,
                "group_id": self.kafka.group_id,
                "topics_count": len(self.consumer.topics),
            },
            "databases": {
                "sink": {
                    "host": self.sink_database.host,
                    "port": self.sink_database.port,
                    "database": self.sink_database.database,
                },
                "source": {
                    "host": self.source_database.host,
                    "port": self.source_database.port,
                    "database": self.source_database.database,
                },
            },
            "consumer": {
                "topics": self.consumer.topics,
                "batch_size": self.consumer.batch_size,
                "max_retries": self.consumer.max_retries,
            },
        }


# Global configuration instance
config = AppConfig.from_env()
config.validate()