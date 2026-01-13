"""
Configuration loader utilities.

Provides functions to load configuration from various sources:
- Environment variables (primary)
- Configuration files (YAML/JSON)
- Default values (fallback)
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from .settings import AppConfig


class ConfigLoader:
    """Configuration loader with support for multiple sources."""

    def __init__(self):
        self.config_paths = [
            Path.cwd() / "config" / "app.yaml",
            Path.cwd() / "config" / "app.json",
            Path.home() / ".cdc_audit" / "config.yaml",
            Path.home() / ".cdc_audit" / "config.json",
        ]

    def load_from_file(self, config_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Load configuration from file.

        Args:
            config_path: Specific config file path, or None to try defaults

        Returns:
            Configuration dictionary from file, or empty dict if not found
        """
        if config_path:
            paths_to_try = [config_path]
        else:
            paths_to_try = self.config_paths

        for path in paths_to_try:
            if path.exists():
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        if path.suffix.lower() in ['.yaml', '.yml']:
                            return yaml.safe_load(f) or {}
                        elif path.suffix.lower() == '.json':
                            return json.load(f) or {}
                except Exception as e:
                    print(f"Warning: Failed to load config from {path}: {e}")
                    continue

        return {}

    def merge_configs(self, file_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge file configuration with environment variables.

        Environment variables take precedence over file config.
        """
        merged = dict(file_config)

        # Environment variable mappings
        env_mappings = {
            'ENVIRONMENT': 'environment',
            'DEBUG': 'debug',
            'APP_NAME': 'app_name',
            'APP_VERSION': 'version',
            # Kafka settings
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka.bootstrap_servers',
            'KAFKA_GROUP_ID': 'kafka.group_id',
            'KAFKA_AUTO_OFFSET_RESET': 'kafka.auto_offset_reset',
            'KAFKA_ENABLE_AUTO_COMMIT': 'kafka.enable_auto_commit',
            'KAFKA_SESSION_TIMEOUT_MS': 'kafka.session_timeout_ms',
            'KAFKA_HEARTBEAT_INTERVAL_MS': 'kafka.heartbeat_interval_ms',
            'KAFKA_MAX_POLL_RECORDS': 'kafka.max_poll_records',
            'KAFKA_FETCH_MIN_BYTES': 'kafka.fetch_min_bytes',
            'KAFKA_FETCH_MAX_WAIT_MS': 'kafka.fetch_max_wait_ms',
            'KAFKA_CONSUMER_TIMEOUT_MS': 'kafka.consumer_timeout_ms',
            'KAFKA_MAX_EMPTY_POLLS': 'kafka.max_empty_polls',
            'KAFKA_IDLE_THRESHOLD_SECONDS': 'kafka.idle_threshold_seconds',
            # Database settings
            'SINK_DB_HOST': 'sink_database.host',
            'SINK_DB_PORT': 'sink_database.port',
            'SINK_DB_USERNAME': 'sink_database.username',
            'SINK_DB_PASSWORD': 'sink_database.password',
            'SINK_DB_NAME': 'sink_database.database',
            'SINK_DB_SCHEMA': 'sink_database.schema',
            'DB_POOL_SIZE': 'sink_database.pool_size',
            'DB_MAX_OVERFLOW': 'sink_database.max_overflow',
            'DB_POOL_TIMEOUT': 'sink_database.pool_timeout',
            'DB_POOL_RECYCLE': 'sink_database.pool_recycle',
            'DB_SSL_MODE': 'sink_database.ssl_mode',
            'DB_SSL_CERT': 'sink_database.ssl_cert',
            'DB_SSL_KEY': 'sink_database.ssl_key',
            'DB_SSL_ROOT_CERT': 'sink_database.ssl_root_cert',
            # Source database settings
            'SOURCE_DB_HOST': 'source_database.host',
            'SOURCE_DB_PORT': 'source_database.port',
            'SOURCE_DB_USERNAME': 'source_database.username',
            'SOURCE_DB_PASSWORD': 'source_database.password',
            'SOURCE_DB_NAME': 'source_database.database',
            # Logging settings
            'LOG_LEVEL': 'logging.level',
            'LOG_FORMAT': 'logging.format',
            'LOG_DATE_FORMAT': 'logging.date_format',
            'LOG_TO_FILE': 'logging.log_to_file',
            'LOG_FILE_PATH': 'logging.log_file_path',
            'LOG_MAX_FILE_SIZE': 'logging.max_file_size',
            'LOG_BACKUP_COUNT': 'logging.backup_count',
            'LOG_STRUCTURED': 'logging.structured',
            # Monitoring settings
            'MONITORING_ENABLED': 'monitoring.enabled',
            'METRICS_PORT': 'monitoring.metrics_port',
            'HEALTH_CHECK_PORT': 'monitoring.health_check_port',
            'PROMETHEUS_ENABLED': 'monitoring.prometheus_enabled',
            'PROMETHEUS_PATH': 'monitoring.prometheus_path',
            'COLLECT_CONSUMER_METRICS': 'monitoring.collect_consumer_metrics',
            'COLLECT_DATABASE_METRICS': 'monitoring.collect_database_metrics',
            'COLLECT_SYSTEM_METRICS': 'monitoring.collect_system_metrics',
            # Consumer settings
            'CDC_TOPICS': 'consumer.topics',
            'CDC_BATCH_SIZE': 'consumer.batch_size',
            'CDC_BATCH_TIMEOUT': 'consumer.batch_timeout_seconds',
            'CDC_MAX_RETRIES': 'consumer.max_retries',
            'CDC_RETRY_DELAY': 'consumer.retry_delay_seconds',
            'CDC_ENABLE_DLQ': 'consumer.enable_dlq',
            'CDC_DLQ_TOPIC': 'consumer.dlq_topic',
            'CDC_MAX_PROCESSING_TIME': 'consumer.max_processing_time_seconds',
        }

        # Override with environment variables
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                self._set_nested_value(merged, config_path, env_value)

        return merged

    def _set_nested_value(self, config: Dict[str, Any], path: str, value: Any) -> None:
        """Set a value in a nested dictionary using dot notation."""
        keys = path.split('.')
        current = config

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Type conversion for known fields
        final_key = keys[-1]
        if final_key in ['port', 'pool_size', 'max_overflow', 'pool_timeout', 'pool_recycle',
                        'max_file_size', 'backup_count', 'metrics_port', 'health_check_port',
                        'batch_size', 'max_retries', 'max_processing_time_seconds']:
            try:
                current[final_key] = int(value)
            except ValueError:
                current[final_key] = value
        elif final_key in ['debug', 'enable_auto_commit', 'log_to_file', 'structured', 'enabled',
                          'prometheus_enabled', 'collect_consumer_metrics', 'collect_database_metrics',
                          'collect_system_metrics', 'enable_dlq']:
            current[final_key] = str(value).lower() in ('true', '1', 'yes', 'on')
        elif final_key in ['batch_timeout_seconds', 'retry_delay_seconds']:
            try:
                current[final_key] = float(value)
            except ValueError:
                current[final_key] = value
        elif final_key == 'bootstrap_servers':
            current[final_key] = [s.strip() for s in str(value).split(',')]
        elif final_key == 'topics':
            current[final_key] = [t.strip() for t in str(value).split(',') if t.strip()]
        else:
            current[final_key] = value

    def load_config(self, config_path: Optional[Path] = None) -> AppConfig:
        """
        Load and create AppConfig from available sources.

        Args:
            config_path: Optional specific config file path

        Returns:
            Fully configured AppConfig instance
        """
        file_config = self.load_from_file(config_path)
        merged_config = self.merge_configs(file_config)

        # Create AppConfig from merged configuration
        # This is a simplified approach - in production you might want
        # a more sophisticated mapping or use a library like pydantic
        config = AppConfig.from_env()

        # Override with merged values (simplified implementation)
        # In a real implementation, you'd want to map the flat dict to nested config

        return config


def load_configuration(config_path: Optional[Path] = None) -> AppConfig:
    """
    Convenience function to load application configuration.

    Args:
        config_path: Optional path to configuration file

    Returns:
        Configured AppConfig instance
    """
    loader = ConfigLoader()
    return loader.load_config(config_path)


# Example configuration file templates
DEFAULT_CONFIG_YAML = """
environment: development
debug: false

kafka:
  bootstrap_servers:
    - localhost:29092
  group_id: cdc_audit_consumer
  auto_offset_reset: earliest
  enable_auto_commit: false

sink_database:
  host: localhost
  port: 5432
  username: admin
  password: admin
  database: sink_db
  schema: public
  pool_size: 10
  max_overflow: 20

source_database:
  host: localhost
  port: 5432
  username: admin
  password: admin
  database: source_db

logging:
  level: INFO
  structured: false

monitoring:
  enabled: true
  metrics_port: 8000
  health_check_port: 8001

consumer:
  topics:
    - source_db.public.people
    - source_db.public.roles
  batch_size: 100
  max_retries: 3
"""

DEFAULT_CONFIG_JSON = json.dumps(yaml.safe_load(DEFAULT_CONFIG_YAML), indent=2)