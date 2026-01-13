"""
Configuration package for CDC Audit System.

This package provides centralized configuration management with:
- Environment variable support
- Configuration file loading (YAML/JSON)
- Type-safe configuration classes
- Validation utilities
- Multiple deployment environment support

Key improvements over original:
- No hardcoded values in application code
- Type safety and validation
- Environment-specific configurations
- Centralized configuration management
- Proper defaults and documentation
"""

from .settings import (
    AppConfig,
    KafkaConfig,
    DatabaseConfig,
    LoggingConfig,
    MonitoringConfig,
    ConsumerConfig,
    Environment,
    OperationType,
    config as app_config
)

from .validator import (
    ConfigValidator,
    validate_configuration,
    get_validation_session
)

from .loader import (
    ConfigLoader,
    load_configuration,
    DEFAULT_CONFIG_YAML,
    DEFAULT_CONFIG_JSON
)

__all__ = [
    # Main configuration classes
    "AppConfig",
    "KafkaConfig",
    "DatabaseConfig",
    "LoggingConfig",
    "MonitoringConfig",
    "ConsumerConfig",
    "Environment",
    "OperationType",

    # Global config instance
    "app_config",

    # Validation utilities
    "ConfigValidator",
    "validate_configuration",
    "get_validation_session",

    # Loading utilities
    "ConfigLoader",
    "load_configuration",
    "DEFAULT_CONFIG_YAML",
    "DEFAULT_CONFIG_JSON",
]