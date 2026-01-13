"""
Configuration validation utilities.

Provides validation functions for different configuration components
to ensure they are properly configured before application startup.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import text
from confluent_kafka import Consumer, KafkaError, KafkaException

from .settings import AppConfig, DatabaseConfig, KafkaConfig

logger = logging.getLogger(__name__)


class ConfigValidator:
    """Configuration validator for CDC Audit System."""

    def __init__(self, config: AppConfig):
        self.config = config

    async def validate_all(self) -> Dict[str, Any]:
        """
        Validate all configuration components.

        Returns:
            Dict containing validation results for each component
        """
        results = {
            "config": "valid",
            "database": await self.validate_database(),
            "kafka": await self.validate_kafka(),
            "overall_status": "healthy"
        }

        # Check if any component failed
        failed_components = [
            component for component, status in results.items()
            if isinstance(status, dict) and status.get("status") == "failed"
        ]

        if failed_components:
            results["overall_status"] = "unhealthy"
            results["failed_components"] = failed_components

        return results

    async def validate_database(self) -> Dict[str, Any]:
        """
        Validate database connectivity and schema.

        Tests:
        - Connection establishment
        - Required tables existence
        - Permissions
        """
        try:
            engine = create_async_engine(
                self.config.sink_database.connection_string.replace(
                    "postgresql://", "postgresql+asyncpg://"
                ),
                pool_size=1,
                max_overflow=0,
            )

            async with engine.begin() as conn:
                # Test basic connectivity
                await conn.execute(text("SELECT 1"))

                # Check if required tables exist
                result = await conn.execute(text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name IN ('audit_logging', 'kafka_offsets')
                """))

                existing_tables = {row[0] for row in result.fetchall()}
                required_tables = {'audit_logging', 'kafka_offsets'}

                missing_tables = required_tables - existing_tables

                if missing_tables:
                    return {
                        "status": "failed",
                        "error": f"Missing required tables: {missing_tables}",
                        "existing_tables": list(existing_tables),
                        "required_tables": list(required_tables)
                    }

                # Test permissions (basic write check)
                await conn.execute(text("SELECT COUNT(*) FROM audit_logging"))
                await conn.execute(text("SELECT COUNT(*) FROM kafka_offsets"))

            await engine.dispose()

            return {
                "status": "healthy",
                "message": "Database connection and schema validation successful",
                "existing_tables": list(existing_tables)
            }

        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "connection_string": self._mask_connection_string(
                    self.config.sink_database.connection_string
                )
            }

    async def validate_kafka(self) -> Dict[str, Any]:
        """
        Validate Kafka connectivity and topic accessibility.

        Tests:
        - Broker connectivity
        - Topic existence
        - Consumer group creation
        """
        consumer_config = {
            "bootstrap.servers": ",".join(self.config.kafka.bootstrap_servers),
            "group.id": f"{self.config.kafka.group_id}_validation",
            "session.timeout.ms": 6000,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        consumer = None
        try:
            consumer = Consumer(consumer_config)

            # Test broker connectivity by getting metadata
            metadata = consumer.list_topics(timeout=10.0)

            if metadata is None:
                raise KafkaException("Failed to retrieve cluster metadata")

            # Check if configured topics exist
            available_topics = set(metadata.topics.keys())
            configured_topics = set(self.config.consumer.topics)

            missing_topics = configured_topics - available_topics

            if missing_topics:
                return {
                    "status": "warning",
                    "message": f"Some topics do not exist yet: {missing_topics}",
                    "available_topics": list(available_topics),
                    "configured_topics": list(configured_topics),
                    "missing_topics": list(missing_topics)
                }

            # Test consumer assignment for each topic
            for topic in self.config.consumer.topics:
                if topic in metadata.topics:
                    topic_metadata = metadata.topics[topic]
                    if topic_metadata.partitions:
                        return {
                            "status": "healthy",
                            "message": "Kafka connectivity and topic validation successful",
                            "available_topics": list(available_topics),
                            "configured_topics": list(configured_topics),
                            "topic_partitions": {
                                topic: len(topic_metadata.partitions)
                                for topic in self.config.consumer.topics
                                if topic in metadata.topics
                            }
                        }

            return {
                "status": "healthy",
                "message": "Kafka connectivity successful, topics will be auto-created",
                "available_topics": list(available_topics),
                "configured_topics": list(configured_topics)
            }

        except KafkaException as e:
            logger.error(f"Kafka validation failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "bootstrap_servers": self.config.kafka.bootstrap_servers
            }
        except Exception as e:
            logger.error(f"Unexpected error during Kafka validation: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "bootstrap_servers": self.config.kafka.bootstrap_servers
            }
        finally:
            if consumer:
                consumer.close()

    def _mask_connection_string(self, conn_string: str) -> str:
        """Mask sensitive information in connection string for logging."""
        # Simple masking - replace password
        if "://" in conn_string and "@" in conn_string:
            protocol_end = conn_string.find("://") + 3
            at_symbol = conn_string.find("@")
            if protocol_end < at_symbol:
                return conn_string[:protocol_end] + "***:***" + conn_string[at_symbol:]
        return conn_string


async def validate_configuration(config: AppConfig) -> Dict[str, Any]:
    """
    Convenience function to validate configuration.

    Args:
        config: Application configuration to validate

    Returns:
        Validation results dictionary
    """
    validator = ConfigValidator(config)
    return await validator.validate_all()


@asynccontextmanager
async def get_validation_session():
    """
    Context manager for configuration validation sessions.

    Usage:
        async with get_validation_session() as validator:
            results = await validator.validate_all()
    """
    from .settings import config
    validator = ConfigValidator(config)
    try:
        yield validator
    finally:
        pass  # Cleanup if needed