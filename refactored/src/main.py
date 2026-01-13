"""
Main entry point for CDC Audit Consumer.

This module provides:
- Application initialization and configuration
- Service orchestration
- Health checks and monitoring endpoints
- Graceful shutdown handling

Key improvements over original:
- Proper application lifecycle management
- Health check endpoints
- Monitoring integration
- Configuration validation
- Structured logging
- Error handling and recovery
"""

import asyncio
import sys

from .config import app_config, validate_configuration
from .database import get_database_manager, DatabaseService, ensure_database_up_to_date
from .consumer import run_consumer_service
from .monitoring import MonitoringService
from .core.logging import setup_logging

logger = None  # Will be set after logging setup


class CDCAuditApplication:
    """
    Main CDC Audit application.

    Orchestrates all services and manages application lifecycle.
    """

    def __init__(self):
        self.config = app_config
        self.database_manager = None
        self.database_service = None
        self.monitoring_service = None
        self.consumer_task = None

    async def initialize(self) -> None:
        """Initialize all application services."""
        logger.info("Initializing CDC Audit Application...")

        try:
            # Validate configuration
            logger.info("Validating configuration...")
            validation_result = await validate_configuration(self.config)

            if validation_result.get("overall_status") != "healthy":
                logger.error(f"Configuration validation failed: {validation_result}")
                sys.exit(1)

            logger.info("Configuration validation successful")

            # Initialize database
            logger.info("Initializing database...")
            self.database_manager = await get_database_manager(self.config.sink_database)

            # Ensure database is up to date
            migration_result = await ensure_database_up_to_date(self.config.sink_database)
            if not migration_result.get("is_up_to_date"):
                logger.error(f"Database migration failed: {migration_result}")
                sys.exit(1)

            logger.info("Database initialization successful")

            # Create database service
            self.database_service = DatabaseService(
                self.database_manager,
                self.config.consumer
            )

            # Initialize monitoring service
            if self.config.monitoring.enabled:
                logger.info("Initializing monitoring service...")
                self.monitoring_service = MonitoringService(
                    self.config.monitoring,
                    self.database_service
                )
                await self.monitoring_service.start()
                logger.info("Monitoring service initialized")

            logger.info("CDC Audit Application initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            await self.cleanup()
            sys.exit(1)

    async def start_consumer(self) -> None:
        """Start the CDC consumer service."""
        logger.info("Starting CDC consumer...")

        try:
            await run_consumer_service(
                self.config.kafka,
                self.config.consumer,
                self.database_service
            )
        except Exception as e:
            logger.error(f"Consumer service failed: {e}")
            raise

    async def run(self) -> None:
        """Run the complete application."""
        await self.initialize()

        try:
            await self.start_consumer()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Application error: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Clean up all resources."""
        logger.info("Cleaning up application resources...")

        # Stop monitoring service
        if self.monitoring_service:
            await self.monitoring_service.stop()

        # Close database connections
        if self.database_manager:
            await self.database_manager.close()

        logger.info("Application cleanup completed")


async def main() -> None:
    """Main application entry point."""
    global logger

    # Set up logging
    setup_logging(app_config.logging, app_config.environment)

    # Import logger after logging setup
    from .core.logging import logger as app_logger
    logger = app_logger

    logger.info(f"Starting CDC Audit Consumer v{app_config.version}")
    logger.info(f"Environment: {app_config.environment.value}")
    logger.info(f"Debug mode: {app_config.debug}")

    # Create and run application
    app = CDCAuditApplication()

    try:
        await app.run()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the application
    asyncio.run(main())