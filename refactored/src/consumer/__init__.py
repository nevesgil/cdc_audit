"""
Consumer package for CDC Audit System.

This package provides:
- Async Kafka consumer with batch processing
- Message transformation and validation
- Comprehensive error handling and retry logic
- Consumer health monitoring and metrics
- Graceful shutdown handling

Key improvements over original:
- Async processing for better performance and scalability
- Batch processing to reduce database load and improve throughput
- Comprehensive error handling with dead letter queue support
- Proper offset management with exactly-once processing semantics
- Health monitoring and detailed metrics collection
- Graceful shutdown with proper cleanup
- Separation of concerns between consumption, transformation, and storage
"""

from .transformer import (
    TransformationError,
    ValidationError,
    TransformedMessage,
    MessageTransformer,
    MessageValidator,
    transform_cdc_message,
)

from .consumer import (
    ConsumerMetrics,
    BatchData,
    CDCConsumerService,
    create_consumer_service,
    run_consumer_service,
)

__all__ = [
    # Transformer classes
    "MessageTransformer",
    "MessageValidator",
    "TransformedMessage",

    # Transformer exceptions
    "TransformationError",
    "ValidationError",

    # Transformer functions
    "transform_cdc_message",

    # Consumer classes
    "ConsumerMetrics",
    "BatchData",
    "CDCConsumerService",

    # Consumer functions
    "create_consumer_service",
    "run_consumer_service",
]