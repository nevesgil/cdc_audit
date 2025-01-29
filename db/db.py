from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    JSON,
    Enum,
    DateTime,
    BigInteger,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import enum
import os

# Use environment variables for database credentials
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5432/sink_db")

engine = create_engine(DATABASE_URL)

Base = declarative_base()


# Enum for operation types
class OperationType(enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


# Audit Logging Table
class AuditLogging(Base):
    __tablename__ = "audit_logging"

    id = Column(Integer, primary_key=True)
    source_table = Column(String, nullable=False)
    operation_type = Column(Enum(OperationType), nullable=False)
    change_timestamp = Column(DateTime, default=func.now())
    old_data = Column(JSON)
    new_data = Column(JSON)
    change_user = Column(String)

    def __repr__(self):
        return (
            f"<AuditLogging(id={self.id}, source_table={self.source_table}, "
            f"operation_type={self.operation_type}, change_timestamp={self.change_timestamp})>"
        )


# Kafka Offset Tracking Table
class KafkaOffset(Base):
    __tablename__ = "kafka_offsets"

    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(BigInteger, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)

    # Unique constraint for topic-partition pairs
    __table_args__ = (UniqueConstraint("topic", "partition", name="unique_topic_partition"),)

    def __repr__(self):
        return (
            f"<KafkaOffset(id={self.id}, topic={self.topic}, partition={self.partition}, "
            f"offset={self.offset}, timestamp={self.timestamp})>"
        )


# Create tables in the database
Base.metadata.create_all(engine)

# Session setup
Session = sessionmaker(bind=engine)
