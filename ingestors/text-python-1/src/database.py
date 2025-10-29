import os
from contextlib import contextmanager
from sqlalchemy import create_engine, Column, String, Text, ForeignKey, Integer
from sqlalchemy.orm import sessionmaker, Session, relationship, declarative_base
from pgvector.sqlalchemy import Vector as PgVector

Base = declarative_base()


class DocumentVector(Base):
    __tablename__ = "Vectors"
    id = Column(String, primary_key=True)
    file_id = Column(String, ForeignKey("Sources.id"), nullable=False)
    embedding = Column(PgVector(1024))
    content = Column(Text)
    chunk_index = Column(Integer)
    source = relationship("Source", back_populates="vectors")


class Source(Base):
    __tablename__ = "Sources"
    id = Column(String, primary_key=True)
    status = Column(Text, nullable=False)
    ingestor_type = Column(String)
    ingestor_version = Column(String)
    vectors = relationship(
        "DocumentVector", back_populates="source", cascade="all, delete-orphan"
    )


def get_database_url():
    return f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"


engine = create_engine(get_database_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_session():
    """Provides a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
