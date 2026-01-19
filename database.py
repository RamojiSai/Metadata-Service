"""Database functions - All CRUD operations"""

import os
from typing import List, Optional, Dict
from sqlalchemy import create_engine, Column as SQLColumn, Integer, String, ForeignKey, Table, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from dotenv import load_dotenv
from log import log_info, log_error, log_exception
from Dbconfig import DATASETS_TABLE, COLUMNS_TABLE, LINEAGE_TABLE

load_dotenv()

# Database URL
DB_USER = os.getenv("DB_USER", "metadata_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "metadata_password")
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "metadata_db")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# SQLAlchemy Models
class Dataset(Base):
    """Dataset model"""
    __tablename__ = DATASETS_TABLE

    id = SQLColumn(Integer, primary_key=True, index=True)
    fqn = SQLColumn(String(500), unique=True, nullable=False, index=True)
    connection_name = SQLColumn(String(100), nullable=False, index=True)
    database_name = SQLColumn(String(100), nullable=False, index=True)
    schema_name = SQLColumn(String(100), nullable=False, index=True)
    table_name = SQLColumn(String(100), nullable=False, index=True)
    source_type = SQLColumn(String(50), nullable=False)

    # Relationships
    columns = relationship("DatasetColumn", back_populates="dataset", cascade="all, delete-orphan")
    upstream_lineages = relationship(
        "Lineage", foreign_keys="Lineage.downstream_id", back_populates="downstream"
    )
    downstream_lineages = relationship(
        "Lineage", foreign_keys="Lineage.upstream_id", back_populates="upstream"
    )


class DatasetColumn(Base):
    """Column model"""
    __tablename__ = COLUMNS_TABLE

    id = SQLColumn(Integer, primary_key=True, index=True)
    dataset_id = SQLColumn(Integer, ForeignKey(f"{DATASETS_TABLE}.id"), nullable=False)
    name = SQLColumn(String(100), nullable=False, index=True)
    type = SQLColumn(String(50), nullable=False)

    # Relationships
    dataset = relationship("Dataset", back_populates="columns")


class Lineage(Base):
    """Lineage model"""
    __tablename__ = LINEAGE_TABLE

    id = SQLColumn(Integer, primary_key=True, index=True)
    upstream_id = SQLColumn(Integer, ForeignKey(f"{DATASETS_TABLE}.id"), nullable=False)
    downstream_id = SQLColumn(Integer, ForeignKey(f"{DATASETS_TABLE}.id"), nullable=False)

    # Relationships
    upstream = relationship("Dataset", foreign_keys=[upstream_id], back_populates="downstream_lineages")
    downstream = relationship("Dataset", foreign_keys=[downstream_id], back_populates="upstream_lineages")


def init_db():
    """Initialize database - create all tables"""
    try:
        Base.metadata.create_all(bind=engine)
        log_info("Database initialized successfully")
    except Exception as e:
        log_error("DatabaseInitError", f"Failed to initialize database: {str(e)}")
        raise


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# CRUD Operations for Datasets
@log_exception
def create_dataset(db: Session, fqn: str, source_type: str, columns: List[Dict]) -> Dataset:
    """Create a new dataset with columns"""
    # Parse FQN
    parts = fqn.split('.')
    connection_name, database_name, schema_name, table_name = parts

    # Create dataset
    dataset = Dataset(
        fqn=fqn,
        connection_name=connection_name,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        source_type=source_type
    )

    db.add(dataset)
    db.flush()  # Get the dataset ID

    # Create columns
    for col in columns:
        column = DatasetColumn(
            dataset_id=dataset.id,
            name=col["name"],
            type=col["type"]
        )
        db.add(column)

    db.commit()
    db.refresh(dataset)
    log_info(f"Dataset created: {fqn}")
    return dataset


@log_exception
def get_dataset_by_fqn(db: Session, fqn: str) -> Optional[Dataset]:
    """Get dataset by FQN"""
    return db.query(Dataset).filter(Dataset.fqn == fqn).first()


@log_exception
def get_dataset_by_id(db: Session, dataset_id: int) -> Optional[Dataset]:
    """Get dataset by ID"""
    return db.query(Dataset).filter(Dataset.id == dataset_id).first()


@log_exception
def get_all_datasets(db: Session) -> List[Dataset]:
    """Get all datasets"""
    return db.query(Dataset).all()


# CRUD Operations for Lineage
@log_exception
def create_lineage(db: Session, upstream_id: int, downstream_id: int) -> Lineage:
    """Create lineage relationship"""
    lineage = Lineage(upstream_id=upstream_id, downstream_id=downstream_id)
    db.add(lineage)
    db.commit()
    db.refresh(lineage)
    log_info(f"Lineage created: {upstream_id} -> {downstream_id}")
    return lineage


@log_exception
def get_upstream_datasets(db: Session, dataset_id: int) -> List[Dataset]:
    """Get all upstream datasets"""
    lineages = db.query(Lineage).filter(Lineage.downstream_id == dataset_id).all()
    return [lineage.upstream for lineage in lineages]


@log_exception
def get_downstream_datasets(db: Session, dataset_id: int) -> List[Dataset]:
    """Get all downstream datasets"""
    lineages = db.query(Lineage).filter(Lineage.upstream_id == dataset_id).all()
    return [lineage.downstream for lineage in lineages]


@log_exception
def get_all_lineages(db: Session) -> List[Lineage]:
    """Get all lineage relationships"""
    return db.query(Lineage).all()


# Search Operations
@log_exception
def search_datasets(db: Session, query: str) -> List[tuple]:
    """
    Search datasets by query string
    Returns list of tuples: (dataset, match_type, priority)
    """
    query_lower = query.lower()
    results = []

    all_datasets = db.query(Dataset).all()

    for dataset in all_datasets:
        # Check table name match
        if query_lower in dataset.table_name.lower():
            results.append((dataset, "table_name", 1))
            continue

        # Check column name match
        column_match = False
        for col in dataset.columns:
            if query_lower in col.name.lower():
                results.append((dataset, "column_name", 2))
                column_match = True
                break
        if column_match:
            continue

        # Check schema name match
        if query_lower in dataset.schema_name.lower():
            results.append((dataset, "schema_name", 3))
            continue

        # Check database name match
        if query_lower in dataset.database_name.lower():
            results.append((dataset, "database_name", 4))
            continue

    # Sort by priority
    results.sort(key=lambda x: x[2])
    return results