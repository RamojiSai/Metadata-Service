"""Pydantic models for input validation and API schemas"""

from typing import List, Optional
from pydantic import BaseModel, Field, validator
from Dbconfig import SOURCE_SYSTEMS


class ColumnSchema(BaseModel):
    """Schema for dataset column"""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "order_id",
                "type": "int"
            }
        }


class DatasetCreate(BaseModel):
    """Schema for creating a dataset"""
    fqn: str = Field(..., description="Fully qualified name (connection.database.schema.table)")
    source_type: str = Field(..., description="Source system type")
    columns: List[ColumnSchema] = Field(..., description="List of columns")

    @validator('source_type')
    def validate_source_type(cls, v):
        if v not in SOURCE_SYSTEMS:
            raise ValueError(f"source_type must be one of {SOURCE_SYSTEMS}")
        return v

    @validator('fqn')
    def validate_fqn(cls, v):
        parts = v.split('.')
        if len(parts) != 4:
            raise ValueError("FQN must have format: connection.database.schema.table")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "fqn": "snowflake_prod.sales.public.orders",
                "source_type": "PostgreSQL",
                "columns": [
                    {"name": "order_id", "type": "int"},
                    {"name": "customer_name", "type": "varchar"}
                ]
            }
        }


class DatasetResponse(BaseModel):
    """Schema for dataset response"""
    id: int
    fqn: str
    connection_name: str
    database_name: str
    schema_name: str
    table_name: str
    source_type: str
    columns: List[ColumnSchema]

    class Config:
        from_attributes = True


class LineageCreate(BaseModel):
    """Schema for creating lineage relationship"""
    upstream_fqn: str = Field(..., description="Upstream dataset FQN")
    downstream_fqn: str = Field(..., description="Downstream dataset FQN")

    @validator('upstream_fqn', 'downstream_fqn')
    def validate_fqn(cls, v):
        parts = v.split('.')
        if len(parts) != 4:
            raise ValueError("FQN must have format: connection.database.schema.table")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "upstream_fqn": "snowflake.sales.bronze.orders_raw",
                "downstream_fqn": "snowflake.sales.silver.orders_clean"
            }
        }


class LineageResponse(BaseModel):
    """Schema for lineage response"""
    id: int
    upstream_fqn: str
    downstream_fqn: str

    class Config:
        from_attributes = True


class SearchResponse(BaseModel):
    """Schema for search results"""
    dataset: DatasetResponse
    match_type: str  # table_name, column_name, schema_name, database_name
    priority: int
    upstream_datasets: List[str] = []
    downstream_datasets: List[str] = []

    class Config:
        from_attributes = True


class ErrorResponse(BaseModel):
    """Schema for error responses"""
    error: str
    details: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Cycle detected",
                "details": "Creating this lineage would create a cycle: A -> B -> C -> A"
            }
        }