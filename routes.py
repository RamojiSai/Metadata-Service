"""API Routes - All endpoints"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from database import get_db
from utils import (
    DatasetCreate, DatasetResponse, LineageCreate, LineageResponse,
    SearchResponse, ErrorResponse, ColumnSchema
)
from Appmanagement import (
    add_dataset, add_lineage, search_datasets_with_lineage,
    get_dataset_lineage, validate_dataset_data,
    CycleDetectionError, DatasetNotFoundError, DatasetAlreadyExistsError
)
from log import log_info, log_error

# Create router
router = APIRouter()


@router.post("/datasets", response_model=DatasetResponse)
def create_dataset(dataset: DatasetCreate, db: Session = Depends(get_db)):
    """
    Create a new dataset with columns
    
    - **fqn**: Fully qualified name (connection.database.schema.table)
    - **source_type**: Source system (MySQL, MSSQL, PostgreSQL)
    - **columns**: List of columns with name and type
    """
    try:
        # Validate data
        validate_dataset_data(dataset.fqn, dataset.source_type, 
                            [col.dict() for col in dataset.columns])
        
        # Create dataset
        db_dataset = add_dataset(
            db,
            fqn=dataset.fqn,
            source_type=dataset.source_type,
            columns=[col.dict() for col in dataset.columns]
        )
        
        # Build response
        return DatasetResponse(
            id=db_dataset.id,
            fqn=db_dataset.fqn,
            connection_name=db_dataset.connection_name,
            database_name=db_dataset.database_name,
            schema_name=db_dataset.schema_name,
            table_name=db_dataset.table_name,
            source_type=db_dataset.source_type,
            columns=[ColumnSchema(name=col.name, type=col.type) for col in db_dataset.columns]
        )
    
    except DatasetAlreadyExistsError as e:
        log_error("DatasetAlreadyExists", str(e))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "Dataset already exists", "details": str(e)}
        )
    except ValueError as e:
        log_error("ValidationError", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Validation error", "details": str(e)}
        )
    except Exception as e:
        log_error("UnexpectedError", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "details": str(e)}
        )


@router.post("/lineage", response_model=LineageResponse)
def create_lineage(lineage: LineageCreate, db: Session = Depends(get_db)):
    """
    Create lineage relationship between two datasets
    
    - **upstream_fqn**: Source dataset FQN
    - **downstream_fqn**: Target dataset FQN
    
    Validates that no cycles are created in the lineage graph
    """
    try:
        db_lineage = add_lineage(
            db,
            upstream_fqn=lineage.upstream_fqn,
            downstream_fqn=lineage.downstream_fqn
        )
        
        return LineageResponse(
            id=db_lineage.id,
            upstream_fqn=lineage.upstream_fqn,
            downstream_fqn=lineage.downstream_fqn
        )
    
    except CycleDetectionError as e:
        log_error("CycleDetected", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Cycle detected", "details": str(e)}
        )
    except DatasetNotFoundError as e:
        log_error("DatasetNotFound", str(e))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Dataset not found", "details": str(e)}
        )
    except Exception as e:
        log_error("UnexpectedError", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "details": str(e)}
        )


@router.get("/search", response_model=List[SearchResponse])
def search_datasets(query: str, db: Session = Depends(get_db)):
    """
    Search datasets by name or column names
    
    - **query**: Search term (searches table name, column names, schema name, database name)
    
    Results are sorted by priority:
    1. Table name match
    2. Column name match
    3. Schema name match
    4. Database name match
    """
    try:
        if not query or len(query.strip()) == 0:
            raise ValueError("Query parameter cannot be empty")
        
        results = search_datasets_with_lineage(db, query)
        
        response = []
        for result in results:
            dataset = result["dataset"]
            response.append(SearchResponse(
                dataset=DatasetResponse(
                    id=dataset.id,
                    fqn=dataset.fqn,
                    connection_name=dataset.connection_name,
                    database_name=dataset.database_name,
                    schema_name=dataset.schema_name,
                    table_name=dataset.table_name,
                    source_type=dataset.source_type,
                    columns=[ColumnSchema(name=col.name, type=col.type) for col in dataset.columns]
                ),
                match_type=result["match_type"],
                priority=result["priority"],
                upstream_datasets=result["upstream_datasets"],
                downstream_datasets=result["downstream_datasets"]
            ))
        
        return response
    
    except ValueError as e:
        log_error("ValidationError", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Validation error", "details": str(e)}
        )
    except Exception as e:
        log_error("UnexpectedError", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "details": str(e)}
        )


@router.get("/datasets/{fqn:path}/lineage",)
def get_lineage(fqn: str, db: Session = Depends(get_db)):
    """
    Get lineage information for a specific dataset
    
    - **fqn**: Fully qualified name of the dataset
    
    Returns upstream and downstream datasets
    """
    try:
        lineage_info = get_dataset_lineage(db, fqn)
        
        return {
            "fqn": fqn,
            "upstream_datasets": lineage_info["upstream"],
            "downstream_datasets": lineage_info["downstream"]
        }
    
    except DatasetNotFoundError as e:
        log_error("DatasetNotFound", str(e))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Dataset not found", "details": str(e)}
        )
    except Exception as e:
        log_error("UnexpectedError", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "details": str(e)}
        )