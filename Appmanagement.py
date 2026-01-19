"""Application management - Business logic and validation"""

from typing import List, Dict, Set, Optional
from sqlalchemy.orm import Session
from database import (
    create_dataset as db_create_dataset,
    get_dataset_by_fqn,
    get_dataset_by_id,
    create_lineage as db_create_lineage,
    get_upstream_datasets,
    get_downstream_datasets,
    search_datasets as db_search_datasets,
    Dataset
)
from log import log_info, log_error, log_exception


class CycleDetectionError(Exception):
    """Exception raised when a cycle is detected in lineage"""
    pass


class DatasetNotFoundError(Exception):
    """Exception raised when dataset is not found"""
    pass


class DatasetAlreadyExistsError(Exception):
    """Exception raised when dataset already exists"""
    pass


@log_exception
def add_dataset(db: Session, fqn: str, source_type: str, columns: List[Dict]) -> Dataset:
    """
    Add a new dataset to the system
    Validates that dataset doesn't already exist
    """
    # Check if dataset already exists
    existing = get_dataset_by_fqn(db, fqn)
    if existing:
        log_error("DatasetAlreadyExists", f"Dataset with FQN {fqn} already exists")
        raise DatasetAlreadyExistsError(f"Dataset with FQN {fqn} already exists")

    # Create dataset
    dataset = db_create_dataset(db, fqn, source_type, columns)
    log_info(f"Dataset added successfully: {fqn}")
    return dataset


@log_exception
def detect_cycle_dfs(db: Session, start_id: int, target_id: int) -> bool:
    """
    Detect if adding edge (start_id -> target_id) would create a cycle
    Uses DFS to check if there's already a path from target_id to start_id
    If such path exists, adding start_id -> target_id would create a cycle
    """
    visited = set()
    
    def dfs(current_id: int) -> bool:
        if current_id == start_id:
            return True  # Cycle detected
        
        if current_id in visited:
            return False
        
        visited.add(current_id)
        
        # Get all downstream datasets from current
        downstream = get_downstream_datasets(db, current_id)
        for ds in downstream:
            if dfs(ds.id):
                return True
        
        return False
    
    # Start DFS from target_id to see if we can reach start_id
    return dfs(target_id)


@log_exception
def add_lineage(db: Session, upstream_fqn: str, downstream_fqn: str):
    """
    Add lineage relationship between two datasets
    Validates:
    1. Both datasets exist
    2. No cycle would be created
    """
    # Check if datasets exist
    upstream_dataset = get_dataset_by_fqn(db, upstream_fqn)
    if not upstream_dataset:
        log_error("DatasetNotFound", f"Upstream dataset {upstream_fqn} not found")
        raise DatasetNotFoundError(f"Upstream dataset {upstream_fqn} not found")
    
    downstream_dataset = get_dataset_by_fqn(db, downstream_fqn)
    if not downstream_dataset:
        log_error("DatasetNotFound", f"Downstream dataset {downstream_fqn} not found")
        raise DatasetNotFoundError(f"Downstream dataset {downstream_fqn} not found")
    
    # Check for self-reference
    if upstream_dataset.id == downstream_dataset.id:
        log_error("CycleDetected", "Cannot create lineage to self")
        raise CycleDetectionError("Cannot create lineage to self")
    
    # Check if this would create a cycle
    # If there's already a path from downstream to upstream, adding upstream->downstream creates cycle
    if detect_cycle_dfs(db, upstream_dataset.id, downstream_dataset.id):
        error_msg = f"Creating lineage {upstream_fqn} -> {downstream_fqn} would create a cycle"
        log_error("CycleDetected", error_msg)
        raise CycleDetectionError(error_msg)
    
    # Create lineage
    lineage = db_create_lineage(db, upstream_dataset.id, downstream_dataset.id)
    log_info(f"Lineage added: {upstream_fqn} -> {downstream_fqn}")
    return lineage


@log_exception
def search_datasets_with_lineage(db: Session, query: str) -> List[Dict]:
    """
    Search datasets and include their lineage information
    Returns sorted results with priority
    """
    results = db_search_datasets(db, query)
    
    search_results = []
    for dataset, match_type, priority in results:
        # Get upstream datasets
        upstream = get_upstream_datasets(db, dataset.id)
        upstream_fqns = [ds.fqn for ds in upstream]
        
        # Get downstream datasets
        downstream = get_downstream_datasets(db, dataset.id)
        downstream_fqns = [ds.fqn for ds in downstream]
        
        # Build result
        result = {
            "dataset": dataset,
            "match_type": match_type,
            "priority": priority,
            "upstream_datasets": upstream_fqns,
            "downstream_datasets": downstream_fqns
        }
        search_results.append(result)
    
    log_info(f"Search completed: '{query}' - {len(search_results)} results found")
    return search_results


@log_exception
def get_dataset_lineage(db: Session, fqn: str) -> Dict:
    """
    Get complete lineage information for a dataset
    """
    dataset = get_dataset_by_fqn(db, fqn)
    if not dataset:
        raise DatasetNotFoundError(f"Dataset {fqn} not found")
    
    upstream = get_upstream_datasets(db, dataset.id)
    downstream = get_downstream_datasets(db, dataset.id)
    
    return {
        "dataset": dataset,
        "upstream": [ds.fqn for ds in upstream],
        "downstream": [ds.fqn for ds in downstream]
    }


@log_exception
def validate_dataset_data(fqn: str, source_type: str, columns: List[Dict]):
    """
    Validate dataset data before creation
    """
    # FQN validation
    parts = fqn.split('.')
    if len(parts) != 4:
        raise ValueError("FQN must have exactly 4 parts: connection.database.schema.table")
    
    # Check if columns list is not empty
    if not columns:
        raise ValueError("Dataset must have at least one column")
    
    # Check for duplicate column names
    column_names = [col["name"] for col in columns]
    if len(column_names) != len(set(column_names)):
        raise ValueError("Duplicate column names are not allowed")
    
    return True