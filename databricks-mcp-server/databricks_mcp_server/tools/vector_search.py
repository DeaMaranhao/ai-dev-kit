"""Vector Search tools - Manage endpoints, indexes, and query vector data.

Provides 8 workflow-oriented tools following the Lakebase pattern:
- create_or_update for idempotent resource management
- get doubling as list when no name/id provided
- explicit delete
- query as hot-path, manage_vs_data for maintenance ops (upsert/delete/scan/sync)
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

from databricks_tools_core.vector_search import (
    create_vs_endpoint as _create_vs_endpoint,
    get_vs_endpoint as _get_vs_endpoint,
    list_vs_endpoints as _list_vs_endpoints,
    delete_vs_endpoint as _delete_vs_endpoint,
    create_vs_index as _create_vs_index,
    get_vs_index as _get_vs_index,
    list_vs_indexes as _list_vs_indexes,
    delete_vs_index as _delete_vs_index,
    sync_vs_index as _sync_vs_index,
    query_vs_index as _query_vs_index,
    upsert_vs_data as _upsert_vs_data,
    delete_vs_data as _delete_vs_data,
    scan_vs_index as _scan_vs_index,
)

from ..server import mcp

logger = logging.getLogger(__name__)


# ============================================================================
# Helpers
# ============================================================================


def _find_endpoint_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Find a vector search endpoint by name, returns None if not found."""
    try:
        result = _get_vs_endpoint(name=name)
        if result.get("state") == "NOT_FOUND":
            return None
        return result
    except Exception:
        return None


def _find_index_by_name(index_name: str) -> Optional[Dict[str, Any]]:
    """Find a vector search index by name, returns None if not found."""
    try:
        result = _get_vs_index(index_name=index_name)
        if result.get("state") == "NOT_FOUND":
            return None
        return result
    except Exception:
        return None


# ============================================================================
# Tool 1: create_or_update_vs_endpoint
# ============================================================================


@mcp.tool(timeout=120)
def create_or_update_vs_endpoint(
    name: str,
    endpoint_type: str = "STANDARD",
) -> Dict[str, Any]:
    """Idempotent create for Vector Search endpoints. Returns existing if already exists.

    endpoint_type: "STANDARD" (<100ms) or "STORAGE_OPTIMIZED" (~250ms, 1B+ vectors).
    Async creation - use get_vs_endpoint() to poll status.
    Returns: {name, endpoint_type, created: bool}."""
    existing = _find_endpoint_by_name(name)
    if existing:
        return {**existing, "created": False}

    result = _create_vs_endpoint(name=name, endpoint_type=endpoint_type)

    try:
        from ..manifest import track_resource

        track_resource(
            resource_type="vs_endpoint",
            name=name,
            resource_id=name,
        )
    except Exception:
        pass  # best-effort tracking

    return {**result, "created": True}


@mcp.tool(timeout=30)
def get_vs_endpoint(
    name: Optional[str] = None,
) -> Dict[str, Any]:
    """Get endpoint details or list all. Pass name for one, omit for all.

    Returns: {name, state, num_indexes} or {endpoints: [...]}."""
    if name:
        return _get_vs_endpoint(name=name)

    return {"endpoints": _list_vs_endpoints()}


# ============================================================================
# Tool 3: delete_vs_endpoint
# ============================================================================


@mcp.tool(timeout=60)
def delete_vs_endpoint(name: str) -> Dict[str, Any]:
    """Delete a Vector Search endpoint. All indexes must be deleted first.

    Returns: {name, status}."""
    return _delete_vs_endpoint(name=name)


# ============================================================================
# Tool 4: create_or_update_vs_index
# ============================================================================


@mcp.tool(timeout=120)
def create_or_update_vs_index(
    name: str,
    endpoint_name: str,
    primary_key: str,
    index_type: str = "DELTA_SYNC",
    delta_sync_index_spec: Optional[Dict[str, Any]] = None,
    direct_access_index_spec: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Idempotent create for Vector Search indexes. Returns existing if found. Auto-triggers initial sync for DELTA_SYNC.

    index_type: "DELTA_SYNC" (auto-sync from Delta) or "DIRECT_ACCESS" (manual CRUD).
    delta_sync_index_spec: {source_table, embedding_source_columns (managed) OR embedding_vector_columns (self-managed), pipeline_type: TRIGGERED|CONTINUOUS}.
    direct_access_index_spec: {embedding_vector_columns, schema_json}.
    See databricks-vector-search skill for full spec details.
    Returns: {name, created: bool, sync_triggered}."""
    existing = _find_index_by_name(name)
    if existing:
        return {**existing, "created": False}

    result = _create_vs_index(
        name=name,
        endpoint_name=endpoint_name,
        primary_key=primary_key,
        index_type=index_type,
        delta_sync_index_spec=delta_sync_index_spec,
        direct_access_index_spec=direct_access_index_spec,
    )

    # Trigger initial sync for DELTA_SYNC indexes
    if index_type == "DELTA_SYNC" and result.get("status") != "ALREADY_EXISTS":
        try:
            _sync_vs_index(index_name=name)
            result["sync_triggered"] = True
        except Exception as e:
            logger.warning("Failed to trigger initial sync for index '%s': %s", name, e)
            result["sync_triggered"] = False

    try:
        from ..manifest import track_resource

        track_resource(
            resource_type="vs_index",
            name=name,
            resource_id=name,
        )
    except Exception:
        pass  # best-effort tracking

    return {**result, "created": True}


@mcp.tool(timeout=30)
def get_vs_index(
    index_name: Optional[str] = None,
    endpoint_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Get index details or list indexes.

    index_name: Get one index. endpoint_name: List indexes on endpoint. Omit both: list all.
    Returns: {name, state, ...} or {indexes: [...]}."""
    if index_name:
        return _get_vs_index(index_name=index_name)

    if endpoint_name:
        return {"indexes": _list_vs_indexes(endpoint_name=endpoint_name)}

    # List all indexes across all endpoints
    all_indexes = []
    endpoints = _list_vs_endpoints()
    for ep in endpoints:
        ep_name = ep.get("name")
        if not ep_name:
            continue
        try:
            indexes = _list_vs_indexes(endpoint_name=ep_name)
            for idx in indexes:
                idx["endpoint_name"] = ep_name
            all_indexes.extend(indexes)
        except Exception:
            logger.warning("Failed to list indexes on endpoint '%s'", ep_name)
    return {"indexes": all_indexes}


# ============================================================================
# Tool 6: delete_vs_index
# ============================================================================


@mcp.tool(timeout=60)
def delete_vs_index(index_name: str) -> Dict[str, Any]:
    """Delete a Vector Search index. Returns: {name, status}."""
    return _delete_vs_index(index_name=index_name)


# ============================================================================
# Tool 7: query_vs_index
# ============================================================================


@mcp.tool(timeout=60)
def query_vs_index(
    index_name: str,
    columns: List[str],
    query_text: Optional[str] = None,
    query_vector: Optional[List[float]] = None,
    num_results: int = 5,
    filters_json: Optional[Union[str, dict]] = None,
    filter_string: Optional[str] = None,
    query_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Query a Vector Search index for similar documents.

    Use query_text (managed embeddings) or query_vector (pre-computed).
    Filters: filters_json for Standard, filter_string (SQL) for Storage-Optimized.
    query_type: "ANN" (default) or "HYBRID".
    Returns: {columns, data (score appended), num_results}."""
    # MCP deserializes JSON params, so filters_json may arrive as a dict
    if isinstance(filters_json, dict):
        filters_json = json.dumps(filters_json)

    return _query_vs_index(
        index_name=index_name,
        columns=columns,
        query_text=query_text,
        query_vector=query_vector,
        num_results=num_results,
        filters_json=filters_json,
        filter_string=filter_string,
        query_type=query_type,
    )


# ============================================================================
# Tool 8: manage_vs_data
# ============================================================================


@mcp.tool(timeout=120)
def manage_vs_data(
    index_name: str,
    operation: str,
    inputs_json: Optional[Union[str, list]] = None,
    primary_keys: Optional[List[str]] = None,
    num_results: int = 100,
) -> Dict[str, Any]:
    """Manage Vector Search index data: upsert, delete, scan, or sync.

    Operations:
    - upsert: requires inputs_json (records with pk + embedding)
    - delete: requires primary_keys
    - scan: optional num_results (default 100)
    - sync: triggers re-sync for TRIGGERED DELTA_SYNC indexes"""
    op = operation.lower()

    if op == "upsert":
        if inputs_json is None:
            return {"error": "inputs_json is required for upsert operation."}
        # MCP deserializes JSON params, so inputs_json may arrive as a list
        if isinstance(inputs_json, (dict, list)):
            inputs_json = json.dumps(inputs_json)
        return _upsert_vs_data(index_name=index_name, inputs_json=inputs_json)

    elif op == "delete":
        if primary_keys is None:
            return {"error": "primary_keys is required for delete operation."}
        return _delete_vs_data(index_name=index_name, primary_keys=primary_keys)

    elif op == "scan":
        return _scan_vs_index(index_name=index_name, num_results=num_results)

    elif op == "sync":
        _sync_vs_index(index_name=index_name)
        return {"index_name": index_name, "status": "sync_triggered"}

    else:
        return {"error": f"Invalid operation '{operation}'. Use 'upsert', 'delete', 'scan', or 'sync'."}
