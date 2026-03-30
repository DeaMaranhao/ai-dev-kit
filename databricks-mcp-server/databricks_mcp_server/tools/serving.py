"""Model Serving tools - Query and manage serving endpoints."""

from typing import Any, Dict, List, Optional

from databricks_tools_core.serving import (
    get_serving_endpoint_status as _get_serving_endpoint_status,
    query_serving_endpoint as _query_serving_endpoint,
    list_serving_endpoints as _list_serving_endpoints,
)

from ..server import mcp


@mcp.tool(timeout=30)
def get_serving_endpoint_status(name: str) -> Dict[str, Any]:
    """Get status of a Model Serving endpoint.

    Returns: {name, state (READY/NOT_READY/NOT_FOUND), config_update, served_entities, error}."""
    return _get_serving_endpoint_status(name=name)


@mcp.tool(timeout=120)
def query_serving_endpoint(
    name: str,
    messages: Optional[List[Dict[str, str]]] = None,
    inputs: Optional[Dict[str, Any]] = None,
    dataframe_records: Optional[List[Dict[str, Any]]] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
) -> Dict[str, Any]:
    """Query a Model Serving endpoint.

    Input formats (use one):
    - messages: Chat/agent endpoints. Format: [{"role": "user", "content": "..."}]
    - inputs: Custom pyfunc models (dict matching model signature)
    - dataframe_records: ML models. Format: [{"feature1": 1.0, ...}]

    See databricks-model-serving skill for endpoint configuration.
    Returns: {choices: [...]} for chat or {predictions: [...]} for ML."""
    return _query_serving_endpoint(
        name=name,
        messages=messages,
        inputs=inputs,
        dataframe_records=dataframe_records,
        max_tokens=max_tokens,
        temperature=temperature,
    )


@mcp.tool(timeout=30)
def list_serving_endpoints(limit: int = 50) -> List[Dict[str, Any]]:
    """List Model Serving endpoints in the workspace.

    Returns: [{name, state, creation_timestamp, creator, served_entities_count}, ...]"""
    return _list_serving_endpoints(limit=limit)
