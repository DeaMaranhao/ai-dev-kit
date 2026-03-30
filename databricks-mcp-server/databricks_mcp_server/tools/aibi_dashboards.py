"""AI/BI Dashboard tools - Create and manage AI/BI dashboards.

Note: AI/BI dashboards were previously known as Lakeview dashboards.
The SDK/API still uses the 'lakeview' name internally.

Provides 4 workflow-oriented tools following the Lakebase pattern:
- create_or_update_dashboard: idempotent create/update with auto-publish
- get_dashboard: get details by ID, or list all
- delete_dashboard: move to trash (renamed from trash_dashboard for consistency)
- publish_dashboard: publish or unpublish via boolean toggle
"""

import json
from typing import Any, Dict, Union

from databricks_tools_core.aibi_dashboards import (
    create_or_update_dashboard as _create_or_update_dashboard,
    get_dashboard as _get_dashboard,
    list_dashboards as _list_dashboards,
    publish_dashboard as _publish_dashboard,
    trash_dashboard as _trash_dashboard,
    unpublish_dashboard as _unpublish_dashboard,
)

from ..manifest import register_deleter
from ..server import mcp


def _delete_dashboard_resource(resource_id: str) -> None:
    _trash_dashboard(dashboard_id=resource_id)


register_deleter("dashboard", _delete_dashboard_resource)


# ============================================================================
# Tool 1: create_or_update_dashboard
# ============================================================================


@mcp.tool(timeout=120)
def create_or_update_dashboard(
    display_name: str,
    parent_path: str,
    serialized_dashboard: Union[str, dict],
    warehouse_id: str,
    publish: bool = True,
) -> Dict[str, Any]:
    """Create/update AI/BI dashboard from JSON. MUST test queries with execute_sql() first!

    Widget structure: queries is TOP-LEVEL SIBLING of spec (NOT inside spec, NOT named_queries).
    fields[].name MUST match encodings fieldName exactly. Use datasetName (camelCase).
    Versions: counter/table/filter=2, bar/line/pie=3. Layout: 6-col grid.
    Filter types: filter-multi-select, filter-single-select, filter-date-range-picker.
    Text widget uses textbox_spec (no spec block). See databricks-aibi-dashboards skill.

    Returns: {success, dashboard_id, path, url, published, error}."""
    # MCP deserializes JSON params, so serialized_dashboard may arrive as a dict
    if isinstance(serialized_dashboard, dict):
        serialized_dashboard = json.dumps(serialized_dashboard)

    result = _create_or_update_dashboard(
        display_name=display_name,
        parent_path=parent_path,
        serialized_dashboard=serialized_dashboard,
        warehouse_id=warehouse_id,
        publish=publish,
    )

    # Track resource on successful create/update
    try:
        if result.get("success") and result.get("dashboard_id"):
            from ..manifest import track_resource

            track_resource(
                resource_type="dashboard",
                name=display_name,
                resource_id=result["dashboard_id"],
                url=result.get("url"),
            )
    except Exception:
        pass  # best-effort tracking

    return result


# ============================================================================
# Tool 2: get_dashboard
# ============================================================================


@mcp.tool(timeout=30)
def get_dashboard(
    dashboard_id: str = None,
    page_size: int = 25,
) -> Dict[str, Any]:
    """Get dashboard by ID or list all. Pass dashboard_id for one, omit to list all."""
    if dashboard_id:
        return _get_dashboard(dashboard_id=dashboard_id)

    return _list_dashboards(page_size=page_size)


# ============================================================================
# Tool 3: delete_dashboard
# ============================================================================


@mcp.tool(timeout=30)
def delete_dashboard(dashboard_id: str) -> Dict[str, str]:
    """Soft-delete dashboard (moves to trash). Returns: {status, message}."""
    result = _trash_dashboard(dashboard_id=dashboard_id)
    try:
        from ..manifest import remove_resource

        remove_resource(resource_type="dashboard", resource_id=dashboard_id)
    except Exception:
        pass
    return result


# ============================================================================
# Tool 4: publish_dashboard
# ============================================================================


@mcp.tool(timeout=60)
def publish_dashboard(
    dashboard_id: str,
    warehouse_id: str = None,
    publish: bool = True,
    embed_credentials: bool = True,
) -> Dict[str, Any]:
    """Publish/unpublish dashboard. publish=False to unpublish. warehouse_id required for publish.

    embed_credentials=True allows users without data access to view (uses SP permissions)."""
    if not publish:
        return _unpublish_dashboard(dashboard_id=dashboard_id)

    if not warehouse_id:
        return {"error": "warehouse_id is required for publishing."}

    return _publish_dashboard(
        dashboard_id=dashboard_id,
        warehouse_id=warehouse_id,
        embed_credentials=embed_credentials,
    )
