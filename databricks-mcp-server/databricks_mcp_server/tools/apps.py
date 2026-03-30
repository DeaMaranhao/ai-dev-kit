"""App tools - Manage Databricks Apps lifecycle.

Provides 3 workflow-oriented tools following the Lakebase pattern:
- create_or_update_app: idempotent create + optional deploy
- get_app: get details by name (with optional logs), or list all
- delete_app: delete by name
"""

import logging
from typing import Any, Dict, Optional

from databricks_tools_core.apps.apps import (
    create_app as _create_app,
    get_app as _get_app,
    list_apps as _list_apps,
    deploy_app as _deploy_app,
    delete_app as _delete_app,
    get_app_logs as _get_app_logs,
)
from databricks_tools_core.identity import with_description_footer

from ..manifest import register_deleter
from ..server import mcp

logger = logging.getLogger(__name__)


def _delete_app_resource(resource_id: str) -> None:
    _delete_app(name=resource_id)


register_deleter("app", _delete_app_resource)


# ============================================================================
# Helpers
# ============================================================================


def _find_app_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Find an app by name, returns None if not found."""
    try:
        result = _get_app(name=name)
        if result.get("error"):
            return None
        return result
    except Exception:
        return None


# ============================================================================
# Tool 1: create_or_update_app
# ============================================================================


@mcp.tool(timeout=180)
def create_or_update_app(
    name: str,
    source_code_path: Optional[str] = None,
    description: Optional[str] = None,
    mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Create app if not exists, optionally deploy. Deploys latest code if source_code_path provided.

    See databricks-app-python skill for app development guidance.
    Returns: {name, created: bool, url, status, deployment}."""
    existing = _find_app_by_name(name)

    if existing:
        result = {**existing, "created": False}
    else:
        app_result = _create_app(name=name, description=with_description_footer(description))
        result = {**app_result, "created": True}

        # Track resource on successful create
        try:
            if result.get("name"):
                from ..manifest import track_resource

                track_resource(
                    resource_type="app",
                    name=result["name"],
                    resource_id=result["name"],
                )
        except Exception:
            pass  # best-effort tracking

    # Deploy if source_code_path provided
    if source_code_path:
        try:
            deployment = _deploy_app(
                app_name=name,
                source_code_path=source_code_path,
                mode=mode,
            )
            result["deployment"] = deployment
        except Exception as e:
            logger.warning("Failed to deploy app '%s': %s", name, e)
            result["deployment_error"] = str(e)

    return result


# ============================================================================
# Tool 2: get_app
# ============================================================================


@mcp.tool(timeout=30)
def get_app(
    name: Optional[str] = None,
    name_contains: Optional[str] = None,
    include_logs: bool = False,
    deployment_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get app details or list all. include_logs=True for deployment logs.

    Returns: {name, url, status, logs} or {apps: [...]}."""
    if name:
        result = _get_app(name=name)

        if include_logs:
            try:
                logs = _get_app_logs(
                    app_name=name,
                    deployment_id=deployment_id,
                )
                result["logs"] = logs.get("logs", "")
                result["logs_deployment_id"] = logs.get("deployment_id")
            except Exception as e:
                result["logs_error"] = str(e)

        return result

    return {"apps": _list_apps(name_contains=name_contains)}


# ============================================================================
# Tool 3: delete_app
# ============================================================================


@mcp.tool(timeout=60)
def delete_app(name: str) -> Dict[str, str]:
    """Delete a Databricks App."""
    result = _delete_app(name=name)

    # Remove from tracked resources
    try:
        from ..manifest import remove_resource

        remove_resource(resource_type="app", resource_id=name)
    except Exception:
        pass  # best-effort tracking

    return result
