"""File tools - Upload and delete files and folders in Databricks workspace."""

from typing import Any, Dict

from databricks_tools_core.file import (
    delete_from_workspace as _delete_from_workspace,
    upload_to_workspace as _upload_to_workspace,
)

from ..server import mcp


@mcp.tool
def upload_to_workspace(
    local_path: str,
    workspace_path: str,
    max_workers: int = 10,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Upload files/folders to Databricks workspace. Supports files, folders, globs, tilde expansion.

    Returns: {local_folder, remote_folder, total_files, successful, failed, success, failed_uploads}."""
    result = _upload_to_workspace(
        local_path=local_path,
        workspace_path=workspace_path,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    return {
        "local_folder": result.local_folder,
        "remote_folder": result.remote_folder,
        "total_files": result.total_files,
        "successful": result.successful,
        "failed": result.failed,
        "success": result.success,
        "failed_uploads": [
            {"local_path": r.local_path, "error": r.error} for r in result.get_failed_uploads()
        ]
        if result.failed > 0
        else [],
    }


@mcp.tool
def delete_from_workspace(
    workspace_path: str,
    recursive: bool = False,
) -> Dict[str, Any]:
    """Delete file/folder from workspace. recursive=True for folders. Has safety checks for protected paths.

    Returns: {workspace_path, success, error}."""
    result = _delete_from_workspace(
        workspace_path=workspace_path,
        recursive=recursive,
    )
    return {
        "workspace_path": result.workspace_path,
        "success": result.success,
        "error": result.error,
    }
