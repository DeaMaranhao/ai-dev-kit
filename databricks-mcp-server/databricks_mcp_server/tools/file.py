"""File tools - Upload files and folders to Databricks workspace."""

from typing import Dict, Any

from databricks_tools_core.file import upload_to_workspace as _upload_to_workspace

from ..server import mcp


@mcp.tool(timeout=300)
def upload_to_workspace(
    local_path: str,
    workspace_path: str,
    max_workers: int = 10,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """
    Upload local file(s) or folder(s) to Databricks workspace.

    Works like the `cp` command - handles single files, folders, and glob patterns.
    Automatically creates parent directories in workspace as needed.

    Args:
        local_path: Path to local file, folder, or glob pattern. Examples:
            - "/path/to/file.py" - single file
            - "/path/to/folder" - entire folder (recursive)
            - "/path/to/folder/*" - all files/folders in folder
            - "/path/to/*.py" - glob pattern
        workspace_path: Target path in Databricks workspace
            (e.g., "/Workspace/Users/user@example.com/my-project")
        max_workers: Maximum parallel upload threads (default: 10)
        overwrite: Whether to overwrite existing files (default: True)

    Returns:
        Dictionary with upload statistics:
        - local_folder: Source path
        - remote_folder: Target workspace path
        - total_files: Number of files found
        - successful: Number of successful uploads
        - failed: Number of failed uploads
        - success: True if all uploads succeeded
    """
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
        "failed_uploads": [{"local_path": r.local_path, "error": r.error} for r in result.get_failed_uploads()]
        if result.failed > 0
        else [],
    }
