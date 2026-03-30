"""Volume file tools - Manage files in Unity Catalog Volumes."""

from typing import Dict, Any

from databricks_tools_core.unity_catalog import (
    list_volume_files as _list_volume_files,
    upload_to_volume as _upload_to_volume,
    download_from_volume as _download_from_volume,
    delete_from_volume as _delete_from_volume,
    create_volume_directory as _create_volume_directory,
    get_volume_file_metadata as _get_volume_file_metadata,
)

from ..server import mcp


@mcp.tool(timeout=30)
def list_volume_files(volume_path: str, max_results: int = 500) -> Dict[str, Any]:
    """List files in volume path. Returns: {files: [{name, path, is_directory, file_size, last_modified}], truncated}."""
    # Cap max_results to prevent buffer overflow (1MB JSON limit)
    max_results = min(max_results, 1000)

    # Fetch one extra to detect if there are more results
    results = _list_volume_files(volume_path, max_results=max_results + 1)
    truncated = len(results) > max_results

    # Only return up to max_results
    results = results[:max_results]

    files = [
        {
            "name": r.name,
            "path": r.path,
            "is_directory": r.is_directory,
            "file_size": r.file_size,
            "last_modified": r.last_modified,
        }
        for r in results
    ]

    return {
        "files": files,
        "returned_count": len(files),
        "truncated": truncated,
        "message": f"Results limited to {len(files)} items. Use a more specific path or subdirectory to see more files."
        if truncated
        else None,
    }


@mcp.tool(timeout=300)
def upload_to_volume(
    local_path: str,
    volume_path: str,
    max_workers: int = 4,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Upload file/folder/glob to volume. Auto-creates directories. Returns: {total_files, successful, failed, success}."""
    result = _upload_to_volume(
        local_path=local_path,
        volume_path=volume_path,
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


@mcp.tool(timeout=60)
def download_from_volume(
    volume_path: str,
    local_path: str,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Download file from volume to local path. Returns: {volume_path, local_path, success, error}."""
    result = _download_from_volume(
        volume_path=volume_path,
        local_path=local_path,
        overwrite=overwrite,
    )
    return {
        "volume_path": result.volume_path,
        "local_path": result.local_path,
        "success": result.success,
        "error": result.error,
    }


@mcp.tool(timeout=120)
def delete_from_volume(
    volume_path: str,
    recursive: bool = False,
    max_workers: int = 4,
) -> Dict[str, Any]:
    """Delete file/directory from volume. recursive=True for non-empty dirs. Returns: {success, files_deleted, directories_deleted}."""
    result = _delete_from_volume(
        volume_path=volume_path,
        recursive=recursive,
        max_workers=max_workers,
    )
    return {
        "volume_path": result.volume_path,
        "success": result.success,
        "files_deleted": result.files_deleted,
        "directories_deleted": result.directories_deleted,
        "error": result.error,
    }


@mcp.tool(timeout=30)
def create_volume_directory(volume_path: str) -> Dict[str, Any]:
    """Create directory in volume (like mkdir -p). Idempotent. Returns: {volume_path, success}."""
    try:
        _create_volume_directory(volume_path)
        return {"volume_path": volume_path, "success": True}
    except Exception as e:
        return {"volume_path": volume_path, "success": False, "error": str(e)}


@mcp.tool(timeout=30)
def get_volume_file_info(volume_path: str) -> Dict[str, Any]:
    """Get file metadata. Returns: {name, path, is_directory, file_size, last_modified}."""
    try:
        info = _get_volume_file_metadata(volume_path)
        return {
            "name": info.name,
            "path": info.path,
            "is_directory": info.is_directory,
            "file_size": info.file_size,
            "last_modified": info.last_modified,
            "success": True,
        }
    except Exception as e:
        return {"volume_path": volume_path, "success": False, "error": str(e)}
