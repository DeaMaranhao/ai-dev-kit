"""Unit tests for workspace file upload functions."""

import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from databricks_tools_core.file.workspace import (
    upload_to_workspace,
    _collect_files,
    _collect_directories,
    UploadResult,
    FolderUploadResult,
)


class TestCollectFiles:
    """Tests for _collect_files helper function."""

    def test_collects_files_recursively(self, tmp_path):
        """Should collect all files in nested directories."""
        # Create nested structure
        (tmp_path / "file1.py").write_text("content1")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "file2.py").write_text("content2")
        (tmp_path / "subdir" / "nested").mkdir()
        (tmp_path / "subdir" / "nested" / "file3.py").write_text("content3")

        files = _collect_files(str(tmp_path))

        assert len(files) == 3
        rel_paths = {f[1] for f in files}
        assert "file1.py" in rel_paths
        assert os.path.join("subdir", "file2.py") in rel_paths
        assert os.path.join("subdir", "nested", "file3.py") in rel_paths

    def test_skips_hidden_files(self, tmp_path):
        """Should skip files starting with dot."""
        (tmp_path / "visible.py").write_text("content")
        (tmp_path / ".hidden").write_text("hidden")

        files = _collect_files(str(tmp_path))

        assert len(files) == 1
        assert files[0][1] == "visible.py"

    def test_skips_pycache(self, tmp_path):
        """Should skip __pycache__ directories."""
        (tmp_path / "file.py").write_text("content")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "cached.pyc").write_text("cached")

        files = _collect_files(str(tmp_path))

        assert len(files) == 1
        assert files[0][1] == "file.py"


class TestCollectDirectories:
    """Tests for _collect_directories helper function."""

    def test_collects_directories_recursively(self, tmp_path):
        """Should collect all directories."""
        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir1" / "subdir").mkdir()
        (tmp_path / "dir2").mkdir()

        dirs = _collect_directories(str(tmp_path))

        assert "dir1" in dirs
        assert "dir2" in dirs
        assert os.path.join("dir1", "subdir") in dirs

    def test_skips_hidden_directories(self, tmp_path):
        """Should skip directories starting with dot."""
        (tmp_path / "visible").mkdir()
        (tmp_path / ".hidden").mkdir()

        dirs = _collect_directories(str(tmp_path))

        assert "visible" in dirs
        assert ".hidden" not in dirs


class TestUploadToWorkspace:
    """Tests for upload_to_workspace function."""

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_single_file_upload(self, mock_get_client, tmp_path):
        """Should upload a single file correctly."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create a test file
        test_file = tmp_path / "test.py"
        test_file.write_text("print('hello')")

        result = upload_to_workspace(
            local_path=str(test_file),
            workspace_path="/Workspace/Users/test@example.com/test.py",
        )

        assert result.success
        assert result.total_files == 1
        assert result.successful == 1
        assert result.failed == 0
        mock_client.workspace.upload.assert_called_once()

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_folder_upload(self, mock_get_client, tmp_path):
        """Should upload a folder with all files."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create test folder with files
        (tmp_path / "file1.py").write_text("content1")
        (tmp_path / "file2.py").write_text("content2")

        result = upload_to_workspace(
            local_path=str(tmp_path),
            workspace_path="/Workspace/Users/test@example.com/project",
        )

        assert result.success
        assert result.total_files == 2
        assert result.successful == 2
        assert result.failed == 0
        assert mock_client.workspace.upload.call_count == 2
        mock_client.workspace.mkdirs.assert_called()

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_glob_pattern_files(self, mock_get_client, tmp_path):
        """Should upload files matching glob pattern."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create test files
        (tmp_path / "file1.py").write_text("content1")
        (tmp_path / "file2.py").write_text("content2")
        (tmp_path / "data.json").write_text("{}")

        result = upload_to_workspace(
            local_path=str(tmp_path / "*.py"),
            workspace_path="/Workspace/Users/test@example.com/project",
        )

        assert result.success
        assert result.total_files == 2  # Only .py files
        assert result.successful == 2

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_glob_pattern_star(self, mock_get_client, tmp_path):
        """Should upload all contents with /* pattern."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create test folder with subdir
        (tmp_path / "file.py").write_text("content")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "nested.py").write_text("nested")

        result = upload_to_workspace(
            local_path=str(tmp_path / "*"),
            workspace_path="/Workspace/Users/test@example.com/project",
        )

        assert result.success
        # file.py + subdir/nested.py
        assert result.total_files == 2

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_nonexistent_path_returns_error(self, mock_get_client):
        """Should return error result for nonexistent path."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        result = upload_to_workspace(
            local_path="/nonexistent/path/file.py",
            workspace_path="/Workspace/Users/test@example.com/file.py",
        )

        assert not result.success
        assert result.failed == 1
        assert "not found" in result.results[0].error.lower()

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_empty_glob_returns_error(self, mock_get_client, tmp_path):
        """Should return error when glob matches nothing."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        result = upload_to_workspace(
            local_path=str(tmp_path / "*.nonexistent"),
            workspace_path="/Workspace/Users/test@example.com/project",
        )

        assert not result.success
        assert "no files match" in result.results[0].error.lower()

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_creates_parent_directories(self, mock_get_client, tmp_path):
        """Should create parent directories for single file upload."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        test_file = tmp_path / "test.py"
        test_file.write_text("content")

        upload_to_workspace(
            local_path=str(test_file),
            workspace_path="/Workspace/Users/test@example.com/deep/nested/test.py",
        )

        # Should call mkdirs for parent directory
        mock_client.workspace.mkdirs.assert_called_with(
            "/Workspace/Users/test@example.com/deep/nested"
        )

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_handles_upload_failure(self, mock_get_client, tmp_path):
        """Should handle upload failures gracefully."""
        mock_client = mock.Mock()
        mock_client.workspace.upload.side_effect = Exception("Upload failed")
        mock_get_client.return_value = mock_client

        test_file = tmp_path / "test.py"
        test_file.write_text("content")

        result = upload_to_workspace(
            local_path=str(test_file),
            workspace_path="/Workspace/Users/test@example.com/test.py",
        )

        assert not result.success
        assert result.failed == 1
        assert "Upload failed" in result.results[0].error

    @mock.patch("databricks_tools_core.file.workspace.get_workspace_client")
    def test_expands_tilde_in_path(self, mock_get_client, tmp_path):
        """Should expand ~ in local path."""
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create a file in a known location we can reference with ~
        home = Path.home()
        test_dir = home / ".test_upload_workspace"
        test_dir.mkdir(exist_ok=True)
        test_file = test_dir / "test.py"
        test_file.write_text("content")

        try:
            result = upload_to_workspace(
                local_path="~/.test_upload_workspace/test.py",
                workspace_path="/Workspace/Users/test@example.com/test.py",
            )

            assert result.total_files == 1
            mock_client.workspace.upload.assert_called_once()
        finally:
            # Cleanup
            test_file.unlink()
            test_dir.rmdir()
