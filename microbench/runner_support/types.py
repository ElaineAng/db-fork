from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dblib.file_copy import FileCopyToolSuite


@dataclass
class BackendInfo:
    """Backend-specific connection and project information."""

    # NOTE: Default URI isn't the URI for the benchmark database. The benchmark
    # runs on a separate database
    default_uri: str = ""
    default_branch_id: str = ""
    default_branch_name: str = ""
    neon_project_id: Optional[str] = None
    xata_project_id: Optional[str] = None
    file_copy_info: Optional["FileCopyToolSuite.FileCopyInfo"] = None
    setup_branches: Optional[list[str]] = None
