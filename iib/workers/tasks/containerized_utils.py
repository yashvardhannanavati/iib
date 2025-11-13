# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains utility functions for containerized IIB operations."""
import json
import logging
import os
from typing import Dict, Optional

from iib.exceptions import IIBError

log = logging.getLogger(__name__)


def write_build_metadata(
    local_repo_path: str,
    opm_version: str,
    ocp_version: str,
    distribution_scope: str,
    binary_image: str,
    request_id: int,
) -> None:
    """
    Write build metadata file for Konflux build task.

    This function creates a JSON metadata file that contains information needed by the
    Konflux build task, including OPM version, labels, binary image, and request ID.

    :param str local_repo_path: Path to local Git repository
    :param str opm_version: OPM version string (e.g., "opm-1.40.0")
    :param str ocp_version: OCP version (e.g., "v4.19")
    :param str distribution_scope: Distribution scope (e.g., "PROD")
    :param str binary_image: Binary image pullspec
    :param int request_id: Request ID
    """
    metadata = {
        'opm_version': opm_version,
        'labels': {
            'com.redhat.index.delivery.version': ocp_version,
            'com.redhat.index.delivery.distribution_scope': distribution_scope,
        },
        'binary_image': binary_image,
        'request_id': request_id,
    }

    metadata_path = os.path.join(local_repo_path, '.iib-build-metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    log.info('Written build metadata to %s', metadata_path)


def cleanup_on_failure(
    mr_details: Optional[Dict[str, str]],
    index_git_repo: Optional[str],
    request_id: int,
    from_index: str,
    index_repo_map: Dict[str, str],
    reason: str = "error",
) -> None:
    """
    Clean up Git changes on failure by either closing an MR or reverting a commit.

    If a merge request was created, it will be closed (since the commit is only in a
    feature branch). If changes were pushed directly to the main branch, the commit
    will be reverted.

    :param Optional[Dict[str, str]] mr_details: Details of the merge request if one was created
    :param Optional[str] index_git_repo: URL of the Git repository
    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param Dict[str, str] index_repo_map: Mapping of index images to Git repositories
    :param str reason: Reason for the cleanup (used in log messages)
    """
    if mr_details and index_git_repo:
        # If we created an MR, just close it (commit is only in feature branch)
        log.info("Closing merge request due to %s", reason)
        try:
            from iib.workers.tasks.git_utils import close_mr

            close_mr(mr_details, index_git_repo)
            log.info("Closed merge request: %s", mr_details.get('mr_url'))
        except Exception as close_error:
            log.warning("Failed to close merge request: %s", close_error)
    elif index_git_repo:
        # If we pushed directly, revert the commit
        log.error("Reverting commit due to %s", reason)
        try:
            from iib.workers.tasks.git_utils import revert_last_commit

            revert_last_commit(
                request_id=request_id,
                from_index=from_index,
                index_repo_map=index_repo_map,
            )
        except Exception as revert_error:
            log.error("Failed to revert commit: %s", revert_error)

