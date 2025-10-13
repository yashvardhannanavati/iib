# SPDX-License-Identifier: GPL-3.0-or-later
<<<<<<< HEAD
import json
=======
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
import logging
import os
import tempfile
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.build import (
    _add_label_to_index,
<<<<<<< HEAD
    _cleanup,
    _update_index_image_build_state,
)
from iib.workers.tasks.celery import app
=======
    _build_image,
    _cleanup,
    _create_and_push_manifest_list,
    _push_image,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
from iib.workers.tasks.git_utils import (
    create_mr,
    push_configs_to_git,
    clone_git_repo,
    get_git_token,
    get_last_commit_sha,
)
from iib.workers.tasks.konflux_utils import wait_for_pipeline_completion, find_pipelinerun
from iib.workers.tasks.opm_operations import (
<<<<<<< HEAD
=======
    opm_registry_add_fbc_fragment,
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
    Opm,
    opm_registry_add_fbc_fragment_containerized,
)
from iib.workers.tasks.oras_utils import (
    get_oras_artifact,
    get_indexdb_artifact_pullspec,
    verify_indexdb_cache_for_image,
    refresh_indexdb_cache_for_image,
    push_oras_artifact,
)
from iib.workers.tasks.utils import (
    get_resolved_image,
    prepare_request_for_build,
    request_logger,
    set_registry_token,
    RequestConfigFBCOperation,
<<<<<<< HEAD
    change_dir,
=======
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
)

__all__ = ['handle_containerized_fbc_operation_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_fbc_operation_request",
    attributes=get_binary_versions(),
)
def handle_containerized_fbc_operation_request(
    request_id: int,
    fbc_fragments: List[str],
    from_index: Optional[str] = None,
    binary_image: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    build_tags: Optional[Set[str]] = None,
    add_arches: Optional[Set[str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    used_fbc_fragment: bool = False,
) -> None:
    """
    Add fbc fragments to an fbc index image.

    :param list fbc_fragments: list of fbc fragments that need to be added to final FBC index image
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    :param bool used_fbc_fragment: flag indicating if the original request used fbc_fragment
        (single) instead of fbc_fragments (array). Used for backward compatibility.
    """
    _cleanup()
    set_request_state(request_id, 'in_progress', 'Resolving the fbc fragments')

    # Resolve all fbc fragments
    resolved_fbc_fragments = []
    for fbc_fragment in fbc_fragments:
        with set_registry_token(overwrite_from_index_token, fbc_fragment, append=True):
            resolved_fbc_fragment = get_resolved_image(fbc_fragment)
            resolved_fbc_fragments.append(resolved_fbc_fragment)

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            fbc_fragments=fbc_fragments,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    Opm.set_opm_version(from_index_resolved)

    # Store all resolved fragments
    prebuild_info['fbc_fragments_resolved'] = resolved_fbc_fragments

    # For backward compatibility, only populate old fields if original request used fbc_fragment
    # This flag should be passed from the API layer
    if used_fbc_fragment and resolved_fbc_fragments:
        prebuild_info['fbc_fragment_resolved'] = resolved_fbc_fragments[0]

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        # if not verify_indexdb_cache_for_image(from_index):
        #     refresh_indexdb_cache_for_image(from_index)

        artifact_ref = get_indexdb_artifact_pullspec(from_index)
        artifact_dir = get_oras_artifact(
            artifact_ref,
            temp_dir,
        )

        # TODO - FIX the DB PATH
<<<<<<< HEAD
        artifact_index_db_file = os.path.join(
            artifact_dir, get_worker_config()['temp_index_db_path']
        )

        log.debug("Artifact DB path %s", artifact_index_db_file)
        if not os.path.exists(artifact_index_db_file):
            log.error("Artifact DB file not found at %s", artifact_index_db_file)
            raise IIBError(f"Artifact DB file not found at {artifact_index_db_file}")
=======

        artifact_index_db_file = os.path.join(artifact_dir, "var/lib/iib/_hidden/do.not.edit.db")
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)

        index_git_repo = index_to_gitlab_push_map[from_index]
        token_name, git_token = get_git_token(index_git_repo)
        branch = prebuild_info['ocp_version']

        local_git_repo_path = f"{temp_dir}/git/{branch}"
        os.makedirs(local_git_repo_path, exist_ok=True)

<<<<<<< HEAD
        # TODO - GitClone takes time - can we keep the copy and just pull the difference? (6min on dev-env)
=======
        # TODO - GitClone takes time - can we keep the copy and just pull the difference?
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
        clone_git_repo(index_git_repo, branch, token_name, git_token, local_git_repo_path)

        localized_git_catalog_path = os.path.join(local_git_repo_path, 'configs')
        if not os.path.exists(localized_git_catalog_path):
            raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")

<<<<<<< HEAD
=======
        log.debug("Artifact DB path %s", artifact_index_db_file)
        if not os.path.exists(artifact_index_db_file):
            raise IIBError(f"Artifact DB file not found at {artifact_index_db_file}")

>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
        # Process all resolved fbc fragments at once
        tmp_catalog_path, tmp_indexdb_path, _ = opm_registry_add_fbc_fragment_containerized(
            request_id=request_id,
            temp_dir=temp_dir,
            from_index_configs_dir=localized_git_catalog_path,
            binary_image=binary_image_resolved,
            fbc_fragments=resolved_fbc_fragments,
            overwrite_from_index_token=overwrite_from_index_token,
            generate_cache=False,
            index_db_path=artifact_index_db_file,
        )

        _add_label_to_index(
            'com.redhat.index.delivery.version',
            prebuild_info['ocp_version'],
            local_git_repo_path,
            'index.Dockerfile',
        )

        _add_label_to_index(
            'com.redhat.index.delivery.distribution_scope',
            prebuild_info['distribution_scope'],
            local_git_repo_path,
            'index.Dockerfile',
        )

        log.info("Commiting changes to Git repository. Triggering KONFLUX pipeline.")
        if not overwrite_from_index_token:
            result = create_mr(
                request_id=request_id,
                local_repo_path=local_git_repo_path,
                repo_url=index_git_repo,
                branch=branch,
                commit_message=f"Commit for request {request_id}",
            )

<<<<<<< HEAD
            last_commit_sha = get_last_commit_sha(local_repo_path=local_git_repo_path)
            if result:
                relative_db_path = get_worker_config()['temp_index_db_path']
                cwd = tmp_indexdb_path[: -len(relative_db_path)]
                # TODO - When we will use the Token here - what is the use-case?
                with change_dir(cwd):
                    push_oras_artifact(artifact_ref=artifact_ref, local_path=relative_db_path)
=======
            if result:
                # TODO - When we will use the Token here - what is the use-case?
                push_oras_artifact(artifact_ref=artifact_ref, local_path=tmp_indexdb_path)
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)

    set_request_state(request_id, 'in_progress', 'Waiting on KONFLUX build.')

    #####
    arches = prebuild_info['arches']

<<<<<<< HEAD
    pipelines = find_pipelinerun(last_commit_sha)

    wait_for_pipeline_completion(pipelines[0])
    # TODO - GET this from KONFLUX build
    # output_pull_spec = _create_and_push_manifest_list(request_id, arches, build_tags)
    ######
    #
    # _update_index_image_pull_spec(
    #     output_pull_spec=output_pull_spec,
    #     request_id=request_id,
    #     arches=arches,
    #     from_index=from_index,
    #     overwrite_from_index=overwrite_from_index,
    #     overwrite_from_index_token=overwrite_from_index_token,
    #     resolved_prebuild_from_index=from_index_resolved,
    #     add_or_rm=True,
    # )
=======
    last_commit_sha = get_last_commit_sha(local_repo_path=local_git_repo_path)

    pipelines = find_pipelinerun(last_commit_sha)

    wait_for_pipeline_completion()
    # TODO - GET this from KONFLUX build
    # output_pull_spec = _create_and_push_manifest_list(request_id, arches, build_tags)
    ######

    _update_index_image_pull_spec(
        output_pull_spec=output_pull_spec,
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        overwrite_from_index=overwrite_from_index,
        overwrite_from_index_token=overwrite_from_index_token,
        resolved_prebuild_from_index=from_index_resolved,
        add_or_rm=True,
    )
>>>>>>> 9539ebd (Handling of fbc-operations for containerized IIB)
    _cleanup()
    set_request_state(
        request_id,
        'complete',
        f"The {len(resolved_fbc_fragments)} FBC fragment(s) were successfully added "
        "in the index image",
    )
