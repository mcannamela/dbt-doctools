from typing import Tuple, List

import dbt
from dbt import flags
from dbt.config import read_user_config, RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.graph import Graph
from dbt.main import parse_args, adapter_management


def obtain_manifest_and_graph_and_config(dbt_command: str = 'compile', dbt_args: List[str] = tuple(),
                                         dbt_command_args: List[str] = tuple()) -> Tuple[Manifest, Graph, RuntimeConfig]:
    """Get a fully-loaded dbt manifest by hijacking a compile task

    Much of this is copy-pasta from dbt's own interals, found by following the main entrypoint.
    """
    parsed = parse_args(list(dbt_args) + [dbt_command] + list(dbt_command_args))

    # Set flags from args, user config, and env vars
    user_config = read_user_config(flags.PROFILES_DIR)
    flags.set_from_args(parsed, user_config)
    dbt.tracking.initialize_from_flags()

    with adapter_management():
        task = parsed.cls.from_args(args=parsed)
        task.load_manifest()
        task.compile_manifest()
    m = task.manifest
    g = task.graph
    c = task.config
    return m, g, c
