from typing import Dict, Any, List
import os

from ..constants.constants import ARG_NOMQUAM_THRESHOLD, ARG_SEED

def parse_config(
        list_args_required: List[str] = [], list_args_optional: List[str] = [])\
            -> Dict[str, Any]:
    config = os.environ.copy()

    for arg in list_args_required:
        assert arg in config, f"Parameter '{arg}' required but not set."

    if (ARG_NOMQUAM_THRESHOLD in list_args_optional) or (ARG_NOMQUAM_THRESHOLD in list_args_required):
        if (ARG_NOMQUAM_THRESHOLD not in config) or (config[ARG_NOMQUAM_THRESHOLD] == "-1"):
            config[ARG_NOMQUAM_THRESHOLD] = -1
        else:
            config[ARG_NOMQUAM_THRESHOLD] = float(config[ARG_NOMQUAM_THRESHOLD])

    if ARG_SEED in list_args_required:
        config[ARG_SEED] = int(config[ARG_SEED])

    return config
