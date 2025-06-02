import json
from typing import Dict
import re

from bs4 import BeautifulSoup


def load_config(config_file) -> Dict:
    """Load the configuration file

    Returns
    -------
    Dict
        Dict containing the keys for the various parameters used
    """

    with open(config_file) as f:
        config_data = json.loads(f.read())

    return config_data


def get_sample_id_from_files(files: list) -> Dict:
    """Get the sample id from the new files detected and link sample ids to
    their files

    Parameters
    ----------
    files : list
        List of DNAnexus file objects

    Returns
    -------
    Dict
        Dict containing the sample id and their files
    """

    patterns = [
        r"[-_]reported_structural_variants\..*\.csv",
        r"[-_]reported_variants\..*\.csv",
        r"\..*\.supplementary\.html",
    ]

    detected_sample_ids = set()

    for file in files:
        file_name = file.name
        # we have some .html files that we don't want to match
        matched_pattern = None

        for pattern in patterns:
            match = re.search(pattern, file_name)

            if match:
                matched_pattern = match

        if matched_pattern:
            detected_sample_ids.add(file_name[: matched_pattern.start()])

    file_dict = {}

    for sample_id in detected_sample_ids:
        file_dict.setdefault(sample_id, [])

        for file in files:
            if sample_id in file.name:
                file_dict[sample_id].append(file)

    assert all(file_dict)

    return file_dict


def remove_pid_div_from_supplementary_file(file):
    soup = BeautifulSoup(file, "html.parser")
    return soup
