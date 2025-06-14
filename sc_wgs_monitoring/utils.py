import importlib.util
from pathlib import Path
from typing import Dict
import re
import sys

from bs4 import BeautifulSoup


def load_config(config_file) -> Dict:
    """Load the configuration file

    Returns
    -------
    Dict
        Dict containing the keys for the various parameters used
    """

    config_path = Path(config_file)

    spec = importlib.util.spec_from_file_location(
        config_path.name.replace(".py", ""), config_path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[config_path.name] = module
    spec.loader.exec_module(module)
    return module.CONFIG


def get_sample_id_from_files(files: list, patterns: list) -> Dict:
    """Get the sample id from the new files detected and link sample ids to
    their files

    Parameters
    ----------
    files : list
        List of DNAnexus file objects
    patterns : list
        Expected patterns for the file name

    Returns
    -------
    Dict
        Dict containing the sample id and their files
    """

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


def remove_pid_div_from_supplementary_file(
    file: str, pid_div_id: str
) -> BeautifulSoup:
    """Remove the div with personal identifiable data from the supplementary
    HTML

    Parameters
    ----------
    file : str
        File path to the supplementary HTML file
    pid_div_id : str
        Id of the div to look for in the HTML file

    Returns
    -------
    BeautifulSoup
        BeautifulSoup object containing the HTML supplementary without the PID
        div
    """

    with open(file) as f:
        soup = BeautifulSoup(f, "html.parser")
        pid_div = soup.find("div", id=pid_div_id)

        if pid_div is not None:
            pid_div.decompose()

        return soup


def find_files_in_clingen_input_location(location: str) -> list:
    """Return all files present in the given location

    Parameters
    ----------
    location : str
        Path in which to look for files

    Returns
    -------
    list
        List of all the files present at that location
    """

    return [path for path in Path(location).iterdir()]


def convert_time_to_epoch(time: str) -> int:
    """Convert provided time from the user to Epoch

    Parameters
    ----------
    time : str
        String representing the time to convert

    Returns
    -------
    int
        Integer representing Epoch

    Raises
    ------
    AssertionError
        If the time doesn't end with the appropriate suffix
    """

    match = re.search(r"[smhd]", time)

    if not match:
        raise AssertionError("No handled unit detected")

    unit = match.group(0)

    time_without_unit = int(time.strip(unit))

    if unit == "s":
        return time_without_unit
    elif unit == "m":
        return time_without_unit * 60
    elif unit == "h":
        return time_without_unit * 3600
    elif unit == "d":
        return time_without_unit * 3600 * 24
