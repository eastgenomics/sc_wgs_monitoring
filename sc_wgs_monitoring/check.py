import datetime
from pathlib import Path
import re


def check_dnanexus_id(string: str) -> bool:
    """Check if the given string is a dnanexus id

    Parameters
    ----------
    string : str
        String to check

    Returns
    -------
    bool
        Result of the regex
    """

    return (
        True
        if re.search(r"file|project|job-[0-9a-zA-Z]{24}", string)
        else False
    )


def check_file_input_name_is_correct(string: str, patterns: list) -> bool:
    """Check if the given string matches the expected pattern for the input
    for the workbook job

    Parameters
    ----------
    string : str
        String to check
    patterns : list
        Expected patterns for the file name

    Returns
    -------
    bool
        Result of the regexes
    """

    for pattern in patterns:
        if re.search(pattern, string):
            return True

    return False


def check_if_file_exists(path: str) -> bool:
    """Check if a path exists

    Parameters
    ----------
    path : str
        Path to check

    Returns
    -------
    bool
        Result of the check
    """

    return True if Path(path).exists() else False


def filter_file_using_time_to_check(file: Path, time_to_check: int):
    """Check if the given file has been modified in the last x seconds

    Parameters
    ----------
    file : Path
        File to check
    time_to_check : int
        Integer representing the number of seconds

    Returns
    -------
    bool
        Bool to represent if the file has been modified in the last x seconds
    """

    now = datetime.datetime.today().timestamp()
    last_modified_time = file.stat().st_mtime

    if now - last_modified_time <= time_to_check:
        return True
    else:
        return False
