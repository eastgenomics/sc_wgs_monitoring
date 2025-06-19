import datetime
import os
from pathlib import Path
import re
import time

import dxpy


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


def check_if_job_is_done(job_id: str) -> bool:
    """Check if the job from a given job id has finished

    Parameters
    ----------
    job_id : str
        Job id of the job to check

    Returns
    -------
    bool
        Boolean indicating the job status
    """

    start_time = time.time()

    while True:
        # have to redefine the job object to update the job state
        job = dxpy.DXJob(job_id)

        if job.state not in ["runnable", "running"]:
            return True
        else:
            # if it's been more than 10 minutes
            if time.time() - start_time >= 600:
                return False

            os.sleep(15)
