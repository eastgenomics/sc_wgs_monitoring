import concurrent
import datetime
import importlib.util
from pathlib import Path, PosixPath
from typing import Dict
import os
import re
from shutil import move
import sys

from bs4 import BeautifulSoup
import dxpy
from sqlalchemy.schema import Table
from sqlalchemy.orm import Session

from sc_wgs_monitoring import check, db, dnanexus


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


def load_env_variables() -> tuple:
    """Load the environment variables required for connecting to DNAnexus, the
    WGS database and to interact with Slack

    Returns
    -------
    tuple
        Tuple containing the variables from the environment

    Raises
    ------
    KeyError
        If a key doesn't exist, raise an error
    """

    try:
        dx_token = os.environ["DNANEXUS_TOKEN"]
        slack_token = os.environ["SLACK_TOKEN"]
        slack_log_channel = os.environ["SLACK_LOG_CHANNEL"]
        slack_alert_channel = os.environ["SLACK_ALERT_CHANNEL"]
        sc_wgs_db = os.environ["DB_NAME"]
        postgres_user = os.environ["DB_USER"]
        postgres_pwd = os.environ["DB_PASSWORD"]

    except KeyError as e:
        key = e.args[0]

        raise KeyError(
            f"Unable to import {key} from environment, is an .env file "
            "present or env variables set?"
        )

    return (
        dx_token,
        slack_token,
        slack_log_channel,
        slack_alert_channel,
        sc_wgs_db,
        postgres_user,
        postgres_pwd,
    )


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

    for sample_id, sample_files in file_dict.items():
        assert (
            len(sample_files) == 3
        ), f"{sample_id} doesn't have 3 files associated: {sample_files}"

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


def write_file(file_name: str, file_content: str):
    """Write a file to give location and given content

    Parameters
    ----------
    file_name : str
        File name + path
    file_content : str
        Already formatted file content
    """

    with open(file_name, "w") as f:
        f.write(str(file_content))


def create_output_folder(sample_id: str, location: str) -> PosixPath:
    """Create the output folder for the WGS workbook jobs

    Parameters
    ----------
    sample_id : str
        Sample id
    location : str
        Root location for all the output folder

    Returns
    -------
    PosixPath
        Path of the created folder
    """

    today = datetime.date.today().strftime("%Y-%m")
    output_folder = Path(f"{location}/{today}/{sample_id}")
    output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder


def download_file_and_update_db(
    session: Session,
    table: Table,
    sample_id: str,
    download_location: str,
    job: dxpy.DXJob,
):
    """Given a job, download the dnanexus output file in given clingen
    location. Update database with the download location.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object
    table : Table
        SQLAlchemy Table object
    sample_id : str
        Sample id
    download_location : str
        Path to where the output will be downloaded to
    job : dxpy.DXJob
        DXJob object for the workbook job for the given sample
    """

    output_id = dnanexus.get_output_id(job.describe())
    dxpy.bindings.dxfile_functions.download_dxfile(
        output_id,
        download_location / f"{sample_id}.xlsx",
    )

    if Path(download_location / f"{sample_id}.xlsx").exists():
        db.update_in_db(
            session,
            table,
            sample_id,
            {
                "workbook_clingen_location": f"{download_location}",
                "processing_status": "Workbook downloaded",
            },
        )
    else:
        raise FileNotFoundError(
            f"{Path(download_location / f'{sample_id}.xlsx')} wasn't downloaded"
        )


def move_files(location: str, *files_to_move) -> None:
    """Move list of files to given location

    Parameters
    ----------
    location : str
        Location in which to move the data in
    files_to_move : *args
        Iterable of files to move
    """

    new_folder = Path(location)

    for file in files_to_move:
        if type(file) is not PosixPath:
            for f in file:
                if type(f) is PosixPath:
                    move(f, new_folder / f.name)
        else:
            move(file, new_folder / file.name)


def start_parallel_workbook_jobs(
    session: Session, table: Table, args_for_starting_jobs: list
) -> list:
    """Given a list of argument dict per job, start the jobs in parallel

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object
    table : Table
        SQLAlchemy Table object
    args_for_starting_jobs : list
        List of dict containing the arguments for starting a job

    Returns
    -------
    list
        List of job objects created by DNAnexus
    """

    sample_jobs = {}
    errors = []

    # start the jobs
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Start the jobs and mark each future with the job objects
        future_to_job = {
            executor.submit(
                dnanexus.start_wgs_workbook_job,
                inputs,
                app,
                job_name,
                output_folder,
            ): job_name
            for (
                inputs,
                app,
                job_name,
                output_folder,
            ) in args_for_starting_jobs
        }

        for future in concurrent.futures.as_completed(future_to_job):
            sample_id = future_to_job[future]

            try:
                job = future.result()
                sample_jobs[sample_id] = job.id

            except Exception as exc:
                errors.append(f"{sample_id} generated an exception: {exc}")
                continue

            db.update_in_db(
                session,
                table,
                sample_id,
                {
                    "job_id": job.id,
                    "processing_status": "Job started",
                    "workbook_dnanexus_location": f"{job.project}:{job.folder}",
                },
            )

    return sample_jobs, errors


def monitor_jobs(session: Session, table: Table, jobs: list) -> list:
    """Monitor the running jobs and act accordingly when they finish.

    Parameters
    ----------
    session : Session
        SQLAlchemy Session object
    table : Table
        SQLAlchemy Table object
    jobs : list
        List of job objects to monitor

    Returns
    -------
    list
        List of jobs that failed
    """

    job_failures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Start the jobs and mark each future with the job objects
        future_to_completed_job = {
            executor.submit(check.check_if_job_is_done, job_id): job_id
            for job_id in jobs
        }

        for future in concurrent.futures.as_completed(future_to_completed_job):
            job = dxpy.DXJob(future_to_completed_job[future])
            sample_id = job.name
            job_status = job.state

            if future.result() is False:
                job_failures.append(
                    f"- `{job.id}` has been running for more than 1h"
                )
                processing_status = "Job has been running for more than an 1h"
            else:
                if job_status != "done":
                    job_failures.append(f"- `{job.id}` | {job_status}")
                    processing_status = "Job didn't finish successfully"

                else:
                    processing_status = "Job finished"

            # check the job statuses, update the db and download the files in
            # the appropriate locations
            db.update_in_db(
                session,
                table,
                sample_id,
                {
                    "job_status": job_status,
                    "processing_status": processing_status,
                },
            )

    return job_failures
