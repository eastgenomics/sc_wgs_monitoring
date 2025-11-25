import datetime
import logging
from pathlib import Path
import sys
from typing import List, Tuple

import dxpy
from dxpy.bindings import DXProject
from dxpy.bindings.dxapp import DXApp
from sqlalchemy.schema import Table
from sqlalchemy.orm import Session

from sc_wgs_monitoring import check, dnanexus, db, logger, utils

base_log = logging.getLogger("basic")


def setup_environment(
    **args,
) -> Tuple[
    dict,
    str,
    str,
    str,
    Session,
    Table,
    DXApp,
    DXProject,
    datetime.date,
    str,
    str,
]:
    """Setup the environment befor processing anything i.e. load the config
    files, connect to DNAnexus, connect to the database

    Returns
    -------
    tuple
        Tuple containing all the variables for the rest of the script

    Raises
    ------
    AssertionError
        Raised if:
        - time_to_check doesn't end with s, m, h, d
        - -s, -j, -d weren't specified in the command line
    """

    (
        dx_token,
        slack_token,
        slack_log_channel,
        slack_alert_channel,
        sc_wgs_db,
        postgres_user,
        postgres_pwd,
    ) = utils.load_env_variables()

    config_data = utils.load_config(args["config"])

    if args["time_to_check"]:
        assert args["time_to_check"].endswith(
            ("s", "m", "h", "d")
        ), "The time_to_check argument doesn't end with one of s|m|h|d"

    # override the values in the config file if the cli was used to override
    for config_key in config_data:
        config_override_value = args.get(config_key, None)

        if config_override_value:
            config_data[config_key] = config_override_value
        else:
            if config_key in config_data["workbook_inputs"]:
                config_data["workbook_inputs"][
                    config_key
                ] = config_override_value

    # Database setup things
    session, meta = db.connect_to_db(
        postgres_user,
        postgres_pwd,
        sc_wgs_db,
    )
    sc_wgs_table = meta.tables["sc_wgs_data"]

    # DNAnexus setup things
    dnanexus.login_to_dnanexus(dx_token)
    sd_wgs_project = dxpy.bindings.DXProject(config_data["project_id"])
    dxpy.set_workspace_id(sd_wgs_project.id)
    sc_wgs_workbook_app = dxpy.bindings.dxapp.DXApp(
        dxid=config_data["sd_wgs_workbook_app_id"]
    )

    # setup dates
    date = datetime.date.today()
    date_str = date.strftime("%y%m%d")
    now = datetime.datetime.now().strftime("%y%m%d | %H:%M:%S")

    if (
        not args["start_jobs"]
        and not args["download_job_output"]
        and not args["daily_report"]
    ):
        msg = "No processing type specified, please use -s, -j or -d"
        base_log.error(msg)
        raise AssertionError(msg)

    return (
        config_data,
        slack_token,
        slack_log_channel,
        slack_alert_channel,
        session,
        sc_wgs_table,
        sc_wgs_workbook_app,
        sd_wgs_project,
        date,
        date_str,
        now,
    )


def get_inputs(clingen_input_location: str, patterns: dict, **args) -> List:
    """Get the inputs and do some basic checking

    Parameters
    ----------
    clingen_input_location : str
        Clingen input location to look for the input files
    patterns : dict
        Dict containing the patterns to look for and their file type

    Returns
    -------
    list
        List of all files detected to be correct input files

    Raises
    ------
    AssertionError
        If provided dnanexus ids aren't correct
    AssertionError
        If one of the provided file paths doesn't point to an existing
        file
    """

    new_files = None

    # handle given dnanexus ids
    if args["dnanexus_ids"]:
        if all(
            [check.check_dnanexus_id(file) for file in args["dnanexus_ids"]]
        ):
            new_files = [dxpy.DXFile(file) for file in args["dnanexus_ids"]]
        else:
            msg = (
                f"Provided files {args['dnanexus_ids']} are not all "
                "DNAnexus file ids"
            )
            base_log.error(msg)
            raise AssertionError(msg)

    else:
        # handle specified local files to process
        if args["local_files"]:
            if not all(
                [
                    check.check_if_file_exists(file)
                    for file in args["local_files"]
                ]
            ):
                msg = (
                    "One of the files given doesn't exist "
                    f"{'|'.join([file for file in args["local_files"]])}"
                )
                base_log.error(msg)
                raise AssertionError(msg)

            new_files = [Path(file) for file in args["local_files"]]

        # handle file detection
        else:
            new_files = utils.find_input_files_in_clingen_input_location(
                clingen_input_location, patterns
            )

    return new_files


def check_inputs(new_files: list, patterns: dict) -> tuple:
    """Check the inputs before importing in the database

    Parameters
    ----------
    new_files : list
        List of files to check
    patterns : dict
        Dict containing the patterns to look for and their file type

    Returns
    -------
    tuple
        Tuple containing:
        - Dict containing the samples and their files
        - List of errors to capture for future exception raising

    Raises
    ------
    AssertionError
        When no files are left to process after removing files with
        issues
    """
    silent_errors = []

    # group files per id
    sample_files = utils.get_sample_id_from_files(new_files, patterns)

    # check if the files have expected suffixes
    files_with_incorrect_file_name = [
        file.name
        for file in new_files
        if not check.check_file_input_name_is_correct(file.name, patterns)
    ]

    if len(files_with_incorrect_file_name) != 0:
        msg = (
            "The set of provided files is not correct. Expected files "
            f"with the following patterns: {patterns}. "
            f"Got {" | ".join([file for file in files_with_incorrect_file_name])}"
        )
        base_log.error(msg)
        silent_errors.append(msg)

    samples_with_incomplete_sets = [
        (sample, files)
        for sample, files in sample_files.items()
        if len(files) != 3
    ]

    if len(samples_with_incomplete_sets) != 0:
        msg = "The following samples do not have 3 files:\n"

        for sample, files in samples_with_incomplete_sets:
            msg += f"- {sample}\n"

            for file in files:
                msg += f"  - {file}\n"

        base_log.error(msg.rstrip("\n"))
        silent_errors.append(msg.rstrip("\n"))

        samples_with_incomplete_sets = [
            sample for sample, files in samples_with_incomplete_sets
        ]

    sample_files, removed_samples = utils.remove_samples(
        sample_files,
        samples_with_incomplete_sets,
        files_with_incorrect_file_name,
    )

    if len(sample_files) == 0:
        msg = "No samples are left after removing samples with issues.\n"

        if len(files_with_incorrect_file_name) != 0:
            msg += (
                "Removed samples due wrong file name: "
                f"{' | '.join(removed_samples)}\n"
            )

        if len(samples_with_incomplete_sets) != 0:
            msg += (
                "Removed samples due to incomplete set of files: "
                f"{' | '.join(samples_with_incomplete_sets)}\n"
            )

        base_log.error(msg)
        raise AssertionError(msg)

    message = ""

    # build report message for samples and their files
    for sample, files in sample_files.items():
        message += f"- {sample}\n"

        for file in files:
            message += f"  - {file.name}\n"

    msg = (
        f"Detected {len(sample_files)} sample(s) with the following "
        f"files for processing:\n{message}"
    )
    logger.log(base_log, "info", msg)

    return sample_files, silent_errors


def check_processed_samples(
    sample_files: dict, session: Session, sc_wgs_table: Table, **args
) -> dict:
    """Check the samples to see if they have already been processed

    Parameters
    ----------
    sample_files : dict
        Dict of samples and their files
    session : Session
        Session object to query the database from
    sc_wgs_table : Table
        Table object containing the tracking data

    Returns
    -------
    dict
        Dict containing the samples, their files and metadata to indicate
        whether they have been processed in the past
    """

    sample_files_tagged = db.tag_processed_samples(
        session, sc_wgs_table, sample_files
    )

    if all([data["processed"] for data in sample_files_tagged.values()]) and (
        not args["dnanexus_ids"] and not args["local_files"]
    ):
        msg = "All detected samples have already been processed"
        logger.log(base_log, "warning", msg)
        sys.exit()

    processed_samples = [
        sample
        for sample, data in sample_files_tagged.items()
        if data["processed"] is True
    ]

    if processed_samples:
        msg = (
            "The following samples are already in the database:\n - "
            f"{"\n - ".join(processed_samples)}\n"
        )
        logger.log(base_log, "warning", msg)

    return sample_files_tagged


def insert_inputs_in_db(
    sample_files_tagged: dict,
    date: datetime.date,
    date_str: str,
    session: Session,
    sc_wgs_table: Table,
    **args,
) -> dict:
    """Insert the samples and related metadata in the database

    Parameters
    ----------
    sample_files_tagged : dict
        Dict containing the samples and metadata
    date : datetime.date
        Date object
    date_str : str
        String of the date
    session : Session
        Session object
    sc_wgs_table : Table
        Table object

    Returns
    -------
    dict
        Dict containing the samples and metadata prepared with DNAnexus
        information rather than local paths

    Raises
    ------
    Exception
        When the processed status is neither True or False
    """

    db_data = []
    new_sample_files_tagged = {}

    for sample_id, data in sample_files_tagged.items():
        if args["dnanexus_ids"]:
            new_sample_files_tagged.setdefault(sample_id, data)
            # add the folder key to match the data structure in the
            # else
            new_sample_files_tagged[sample_id][
                "folder"
            ] = f"/{date_str}/{sample_id}"
            # make DXFiles for use later if necessary
            new_sample_files_tagged[sample_id]["files"] = [
                (dxpy.DXFile(file) if type(file) is not dxpy.DXFile else file)
                for file in sample_files_tagged[sample_id]["files"]
            ]

        if data["processed"] is True:
            # overwrite existing data in the db
            if args["dnanexus_ids"] or args["local_files"]:
                db.update_in_db(
                    session,
                    sc_wgs_table,
                    sample_id,
                    {
                        "processing_status": "Preprocessing before job start",
                        "date": date_str,
                    },
                )

                msg = f"Updated data for {sample_id} in db"
                logger.log(base_log, "info", msg)

        elif data["processed"] is False:
            sample_data = db.prepare_data_for_import(
                sc_wgs_table,
                **{
                    "referral_id": sample_id,
                    "date": date,
                    "processing_status": "Preprocessing before job start",
                },
            )
            db_data.append(sample_data)

        else:
            base_log.error("Error when checking for processed samples")
            raise Exception

    if db_data:
        db.insert_in_db(session, sc_wgs_table, db_data)

    return new_sample_files_tagged


def remove_pid(sample_files_tagged: dict, pid_div_id: str, **args) -> dict:
    """Remove pid div block from the supplementary HTML files

    Parameters
    ----------
    sample_files_tagged : dict
        Dict containing the samples and their metadata
    pid_div_id : str
        String of the id of the div containing PID

    Returns
    -------
    dict
        Dict with the samples and the new HTML files without PID
    """

    msg = (
        "Removing pid divs, this step can take some time "
        "depending on the number of files to process..."
    )
    logger.log(base_log, "info", msg)

    new_sample_files = {}

    for sample_id, data in sample_files_tagged.items():
        files = []
        new_sample_files[sample_id] = data

        for file in data["files"]:
            # remove the pid div only on unprocessed samples or if
            # local files are specified
            if args["local_files"] or data["processed"] is False:
                if file.name.endswith(".supplementary.html"):
                    files.append(
                        (
                            file,
                            (
                                utils.remove_pid_div_from_supplementary_file(
                                    file,
                                    pid_div_id,
                                )
                            ),
                        )
                    )
                else:
                    files.append(file)

        if files and data["processed"] is False:
            new_sample_files[sample_id]["files"] = files

    return new_sample_files
