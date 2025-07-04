import argparse
import datetime
import logging
from pathlib import Path
import sys

import dxpy

from sc_wgs_monitoring import check, dnanexus, db, logger, notifications, utils

logger.set_up_logger()
base_log = logging.getLogger("basic")


def main(**args):
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

    date = datetime.date.today().strftime("%y%m%d")
    now = datetime.datetime.now().strftime("%y%m%d | %H:%M:%S")

    header_msg = (
        f"{now} - :excel: Solid Cancer Workbooks :excel: - Command line: "
        f"`{' '.join(sys.argv)}`\n\n"
    )

    base_log.info(header_msg.replace("\n", "").replace(":excel:", ""))
    print(header_msg.replace("\n", "").replace(":excel:", ""), flush=True)

    if not args["start_jobs"] and not args["download_job_output"]:
        raise AssertionError(
            "No processing type specified, please use -s or -j"
        )

    # start WGS workbook jobs
    if args["start_jobs"]:
        new_files = None
        patterns = {
            r"[-_]reported_structural_variants\..*\.csv": "reported_structural_variants",
            r"[-_]reported_variants\..*\.csv": "reported_variants",
            r"\..*\.supplementary\.html": "supplementary_html",
        }

        # handle given dnanexus ids
        if args["dnanexus_ids"]:
            if all(
                [
                    check.check_dnanexus_id(file)
                    for file in args["dnanexus_ids"]
                ]
            ):
                new_files = [
                    dxpy.DXFile(file) for file in args["dnanexus_ids"]
                ]
            else:
                raise AssertionError(
                    f"Provided files {args['dnanexus_ids']} are not all "
                    "DNAnexus file ids"
                )

        else:
            # handle specified local files to process
            if args["local_files"]:
                if not all(
                    [
                        check.check_if_file_exists(file)
                        for file in args["local_files"]
                    ]
                ):
                    raise AssertionError(
                        "One of the files given doesn't exist "
                        f"{'|'.join([file for file in args["local_files"]])}"
                    )

                new_files = [Path(file) for file in args["local_files"]]

            # handle file detection
            else:
                new_files = utils.find_input_files_in_clingen_input_location(
                    config_data["clingen_input_location"], patterns
                )

        if new_files:
            # check if the files have expected suffixes
            if not all(
                check.check_file_input_name_is_correct(file.name, patterns)
                for file in new_files
            ):
                raise AssertionError(
                    "The set of provided files is not correct. Expected files "
                    f"with the following patterns: {[
                        r'[-_]reported_structural_variants\..*\.csv',
                        r'[-_]reported_variants\..*\.csv',
                        r'\..*\.supplementary\.html',
                    ]}. "
                    f"Got {" | ".join([file.name for file in new_files])}"
                )

            # group files per id as a sense check
            sample_files = utils.get_sample_id_from_files(new_files, patterns)

            message = ""

            # build report message for samples and their files
            for sample, files in sample_files.items():
                message += f"- {sample}\n"

                for file in files:
                    message += f"  - {file.name}\n"

            base_log.info(
                f"Detected the following files for processing:\n{message}"
            )
            print(
                f"Detected the following files for processing:\n{message}",
                flush=True,
            )

            sample_files_tagged = db.tag_processed_samples(
                session, sc_wgs_table, sample_files
            )

            if all(
                [data["processed"] for data in sample_files_tagged.values()]
            ) and (not args["dnanexus_ids"] and not args["local_files"]):
                base_log.warning(
                    "All detected samples have already been processed",
                )
                print(
                    "All detected samples have already been processed",
                    flush=True,
                )
                sys.exit()

            processed_samples = [
                sample
                for sample, data in sample_files_tagged.items()
                if data["processed"] is True
            ]

            if processed_samples:
                base_log.warning(
                    "The following samples are already in the database:\n - "
                    f"{"\n - ".join(processed_samples)}\n",
                )
                print(
                    "The following samples are already in the database:\n - "
                    f"{"\n - ".join(processed_samples)}\n",
                    flush=True,
                )

            db_data = []

            # if dnanexus file ids were specified no need for upload or removal
            # of pid
            if args["dnanexus_ids"]:
                for sample_id, data in sample_files_tagged.items():
                    # add the folder key to match the data structure in the
                    # else
                    sample_files_tagged[sample_id][
                        "folder"
                    ] = f"/{date}/{sample_id}"
                    # make DXFiles for use later if necessary
                    sample_files_tagged[sample_id]["files"] = [
                        (
                            dxpy.DXFile(file)
                            if type(file) is not dxpy.DXFile
                            else file
                        )
                        for file in sample_files_tagged[sample_id]["files"]
                    ]

                    if data["processed"]:
                        db.update_in_db(
                            session,
                            sc_wgs_table,
                            sample_id,
                            {
                                "processing_status": "Preprocessing before job start",
                                "date": date,
                            },
                        )
                    else:
                        sample_data = db.prepare_data_for_import(
                            sc_wgs_table,
                            **{
                                "referral_id": sample_id,
                                "date": date,
                                "processing_status": "Preprocessing before job start",
                            },
                        )
                        db_data.append(sample_data)

                db.insert_in_db(session, sc_wgs_table, db_data)

                # rename the variable in order to standadize naming afterward
                dnanexus_data = sample_files_tagged

            else:
                # remove the pid div only on unprocessed samples
                sample_files_tagged = {
                    sample_id: [
                        (
                            (
                                file,
                                (
                                    utils.remove_pid_div_from_supplementary_file(
                                        file,
                                        config_data["pid_div_id"],
                                    )
                                ),
                            )
                            if file.name.endswith(".supplementary.html")
                            else file
                        )
                        for file in data["files"]
                    ]
                    for sample_id, data in sample_files_tagged.items()
                    if data["processed"] is False or args["local_files"]
                }

                for sample_id in sample_files_tagged:
                    # prepare the import the unprocessed data in the database
                    sample_data = db.prepare_data_for_import(
                        sc_wgs_table,
                        **{
                            "referral_id": sample_id,
                            "date": date,
                            "processing_status": "Preprocessing before job start",
                        },
                    )
                    db_data.append(sample_data)

                if db_data:
                    db.insert_in_db(session, sc_wgs_table, db_data)

                    base_log.info("Inserted data in db")
                    print("Inserted data in db", flush=True)
                else:
                    base_log.warning("Data couldn't be imported")
                    print("Data couldn't be imported", flush=True)
                    sys.exit()

                dnanexus_data = dnanexus.upload_input_files(
                    date, sd_wgs_project, sample_files_tagged
                )
                base_log.info("Uploaded the files to DNAnexus")
                print("Uploaded the files to DNAnexus", flush=True)

            args_for_starting_jobs = []

            # organise data for preparation for starting the jobs
            for sample_id, data in dnanexus_data.items():
                args_for_starting_jobs.append(
                    dnanexus.prepare_inputs(
                        sample_id,
                        data["files"],
                        data["folder"],
                        patterns,
                        config_data["workbook_inputs"],
                        sc_wgs_workbook_app,
                    )
                )

            sample_jobs, errors = utils.start_parallel_workbook_jobs(
                session, sc_wgs_table, args_for_starting_jobs
            )

            if errors:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(errors)}",
                    slack_alert_channel,
                    slack_token,
                )

            base_log.info("Jobs started, starting job monitoring...")
            print("Jobs started, starting job monitoring...", flush=True)

            job_failures = utils.monitor_jobs(
                session, sc_wgs_table, sample_jobs.values()
            )

            # prepare the download of the output of the workbook
            for sample_id, job_id in sample_jobs.items():
                job = dxpy.DXJob(job_id)
                download_folder = utils.create_output_folder(
                    sample_id, config_data["clingen_download_location"]
                )

                # download the file to clingen and update db with the
                # location
                if job.state == "done":
                    utils.download_file_and_update_db(
                        session,
                        sc_wgs_table,
                        sample_id,
                        download_folder,
                        job,
                    )

                # move the input files only if they are not dnanexus files
                if not args["dnanexus_ids"]:
                    utils.move_files(
                        download_folder, *sample_files_tagged[sample_id]
                    )

            if job_failures:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(job_failures)}",
                    slack_alert_channel,
                    slack_token,
                )

        else:
            base_log.info("Couldn't find any files")
            print("Couldn't find any files", flush=True)

    # check jobs that have finished
    if args["download_job_output"]:
        if args["daily_report"]:
            now = datetime.datetime.strptime(now, "%y%m%d | %H:%M:%S")
            jobs_for_day = db.get_samples_for_the_day(
                session, sc_wgs_table, now - datetime.timedelta(days=1)
            )
            report = notifications.build_report(
                jobs_for_day,
                (now - datetime.timedelta(days=1)).strftime(
                    "%y%m%d | %H:%M:%S"
                ),
            )
            notifications.slack_notify(report, slack_log_channel, slack_token)
            base_log.info(report)
            print(report, flush=True)

        else:
            if args["dnanexus_ids"]:
                executions = [
                    {"id": job_id} for job_id in args["dnanexus_ids"]
                ]
            else:
                # store the job generator in a list so it doesn't get exhausted
                # the first time i go through it
                executions = list(
                    dxpy.bindings.find_executions(
                        executable=config_data["sd_wgs_workbook_app_id"],
                        project=sd_wgs_project,
                        created_after=f"-{args['time_to_check']}",
                    )
                )

            base_log.info(
                "Found the following jobs: "
                f"{" | ".join([job["id"] for job in executions])}"
            )
            print(
                "Found the following jobs: "
                f"{" | ".join([job["id"] for job in executions])}",
                flush=True,
            )

            for execution in executions:
                job = dxpy.DXJob(execution["id"])
                supplementary_html_file = dxpy.DXFile(
                    execution["runInput"]["supplementary_html"][
                        "$dnanexus_link"
                    ]
                )
                sample_id = supplementary_html_file.name.split(".")[0]

                # get job status
                job_status = job.state

                is_in_db = db.look_for_processed_samples(
                    session, sc_wgs_table, sample_id
                )

                if is_in_db is None:
                    sample_data = db.prepare_data_for_import(
                        sc_wgs_table,
                        **{
                            "referral_id": sample_id,
                            "date": date,
                            "job_id": job.id,
                            "job_status": job.state,
                            "processing_status": "Job completed",
                            "workbook_dnanexus_location": f"{config_data['project_id']}:"
                            f"{supplementary_html_file.folder}/output",
                        },
                    )
                    db.insert_in_db(session, sc_wgs_table, [sample_data])
                    base_log.info(f"Inserted {sample_id} data")
                    print(f"Inserted {sample_id} data", flush=True)

                else:
                    db.update_in_db(
                        session,
                        sc_wgs_table,
                        sample_id,
                        {"job_status": job_status},
                    )
                    base_log.info(f"Updated {sample_id} job status in db")
                    print(f"Updated {sample_id} job status in db", flush=True)

                if job_status == "done":
                    utils.download_file_and_update_db(
                        session,
                        sc_wgs_table,
                        sample_id,
                        config_data["clingen_download_location"],
                        job,
                    )
                    base_log.info(f"Downloaded {sample_id} workbook")
                    print(f"Downloaded {sample_id} workbook", flush=True)
                else:
                    base_log.info(f"{job.id} didn't finish successfully")
                    print(f"{job.id} didn't finish successfully", flush=True)

    base_log.info("Finished workbook monitoring")
    print("Finished workbook monitoring", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        required=False,
        default="/app/sc_wgs_monitoring/config/sc_wgs_monitoring/config.py",
    )
    parser.add_argument(
        "-l",
        "--local_files",
        nargs="+",
        help="Local files to process",
    )
    parser.add_argument(
        "-ids",
        "--dnanexus_ids",
        nargs="+",
        help=(
            "DNAnexus ids either for providing inputs for the workbook jobs "
            "or job ids for checking jobs"
        ),
    )
    parser.add_argument(
        "-t",
        "--time_to_check",
        required=False,
        default="",
        help=(
            "Time period in which to check for new jobs. Please "
            "use s, m, h, d as suffixes i.e. 10s will check for jobs "
            "finished in the last 10s"
        ),
    )
    parser.add_argument(
        "-d",
        "--daily_report",
        action="store_true",
        default=False,
        help=(
            "Flag option to send a Slack report for the day for solid cancer "
            "wgs workbook creation"
        ),
    )

    type_processing = parser.add_mutually_exclusive_group()
    type_processing.add_argument(
        "-s",
        "--start_jobs",
        action="store_true",
        default=False,
        help="Flag argument required for starting jobs",
    )
    type_processing.add_argument(
        "-j",
        "--download_job_output",
        action="store_true",
        default=False,
        help=(
            "Flag argument required for downloading output and updating the "
            "database from jobs"
        ),
    )

    subparser = parser.add_subparsers(help="")
    config_override = subparser.add_parser(
        "config_override",
        help="Parser for the parameters used to configuration override",
    )
    config_override.add_argument(
        "-project_id",
        "--project_id",
        help="Project ID in which to upload and process the workbooks",
    )
    config_override.add_argument(
        "-pid_div_id",
        "--pid_div_id",
        help=(
            "The ID of the div element that contains the PID information in "
            "the supplementary HTML"
        ),
    )
    config_override.add_argument(
        "-app_id",
        "--sd_wgs_workbook_app_id",
        help="SD WGS workbook app ID",
    )
    config_override.add_argument(
        "-hotspots",
        "--hotspots",
        help="hotspots parameter to override config data",
    )
    config_override.add_argument(
        "-reference_gene_groups",
        "--reference_gene_groups",
        help="reference_gene_groups parameter to override config data",
    )
    config_override.add_argument(
        "-panelapp",
        "--panelapp",
        help="panelapp parameter to override config data",
    )
    config_override.add_argument(
        "-cytological_bands",
        "--cytological_bands",
        help="cytological_bands parameter to override config data",
    )
    config_override.add_argument(
        "-clinvar",
        "--clinvar",
        help="clinvar parameter to override config data",
    )
    config_override.add_argument(
        "-clinvar_index",
        "--clinvar_index",
        help="clinvar_index parameter to override config data",
    )
    config_override.add_argument(
        "-clingen_input_location",
        "--clingen_input_location",
        help=(
            "Clingen location to check for data (used to override config data)"
        ),
    )
    config_override.add_argument(
        "-clingen_download_location",
        "--clingen_download_location",
        help=(
            "Clingen location to download data to (used to override config "
            "data)"
        ),
    )

    args = vars(parser.parse_args())

    main(**args)
