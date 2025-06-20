import argparse
import datetime
import logging
from pathlib import Path
import re
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

    if not args["start_jobs"] and not args["check_jobs"]:
        raise AssertionError(
            "No processing type specified, please use -s or -c"
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
                new_files = utils.find_files_in_clingen_input_location(
                    config_data["clingen_input_location"]
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

            # get the supplementary files
            supplementary_html_files = [
                file
                for file in new_files
                if re.search(r".*\.supplementary\.html", file.name)
            ]

            # remove the pid div and write the new file
            for file in supplementary_html_files:
                new_html_content = (
                    utils.remove_pid_div_from_supplementary_file(
                        file,
                        config_data["pid_div_id"],
                    )
                )

                utils.write_file(file, new_html_content)

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

            unprocessed_sample_files = db.remove_processed_samples(
                session, sc_wgs_table, sample_files
            )

            if unprocessed_sample_files == {}:
                base_log.warning(
                    "All detected samples have already been processed",
                )
                sys.exit()

            processed_samples = set(sample_files.keys()) - set(
                unprocessed_sample_files.keys()
            )

            if processed_samples:
                base_log.warning(
                    "The following samples have already been processed:\n - "
                    f"{"\n - ".join(processed_samples)}\n",
                )

            db_data = []

            for sample_id in unprocessed_sample_files:
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

            base_log.info("Inserted data in db")

            # if dnanexus file ids were specified no need for upload
            if args["dnanexus_ids"]:
                dnanexus_data = dnanexus.organise_data_for_processing(
                    unprocessed_sample_files
                )

            else:
                dnanexus_data = dnanexus.upload_input_files(
                    date, sd_wgs_project, unprocessed_sample_files
                )
                base_log.info("Uploaded the files to DNAnexus")

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

            jobs, errors = utils.start_parallel_workbook_jobs(
                session, sc_wgs_table, args_for_starting_jobs
            )

            if errors:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(errors)}",
                    slack_alert_channel,
                    slack_token,
                )

            if errors:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(errors)}",
                    slack_alert_channel,
                    slack_token,
                )

            base_log.info("Jobs started, starting job monitoring...")

            job_failures = utils.monitor_jobs(
                session,
                sc_wgs_table,
                jobs,
                config_data["clingen_download_location"],
            )

            if job_failures:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(job_failures)}",
                    slack_alert_channel,
                    slack_token,
                )

        else:
            base_log.info("Couldn't find any files")

    # check jobs that have finished
    if args["check_jobs"]:
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

        else:
            if args["dnanexus_ids"]:
                executions = [
                    dxpy.DXJob(dxid=job_id).describe()
                    for job_id in args["dnanexus_ids"]
                ]
            else:
                executions = dxpy.bindings.find_executions(
                    executable=config_data["sd_wgs_workbook_app_id"],
                    project=sd_wgs_project,
                    created_after=f"-{args['time_to_check']}",
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

                else:
                    db.update_in_db(
                        session,
                        sc_wgs_table,
                        sample_id,
                        {"job_status": job_status},
                    )

                if job_status == "done":
                    utils.download_file_and_update_db(
                        session,
                        sc_wgs_table,
                        sample_id,
                        config_data["clingen_download_location"],
                        job,
                    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-config",
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
        "-c",
        "--check_jobs",
        action="store_true",
        default=False,
        help="Flag argument required for checking jobs",
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
