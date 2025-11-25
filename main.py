import argparse
import datetime
import logging
import sys

import dxpy

from sc_wgs_monitoring import (
    dnanexus,
    db,
    logger,
    notifications,
    setup,
    utils,
)

logger.set_up_logger()
base_log = logging.getLogger("basic")


def main(**args):
    (
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
    ) = setup.setup_environment(**args)

    header_msg = (
        f"{now} - :excel: Solid Cancer Workbooks :excel: - Command line: "
        f"`{' '.join(sys.argv)}`\n\n"
    )

    logger.log(
        base_log, "info", header_msg.replace("\n", "").replace(":excel:", "")
    )

    # start WGS workbook jobs
    if args["start_jobs"]:
        new_files = setup.get_inputs(
            config_data["clingen_download_location"],
            config_data["input_patterns"],
            **args,
        )

        if new_files:
            sample_files, silent_errors = setup.check_inputs(
                new_files, config_data["input_patterns"]
            )

            sample_files_tagged = setup.check_processed_samples(
                sample_files, session, sc_wgs_table, **args
            )
            dnanexus_data = setup.insert_inputs_in_db(
                sample_files_tagged, date, date_str, session, sc_wgs_table
            )

            if not args["dnanexus_ids"]:
                new_sample_files = setup.remove_pid(
                    sample_files_tagged, config_data["pid_div_id"], **args
                )
                sample_files_tagged = new_sample_files

                dnanexus_data = dnanexus.upload_input_files(
                    date, sd_wgs_project, sample_files_tagged
                )

            args_for_starting_jobs = []

            # organise data for preparation for starting the jobs
            for sample_id, data in dnanexus_data.items():
                args_for_starting_jobs.append(
                    dnanexus.prepare_inputs(
                        sample_id,
                        data["files"],
                        data["folder"],
                        config_data["input_patterns"],
                        config_data["workbook_inputs"],
                        sc_wgs_workbook_app,
                        config_data["instance_type"],
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
                            download_folder,
                            *sample_files_tagged[sample_id]["files"],
                        )

            if job_failures:
                notifications.slack_notify(
                    f"{header_msg + '\n'.join(job_failures)}",
                    slack_alert_channel,
                    slack_token,
                )

            # if errors were detected for some but not all files, allow the
            # processing to go through and trigger slack message at the end
            if silent_errors:
                raise AssertionError(
                    (
                        "Errors were detected for some of the files detected. "
                        "Please investigate in the logs"
                    )
                )

        else:
            msg = "Couldn't find any files"
            logger.log(base_log, "info", msg)

    elif args["daily_report"]:
        now = datetime.datetime.strptime(now, "%y%m%d | %H:%M:%S")
        jobs_for_day = db.get_samples_for_the_day(
            session, sc_wgs_table, now - datetime.timedelta(days=1)
        )
        report = notifications.build_report(
            jobs_for_day,
            (now - datetime.timedelta(days=1)).strftime("%y%m%d | %H:%M:%S"),
        )
        notifications.slack_notify(report, slack_log_channel, slack_token)
        logger.log(base_log, "info", report)

    # check jobs that have finished
    elif args["download_job_output"]:
        if args["dnanexus_ids"]:
            executions = [{"id": job_id} for job_id in args["dnanexus_ids"]]
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

        msg = (
            "Found the following jobs: "
            f"{" | ".join([job["id"] for job in executions])}"
        )
        logger.log(base_log, "info", msg)

        for execution in executions:
            job = dxpy.DXJob(execution["id"])
            supplementary_html_file = dxpy.DXFile(
                job.runInput["supplementary_html"]["$dnanexus_link"]
            )
            sample_id = supplementary_html_file.name.split(".")[0]

            # get job status
            job_status = job.state

            is_in_db = db.look_for_processed_samples(
                session, sc_wgs_table, sample_id
            )

            download_folder = utils.create_output_folder(
                sample_id, config_data["clingen_download_location"]
            )

            if is_in_db is None:
                sample_data = db.prepare_data_for_import(
                    sc_wgs_table,
                    **{
                        "referral_id": sample_id,
                        "date": date_str,
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
                    download_folder,
                    job,
                )
            else:
                msg = f"{job.id} didn't finish successfully"
                logger.log(base_log, "info", msg)

    msg = "Finished workbook monitoring"
    logger.log(base_log, "info", msg)


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
    config_override.add_argument(
        "-instance_type",
        "--instance_type",
        help="Instance type to use for the jobs",
    )

    args = vars(parser.parse_args())

    main(**args)
