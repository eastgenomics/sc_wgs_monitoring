import argparse
import datetime
import multiprocessing
from pathlib import Path
import re
import sys

import dxpy

from sc_wgs_monitoring import check, dnanexus, db, notifications, utils


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

    header_msg = f"{now} - Command line: `{' '.join(sys.argv)}`\n\n"

    if not args["start_jobs"] and not args["check_jobs"]:
        raise AssertionError(
            "No processing type specified, please use -s or -c"
        )

    # start WGS workbook jobs
    if args["start_jobs"]:
        db_data = []
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
                files = utils.find_files_in_clingen_input_location(
                    config_data["clingen_input_location"]
                )

                time_to_check = utils.convert_time_to_epoch(
                    args["time_to_check"]
                )

                new_files = [
                    file
                    for file in files
                    if check.filter_file_using_time_to_check(
                        file, time_to_check
                    )
                ]

                if not new_files:
                    print(
                        "No new files modified in the last "
                        f"{args['time_to_check']}. Exiting"
                    )
                    exit()

            # find the html file and remove the pid div from it
            supplementary_html = [
                file
                for file in new_files
                if re.search(r".*\.supplementary\.html", file.name)
            ][0]

            new_html = utils.remove_pid_div_from_supplementary_file(
                supplementary_html, config_data["pid_div_id"]
            )

            with open(supplementary_html, "w") as file:
                file.write(str(new_html))

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

        if new_files:
            # group files per id as a sense check
            sample_files = utils.get_sample_id_from_files(new_files, patterns)

            message = ""

            # build report message for samples and their files
            for sample, files in sample_files.items():
                message += f"- {sample}\n"

                for file in files:
                    message += f"  - {file.name}\n"

            print(f"Detected the following files for processing:\n{message}")

            # query the database to find samples that have already been
            # processed
            processed_samples = [
                db.look_for_processed_samples(session, sc_wgs_table, sample_id)
                for sample_id in sample_files
            ]

            # previous function returns a list of result or None so checking if
            # we have at least one sample to import in the db
            if any(processed_samples):
                # remove all processed samples from dict to be passed
                for sample_id in processed_samples:
                    print(f"{sample_id} has already been processed")
                    del sample_files[sample_id]

            # all samples were removed
            if not sample_files:
                print(
                    "All files detected have already been processed. "
                    "Exiting..."
                )
                exit()

            # if dnanexus file ids were specified no need for upload
            if args["dnanexus_ids"]:
                dnanexus_data = {}

                # go through the dnanexus file ids and match them to the
                # sample id
                for sample_id, files in sample_files.items():
                    dnanexus_data.setdefault(sample_id, {})

                    for file in files:
                        for given_file in new_files:
                            if file.id == given_file.id:
                                dnanexus_data[sample_id].setdefault(
                                    "files", []
                                ).append(file)

                                dnanexus_data[sample_id][
                                    "folder"
                                ] = file.folder

            else:
                dnanexus_data = dnanexus.upload_input_files(
                    date, sd_wgs_project, sample_files
                )
                print("Uploaded the files to DNAnexus")

            args_for_starting_jobs = []

            # organise data for preparation for starting the jobs
            for sample_id, data in dnanexus_data.items():
                inputs = {}

                for file in data["files"]:
                    inputs.update(
                        dnanexus.assign_dxfile_to_workbook_input(
                            file, patterns
                        )
                    )

                args_for_starting_jobs.append(
                    (
                        inputs
                        | {
                            ref_input_name: {
                                "$dnanexus_link": config_data[
                                    "workbook_inputs"
                                ][ref_input_name]
                            }
                            for ref_input_name in config_data[
                                "workbook_inputs"
                            ]
                        },
                        sc_wgs_workbook_app,
                        f"{sc_wgs_workbook_app.name} | {sample_id}",
                        f"{data['folder']}/output",
                    )
                )

            # start the jobs
            with multiprocessing.Pool(processes=10) as pool:
                jobs = pool.starmap(
                    dnanexus.start_wgs_workbook_job, args_for_starting_jobs
                )

            print("Jobs started")

            for job in jobs:
                sample_id = job.name.split(" | ")[1]

                if sample_id in dnanexus_data:
                    dnanexus_data[sample_id]["job"] = job

                # setup dict with the columns that need to be populated
                sample_data = {
                    column.name: None
                    for column in sc_wgs_table.columns
                    if column.name != "id"
                }

                # populate the dict
                sample_data["referral_id"] = sample_id
                sample_data["specimen_id"] = ""
                sample_data["date"] = date
                sample_data["clinical_indication"] = ""
                sample_data["job_id"] = job.id
                sample_data["job_status"] = ""
                sample_data["processing_status"] = "Job started"
                sample_data["workbook_dnanexus_location"] = (
                    f"{config_data['project_id']}:{data['folder']}/output"
                )
                db_data.append(sample_data)

            db.insert_in_db(session, sc_wgs_table, db_data)

            print("Successful db update")

            job_failures = []

            for job in jobs:
                try:
                    job.wait_on_done(10)
                except dxpy.exceptions.DXJobFailureError:
                    job_failures.append(f"- Job {job.id} failed")

            notifications.slack_notify(
                f"{header_msg + '\n'.join(job_failures)}",
                slack_log_channel,
                slack_token,
            )

            # check the job statuses, update the db and download the files in
            # the appropriate locations
            for sample, data in dnanexus_data.items():
                job = data["job"]
                # get job status
                job_status = job.state

                db.update_in_db(
                    session, sc_wgs_table, sample, {"job_status": job_status}
                )

                if job_status == "done":
                    output_id = dnanexus.get_output_id(job.describe())
                    dxpy.bindings.dxfile_functions.download_dxfile(
                        output_id,
                        f"{config_data['clingen_upload_location']}/{sample}.xlsx",
                    )

                    db.update_in_db(
                        session,
                        sc_wgs_table,
                        sample,
                        {
                            "workbook_clingen_location": config_data[
                                "clingen_upload_location"
                            ]
                        },
                    )

        else:
            print("Couldn't find any files")

    # check jobs that have finished
    if args["check_jobs"]:
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
            sample_data = {}
            job = dxpy.DXJob(execution["id"])
            supplementary_html_file = dxpy.DXFile(
                execution["runInput"]["supplementary_html"]["$dnanexus_link"]
            )
            sample_id = supplementary_html_file.name.split(".")[0]

            # get job status
            job_status = job.state

            is_in_db = db.look_for_processed_samples(
                session, sc_wgs_table, sample_id
            )

            if is_in_db is None:
                # populate the dict
                sample_data["referral_id"] = sample_id
                sample_data["specimen_id"] = ""
                sample_data["date"] = date
                sample_data["clinical_indication"] = ""
                sample_data["job_id"] = job.id
                sample_data["job_status"] = job.state
                sample_data["processing_status"] = "Job completed"
                sample_data["workbook_dnanexus_location"] = (
                    f"{config_data['project_id']}:{supplementary_html_file.folder}/output"
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
                output_id = dnanexus.get_output_id(job.describe())
                dxpy.bindings.dxfile_functions.download_dxfile(
                    output_id,
                    f"{config_data['clingen_upload_location']}/{sample_id}.xlsx",
                )

                db.update_in_db(
                    session,
                    sc_wgs_table,
                    sample_id,
                    {
                        "workbook_clingen_location": config_data[
                            "clingen_upload_location"
                        ]
                    },
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-config",
        "--config",
        required=False,
        default="/app/sc_wgs_monitoring/config.py",
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
            "Time period in which to check for presence of new files. Please "
            "use s, m, h, d as suffixes i.e. 10s will check for files "
            "MODIFIED in the last 10s"
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
        "-clingen_upload_location",
        "--clingen_upload_location",
        help=(
            "Clingen location to upload data to (used to override config data)"
        ),
    )

    args = vars(parser.parse_args())

    main(**args)
