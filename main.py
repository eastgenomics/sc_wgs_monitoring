import argparse
import datetime
from pathlib import Path
import re

import dxpy

from sc_wgs_monitoring import check, dnanexus, db, utils


def main(**args):
    config_data = utils.load_config(args["config"])

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
        config_data["user"],
        config_data["pwd"],
        config_data["db_name"],
    )
    sc_wgs_table = meta.tables["sc_wgs_data"]

    # DNAnexus setup things
    dnanexus.login_to_dnanexus(args["dnanexus_token"])
    sd_wgs_project = dxpy.bindings.DXProject(config_data["project_id"])
    dxpy.set_workspace_id(sd_wgs_project.id)

    date = datetime.date.today().strftime("%y%m%d")

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

        # handle given dnanexus file ids
        if args["dnanexus_file_ids"]:
            if all(
                [
                    check.check_dnanexus_id(file)
                    for file in args["dnanexus_file_ids"]
                ]
            ):
                new_files = [
                    dxpy.DXFile(file) for file in args["dnanexus_file_ids"]
                ]
            else:
                raise AssertionError(
                    f"Provided files {args['dnanexus_file_ids']} are not all "
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
                        f"{'|'.join([file for file in new_files])}"
                    )

                new_files = [Path(file) for file in args["local_files"]]

            # handle file detection
            else:
                files = utils.find_files_in_clingen_input_location(
                    config_data["clingen_input_location"]
                )
                new_files = utils.filter_files_using_time_to_check(
                    files, args["time_to_check"]
                )

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
            if args["dnanexus_file_ids"]:
                dnanexus_data = {}

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

            # starting the jobs
            for sample_id, data in dnanexus_data.items():
                # setup dict with the columns that need to be populated
                sample_data = {
                    column.name: None
                    for column in sc_wgs_table.columns
                    if column.name != "id"
                }

                inputs = {}

                for file in data["files"]:
                    inputs.update(
                        dnanexus.assign_dxfile_to_workbook_input(
                            file, patterns
                        )
                    )

                all_inputs = inputs | {
                    "hotspots": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "hotspots"
                        ]
                    },
                    "reference_gene_groups": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "reference_gene_groups"
                        ]
                    },
                    "panelapp": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "panelapp"
                        ]
                    },
                    "cytological_bands": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "cytological_bands"
                        ]
                    },
                    "clinvar": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "clinvar"
                        ]
                    },
                    "clinvar_index": {
                        "$dnanexus_link": config_data["workbook_inputs"][
                            "clinvar_index"
                        ]
                    },
                }

                job = dnanexus.start_wgs_workbook_job(
                    all_inputs,
                    config_data["sd_wgs_workbook_app_id"],
                    f"{data['folder']}/output",
                )
                # job_id.wait_on_done(10)

                # populate the dict
                sample_data["referral_id"] = sample_id
                sample_data["specimen_id"] = ""
                sample_data["date"] = date
                sample_data["clinical_indication"] = ""
                sample_data["job_id"] = job.id
                sample_data["job_status"] = ""
                sample_data["processing_status"] = "Job started"
                sample_data["workbook_location"] = (
                    f"{config_data['project_id']}:{data['folder']}/output"
                )
                db_data.append(sample_data)

            db.insert_in_db(session, sc_wgs_table, db_data)

            print("Job started + successful db update")

        else:
            # TODO probably send a slack log message
            print("Couldn't find any files")

    # # check jobs that have finished
    # if args["check_jobs"]:
    #     executions = dxpy.bindings.find_executions(
    #         executable=config_data["sd_wgs_workbook_app_id"],
    #         project=sd_wgs_project,
    #         created_after=args["time_to_check"],
    #         describe=True,
    #     )

    #     for execution in executions:
    #         for job_output in dnanexus.get_output_id(execution):
    #             dxpy.bindings.dxfile_functions.download_dxfile(
    #                 job_output, config_data["clingen_location"]
    #             )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dnanexus_token")
    parser.add_argument("-t", "--time_to_check", required=False, default=None)
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
        "--dnanexus_file_ids",
        nargs="+",
        help="DNAnexus ids for the input of the workbook job",
    )

    type_processing = parser.add_mutually_exclusive_group()
    type_processing.add_argument(
        "-s", "--start_jobs", action="store_true", default=False
    )
    type_processing.add_argument(
        "-c", "--check_jobs", action="store_true", default=False
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
