import argparse
import datetime
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

    # TODO check if there are new files to process in clingen

    # DNAnexus setup things
    dnanexus.login_to_dnanexus(args["dnanexus_token"])
    sd_wgs_project = dxpy.bindings.DXProject(
        config_data["project_to_check_for_new_files"],
    )
    dxpy.set_workspace_id(sd_wgs_project.id)

    date = datetime.date.today().strftime("%y%m%d")

    if not args["start_jobs"] and not args["check_jobs"]:
        raise AssertionError(
            "No processing type specified, please use -s or -c"
        )

    # start WGS workbook jobs
    if args["start_jobs"]:
        data = []
        new_files = None
        patterns = [
            r"[-_]reported_structural_variants\..*\.csv",
            r"[-_]reported_variants\..*\.csv",
            r"\..*\.supplementary\.html",
        ]

        if args["dnanexus_file_ids"]:
            if all([utils.check_dnanexus_id(file) for file in args["files"]]):
                new_files = [dxpy.DXFile(file) for file in args["files"]]
            else:
                raise AssertionError(
                    f"Provided files {args['files']} are not all DNAnexus "
                    "file ids"
                )

        elif args["local_files"]:
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

            new_files = args["local_files"]
            supplementary_html = [
                file
                for file in new_files
                if re.search(r".*\.supplementary\.html", file)
            ][0]
            new_html = utils.remove_pid_div_from_supplementary_file(
                supplementary_html, config_data["pid_div_id"]
            )

            with open(supplementary_html, "w") as file:
                file.write(str(new_html))

        else:
            # TODO check local clingen location for new files using the
            # time_to_check parameter
            pass

        if not all(
            check.check_file_input_name_is_correct(file, patterns)
            for file in new_files
        ):
            raise AssertionError(
                f"The set of provided files is not correct. Expected files with the following patterns: {[
                    r'[-_]reported_structural_variants\..*\.csv',
                    r'[-_]reported_variants\..*\.csv',
                    r'\..*\.supplementary\.html',
                ]}. "
                f"Got {"|".join([file for file in new_files])}"
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

            folders = dnanexus.upload_input_files(
                date, sd_wgs_project, sample_files
            )

            for folder, sample in folders.items():
                # setup dict with the columns that need to be populated
                sample_data = {
                    column.name: None
                    for column in sc_wgs_table.columns
                    if column.name != "id"
                }

                inputs = {
                    "hotspots": {"$dnanexus_link": config_data["hotspots"]},
                    "refgene_group": {
                        "$dnanexus_link": config_data["refgene_group"]
                    },
                    "clinvar": {"$dnanexus_link": config_data["clinvar"]},
                    "clinvar_index": {
                        "$dnanexus_link": config_data["clinvar_index"]
                    },
                    "nextflow_pipeline_params": folder,
                }

                dnanexus.start_wgs_workbook_job(
                    inputs, config_data["sd_wgs_workbook_app_id"]
                )

                # populate the dict
                sample_data["gel_id"] = sample
                sample_data["date_added"] = date
                sample_data["location_in_dnanexus"] = (
                    f"{config_data['project_to_check_for_new_files']}:{folder}"
                )
                sample_data["status"] = "Job started"
                data.append(sample_data)

            db.insert_in_db(session, sc_wgs_table, data)

        else:
            # TODO probably send a slack log message
            print("Couldn't find any files")

    # check jobs that have finished
    if args["check_jobs"]:
        executions = dxpy.bindings.find_executions(
            executable=config_data["sd_wgs_workbook_app_id"],
            project=sd_wgs_project,
            created_after=args["time_to_check"],
            describe=True,
        )

        for execution in executions:
            for job_output in dnanexus.get_output_id(execution):
                dxpy.bindings.dxfile_functions.download_dxfile(
                    job_output, config_data["clingen_location"]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dnanexus_token")
    parser.add_argument("-t", "--time_to_check", required=False, default="-1d")
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
        "--project_to_check_for_new_files",
        help="Project ID in which to look for new files",
    )
    config_override.add_argument(
        "-app_id",
        "--sd_wgs_workbook_app_id",
        help="SD WGS workbook app ID in which to look for new files",
    )
    config_override.add_argument(
        "-hotspots",
        "--hotspots",
        help="hotspots parameter to override config data",
    )
    config_override.add_argument(
        "-refgene_group",
        "--refgene_group",
        help="refgene_group parameter to override config data",
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
        "-clingen_location",
        "--clingen_location",
        help=(
            "Clingen location to upload data to (used to override config data)"
        ),
    )

    args = vars(parser.parse_args())

    main(**args)
