import argparse
import datetime

import dxpy

from sc_wgs_monitoring import utils, dnanexus, db


def main(**args):
    config_data = utils.load_config()

    # override the values in the config file if the cli was used to override
    for config_key in [
        "hotspots",
        "refgene_group",
        "clinvar",
        "clinvar_index",
        "clingen_location",
    ]:
        config_override_value = args.get(config_key, None)

        if config_override_value:
            config_data[config_key] = config_override_value

    session, meta = db.connect_to_db(
        config_data["endpoint"],
        config_data["port"],
        config_data["user"],
        config_data["pwd"]
    )
    sc_wgs_table = meta.tables["testdirectory.wgs_sc_tracker"]

    dnanexus.login_to_dnanexus(args["dnanexus_token"])
    sd_wgs_project = dxpy.bindings.DXProject(
        config_data["project_to_check_for_new_files"],
    )
    dxpy.set_workspace_id(sd_wgs_project.id)

    date = datetime.date.today().strftime("%y%m%d")

    # start WGS workbook jobs
    if args["start_jobs"]:
        data = {"name": [], "date_job_started": []}

        new_files = dxpy.bindings.find_data_objects(
            project=sd_wgs_project.id, created_after=args["time_to_check"]
        )

        if new_files:
            # group files per id as a sense check
            sample_files = utils.get_sample_id_from_files(
                [
                    dxpy.DXFile(dxid=file["id"], project=file["project"])
                    for file in new_files
                ]
            )

            processed_samples = [
                db.look_for_processed_samples(
                    session, sc_wgs_table, sample_id
                )
                for sample_id in sample_files
            ]

            # remove all processed samples from dict to be passed
            for sample_id in processed_samples:
                del sample_files[sample_id]

            # all samples were removed
            if not sample_files:
                print("All files detected have already been processed")
                exit()

            folders = dnanexus.move_inputs_in_new_folders(
                date, sd_wgs_project, sample_files
            )

            for folder, sample in folders.items():
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

                data["name"].append(sample)
                data["date_job_started"].append(folder.split("/")[1])

            csv = utils.write_confluence_csv(date, data)
            dxpy.upload_local_file(csv, folder=f"/{date}/")

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
    parser.add_argument("time_to_check", default="-1d")

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
