import argparse
import datetime
import json
import re
from typing import Dict

import dxpy


def login_to_dnanexus(token: str):
    """Login to dnanexus using the given token

    Parameters
    ----------
    token : str
        DNAnexus token to login
    """

    dx_security_context = {
        "auth_token_type": "Bearer",
        "auth_token": token,
    }

    dxpy.set_security_context(dx_security_context)


def load_config() -> Dict:
    """Load the configuration file

    Returns
    -------
    Dict
        Dict containing the keys for the various parameters used
    """

    with open("config.json") as f:
        config_data = json.loads(f.read())

    return config_data


def get_sample_id_from_files(files: list) -> Dict:
    """Get the sample id from the new files detected and link sample ids to
    their files

    Parameters
    ----------
    files : list
        List of DNAnexus file objects

    Returns
    -------
    Dict
        Dict containing the sample id and their files
    """

    patterns = [
        r"[-_]reported_structural_variants\..*\.csv",
        r"[-_]reported_variants\..*\.csv",
        r"\..*\.supplementary\.html",
    ]

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

    assert all(file_dict)

    return file_dict


def move_inputs_in_new_folders(
    project: dxpy.DXProject, sample_files: Dict
) -> list:
    """Move the files necessary to the WGS workbook job in a folder as the job
    requires a DNAnexus folder as an input

    Parameters
    ----------
    project: dxpy.DXProject
        DXProject object
    sample_files : Dict
        Dict containing the sample id and their files

    Returns
    -------
    list
        List of folders where the inputs were moved to
    """

    date = datetime.date.today().strftime("%y%m%d")

    folders = []

    for sample, files in sample_files.items():
        folder = f"/{date}/{sample}"
        dxpy.api.project_new_folder(
            project.id, input_params={"folder": folder, "parents": True}
        )

        for file in files:
            file.move(folder)

        folders.append(folder)

    return folders


def start_wgs_workbook_job(workbook_inputs: Dict, app_id: str) -> dxpy.DXJob:
    """Start the WHS Solid cancer workbook job

    Parameters
    ----------
    workbook_inputs : Dict
        Dict containing the inputs for the sample
    app_id : str
        DNAnexus app to use for running the job

    Returns
    -------
    dxpy.DXJob
        DXJob object
    """

    return dxpy.bindings.dxapp.DXApp(dxid=app_id).run(
        workbook_inputs,
        folder=f"{workbook_inputs['nextflow_pipeline_params']}/output",
    )


def get_output_id(execution: Dict) -> str:
    """Get the output file id for the WGS workbook job

    Parameters
    ----------
    execution : Dict
        Dict containing the describe output of the WGS workbook job

    Returns
    -------
    str
        File id of the WGS workbook job output
    """

    if execution["describe"]["state"] == "done":
        job_output = [
            output_id
            for output_id in execution["output"]["published_files"].values()
        ]

        # sense check we have one output only
        if len(job_output) == 1:
            return job_output


def main(**args):
    config_data = load_config()

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

    login_to_dnanexus(args["dnanexus_token"])

    sd_wgs_project = dxpy.bindings.DXProject(
        config_data["project_to_check_for_new_files"],
    )
    dxpy.set_workspace_id(sd_wgs_project.id)

    # start WGS workbook jobs
    if args["start_jobs"]:
        new_files = dxpy.bindings.find_data_objects(
            project=sd_wgs_project.id, created_after=args["time_to_check"]
        )

        if new_files:
            # group files per id as a sense check
            sample_files = get_sample_id_from_files(
                [
                    dxpy.DXFile(dxid=file["id"], project=file["project"])
                    for file in new_files
                ]
            )

            processed_samples = []

            for sample_id, files in sample_files.items():
                processed_files = []

                for file in files:
                    # check if the folder path starts with a date formatted
                    # string i.e. file has been processed
                    if re.match(r"/[0-9]{6}", file.folder):
                        processed_files.append(file)

                # if there is a match in the number of files that have been
                # detected to be processed
                if len(processed_files) == len(files):
                    print(f"Sample {sample_id} has already been processed")
                    processed_samples.append(sample_id)

            # remove all processed samples from dict to be passed
            for sample_id in processed_samples:
                del sample_files[sample_id]

            # all samples were removed
            if not sample_files:
                print("All files detected have already been processed")
                exit()

            folders = move_inputs_in_new_folders(sd_wgs_project, sample_files)

            for folder in folders:
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
                start_wgs_workbook_job(
                    inputs, config_data["sd_wgs_workbook_app_id"]
                )

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
            for job_output in get_output_id(execution):
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
