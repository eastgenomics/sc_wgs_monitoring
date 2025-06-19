from typing import Dict
import re

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


def upload_input_files(
    date: str, project: dxpy.DXProject, sample_files: Dict
) -> dict:
    """Upload the input files required for creating the WGS workbook

    Parameters
    ----------
    date: str
        String for the date in YYMMDD format
    project: dxpy.DXProject
        DXProject object
    sample_files : Dict
        Dict containing the sample id and their files

    Returns
    -------
    dict
        Dict of samples and their files in dnanexus and their location
    """

    data = {}

    for sample, files in sample_files.items():
        folder = f"/{date}/{sample}"
        data.setdefault(sample, {})
        dxpy.api.project_new_folder(
            project.id, input_params={"folder": folder, "parents": True}
        )
        data[sample]["folder"] = folder

        for file in files:
            dxfile = dxpy.upload_local_file(
                file, project=project.id, folder=folder
            )

            data[sample].setdefault("files", []).append(dxfile)

    return data


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

    job_output = execution["output"]["workbook"]["$dnanexus_link"]
    return job_output


def assign_dxfile_to_workbook_input(file: dxpy.DXFile, patterns: dict) -> dict:
    """Assign DXFile to a workbook job input name

    Parameters
    ----------
    file : dxpy.DXFile
        DXFile object to assign to an input name
    patterns : dict
        Dict of the patterns with their associated input name

    Returns
    -------
    dict
        Dict containing the input name and the dnanexus link dict needed
        for starting the job

    Raises
    ------
    AssertionError
        If the file name couldn't be associated with a pattern, raise error
    """
    for pattern, input_name in patterns.items():
        if re.search(pattern, file.name):
            return {input_name: {"$dnanexus_link": file.id}}

    raise AssertionError(
        "Couldn't match a dnanexus filename to an expected workbook input name"
    )


def organise_data_for_processing(sample_files: dict) -> dict:
    """Organise data for processing before preparing the inputs

    Parameters
    ----------
    sample_files : dict
        Dict containing sample ids and files

    Returns
    -------
    dict
        Dict containing the sample, the files and their folder
    """

    dnanexus_data = {}

    # go through the dnanexus file ids and match them to the
    # sample id
    for sample_id, files in sample_files.items():
        dnanexus_data.setdefault(sample_id, {})

        for file in files:
            dnanexus_data[sample_id].setdefault("files", []).append(file)
            dnanexus_data[sample_id]["folder"] = file.folder

    return dnanexus_data


def prepare_inputs(
    sample: str,
    files: list,
    folder: str,
    patterns: list,
    workbook_inputs: dict,
    workbook_app: dxpy.DXApp,
) -> dict:
    """Prepare the inputs for starting the jobs

    Parameters
    ----------
    sample : str
        Sample id
    files : list
        List of files for one job
    folder : str
        Folder output
    patterns : list
        List of patterns for assignment to the correct input name
    workbook_inputs : dict
        Dict containing reference files
    workbook_app : dxpy.DXApp
        DXApp to be use

    Returns
    -------
    dict
        Dict containing all the input names and their inputs
    """

    inputs = {}

    for file in files:
        inputs.update(assign_dxfile_to_workbook_input(file, patterns))

    return (
        inputs
        | {
            ref_input_name: {"$dnanexus_link": workbook_inputs[ref_input_name]}
            for ref_input_name in workbook_inputs
        },
        workbook_app,
        sample,
        f"{folder}/output",
    )


def start_wgs_workbook_job(
    workbook_inputs: Dict,
    app: dxpy.DXApp,
    job_name: str,
    output_folder: str,
) -> dxpy.DXJob:
    """Start the WGS Solid cancer workbook job

    Parameters
    ----------
    workbook_inputs : Dict
        Dict containing the inputs for the sample
    app : dxpy.DXApp
        DNAnexus app to use for running the job
    job_name : str
        String with the job name to give to the job
    output_folder : str
        Output folder in DNAnexus for the job

    Returns
    -------
    dxpy.DXJob
        DXJob object
    """

    return app.run(workbook_inputs, name=job_name, folder=output_folder)
