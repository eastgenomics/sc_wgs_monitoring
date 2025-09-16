from pathlib import PosixPath
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

    data_to_return = {}

    for sample, data in sample_files.items():
        folder = f"/{date}/{sample}"
        data_to_return.setdefault(sample, {})
        dxpy.api.project_new_folder(
            project.id, input_params={"folder": folder, "parents": True}
        )
        data_to_return[sample]["folder"] = folder

        for file in data["files"]:
            if type(file) is PosixPath:
                dxfile = dxpy.upload_local_file(
                    file, project=project.id, folder=folder
                )
            else:
                # tuple containing the file name and the html content
                file_path_obj, html_content = file
                dxfile = dxpy.DXFile()
                dxfile.new(
                    name=file_path_obj.name, project=project.id, folder=folder
                )

                dxfile.write(str(html_content))
                dxfile.close()

                # stupid dnanexus close() is useless, describe() is superior
                dxfile.describe()

            data_to_return[sample].setdefault("files", []).append(dxfile)

    return data_to_return


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


def prepare_inputs(
    sample: str,
    files: list,
    folder: str,
    patterns: dict,
    workbook_inputs: dict,
    workbook_app: dxpy.DXApp,
) -> tuple:
    """Prepare the inputs for starting the jobs

    Parameters
    ----------
    sample : str
        Sample id
    files : list
        List of files for one job
    folder : str
        Folder output
    patterns : dict
        Dict of patterns for assignment to the correct input name
    workbook_inputs : dict
        Dict containing reference files
    workbook_app : dxpy.DXApp
        DXApp to be use

    Returns
    -------
    tuple
        Tuple containing all the input names and their inputs
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
