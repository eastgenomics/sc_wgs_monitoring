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


def move_inputs_in_new_folders(
    date: str, project: dxpy.DXProject, sample_files: Dict
) -> list:
    """Move the files necessary to the WGS workbook job in a folder as the job
    requires a DNAnexus folder as an input

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
    list
        List of folders where the inputs were moved to
    """

    folders = {}

    for sample, files in sample_files.items():
        folder = f"/{date}/{sample}"
        dxpy.api.project_new_folder(
            project.id, input_params={"folder": folder, "parents": True}
        )

        for file in files:
            file.move(folder)

        folders[folder] = sample

    return folders


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
