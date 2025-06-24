# sd_wgs_monitoring

Solid cancer WGS repository to monitor new files and new outputs from WGS workbooks jobs

## How to run

The necessary inputs are:

- The option for checking:
  - `-s` for checking new files, starting jobs and outputting the csv for the Confluence database
    - `-ids` can be used to specify dnanexus file ids. This will start jobs from those files.
    - `-l` can be used to specify local files, starting the process from the specified files.
  - `-c` for checking completed jobs and uploading the output of those jobs.
    - `-t` will look for jobs completed in the specified timeframe
    - `-ids` can be used to specify job ids to check.

```sh
# basic command
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py ...'

# start workbook jobs from files detected in the config location
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s'
# start workbook jobs from dnanexus files
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s -ids ${file_id} ${file_id} ${file_id}'
# start workbook jobs from local files
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s -l ${file} ${file} ${file}'

# check for jobs finished in the last hour
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py -c -t 1h'
# upload files from the specified jobs
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py -c -ids ${job_id}'
```

## Config

The monitoring app uses a Python config file to allow customisation inputs, input and upload locations, app id...

The repo has a default config file: `/app/sc_wgs_monitoring/config.py`. Default values can be overriden by using `-config` or using `config_override --key_name` with key name being the key in the config file described below:

```python
CONFIG = {
    "project_id": "",
    "input_patterns": [
        r"[-_]reported_structural_variants\..*\.csv",
        r"[-_]reported_variants\..*\.csv",
        r"\..*\.supplementary\.html",
    ],
    "pid_div_id": "",
    "sd_wgs_workbook_app_id": "",
    "workbook_inputs": {
        "hotspots": "",
        "reference_gene_groups": "",
        "panelapp": "",
        "cytological_bands": "",
        "clinvar": "",
        "clinvar_index": "",
    },
    "clingen_input_location": "",
    "clingen_download_location": "",
}
```
