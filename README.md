# sc_wgs_monitoring

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
# base command
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py ...'

# start workbook jobs from files detected in the config location
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s'
# start workbook jobs from dnanexus files
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s -ids ${file_id} ${file_id} ${file_id}'
# start workbook jobs from local files
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s -l ${file} ${file} ${file}'
# override config file using individual config key
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s config_override -project_id project-xxxxxxxxxxxxxxxxxxxxxxxx'
# override config file using new config file
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -s -c /app/sc_wgs_monitoring/inputs/new_config'


# check for jobs finished in the last hour
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -j -t 1h'
# upload files from the specified jobs
docker run --rm --env-file ${environment_config_file} --network ${name_of_the_network_in_docker-compose} -v ${local_path_where_inputs_are_located}:/app/sc_wgs_monitoring/inputs -v ${local_path_to_download_workbooks_to}:/app/sc_wgs_monitoring/output -v ${local_path_to_logs}:/app/sc_wgs_monitoring/logs/ ${image_name}:${image_version} sh -c 'python3 /app/sc_wgs_monitoring/main.py -j -ids ${job_id}'
```

## Configs

### Environment variables config

The environment config file contains variables which can't be stored in the app container itself such as tokens.

```txt
DNANEXUS_TOKEN=
DB_NAME=
DB_USER=
DB_PASSWORD=
HOST=
SLACK_TOKEN=
SLACK_LOG_CHANNEL=
SLACK_ALERT_CHANNEL=
HTTPS_PROXY=
```

### App config

The monitoring app uses a Python config file to allow customisation inputs, input and upload locations, app id...

The repo has a default config file: `/app/sc_wgs_monitoring/config.py`. Default values can be overriden by using `--config` or using `config_override --key_name` with key name being the key in the config file described below:

```python
CONFIG = {
    "project_id": "",
    "input_patterns": [
        r"[-_]reported_structural_variants\..*\.csv",
        r"[-_]reported_variants\..*\.csv",
        r"\..*\.supplementary\.html",
    ],
    "pid_div_id": "pid",
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
    "instance_type": "",
}
```

> [!WARNING]
> If providing a new config using the `--config` option, you have to use a "container" path i.e. a path existing in the container. So you either have to mount a new folder or you can use an already mounted volume to put your new config file in.
