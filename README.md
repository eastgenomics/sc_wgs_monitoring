# sd_wgs_monitoring

Solid cancer WGS repository to monitor new files and new outputs from WGS workbooks jobs

## How to run

```sh
python main.py ${dnanexus_token} ${time_to_check_after} ${option} [-c config_override ...]
```

The necessary inputs are:

- DNAnexus token as the repository relies on DNAnexus
- The time to check for new files or jobs i.e. <http://autodoc.dnanexus.com/bindings/python/current/dxpy_search.html#dxpy.bindings.search.find_data_objects>
- The option for checking:
  - `-s` for checking new files, starting jobs and outputting the csv for the Confluence database
  - `-c` for checking completed jobs

```sh
docker exec ${container_id} sh -c 'python3 /app/sc_wgs_monitoring/main.py "$DNANEXUS_TOKEN" ...'
```
