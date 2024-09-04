# CryoGrid data fetcher

Create and launch docker containers to download data to run CryoGrid models using the topoSUB module. 

The following datasets are downloaded: 
- ERA5 data - takes a very long time
- Elevation data from the Coperincus DEM (30m resolution)

Data is downloaded to an S3 bucket.

## Configuring a project


### Configuration file
The project configuration is specified in the `./requests/some_project_name.yaml` file.

If you'd like to create a template of the project configuration file, run the following command:

```python 
import cryogrid_data_fetcher as cdf

cdf.config.make_template('requests/cryogrid_project_name.yaml')
```
Change the values in the template to match your project requirements.

Note that there is a special feature that if the environmental variables 
`START_YEAR` and `END_YEAR` are set, it overrides the `start_year` and `end_year` in 
the configuration file. This can be useful for launching multiple multiple RunAI jobs. 

### `.env` file
You need to provide the credentials for the S3 bucket in the `.env` file. 
This should look something like this:

```bash
AWS_ENDPOINT_URL=https://os.zhdk.cloud.switch.ch
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key

# if you're using Docker, you can also specify Makefile variables here
DOCKER_USER=docker_hub_username  # will be used in Makefile
```

## Downloading Data

### Jupyter notebook

To run the downloader in a Jupyter notebook (in this case the default `demo.ipynb` notebook), use the following code:

```python
import cryogrid_data_fetcher as cdf

# loads BBOX, time range, S3 bucket specs, etc.
config = cdf.config.load('requests/cryogrid_project_name.yaml')

# Data will be downloaded to the S3 bucket - see config file
cdf.download_dem_from_planetary_computer(config)
cdf.download_era5_from_weatherbench(config)
```

### Docker (only ERA5 data)


```bash
# you should have an .env file in this directory with S3 credentials
# the .env file can also contain variables that are used in the Makefile
# providing these these variables in the .env file overrides the default 
# values in the Makefile

make build  # builds the Docker image with the conda environment
make run  # downloads ERA5 data based on config

make run DOCKER_ENTRYPOINT=/bin/bash  # starts bash inside the container
```

### RunAI (only ERA5 data)
```bash
# you should have an .env file in this directory with S3 credentials
# the .env file can also contain variables required for the Makefile:
# DOCKER_USER=docker_hub_username
# RUNAI_PROJECT=runai_project_name

export DOCKER_TAG='latest'  # or any other tag

make build  # builds the Docker image locally with the conda environment
make push  # push Docker image to DockerHub (requires DOCKER_USER as env variable)
make runai-submit 
```

Alternatively, you can use the bash script called `runai_submit_jobs.sh` to launch multiple jobs on RunAI.
