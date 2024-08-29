# CryoGrid data downloader

Create and launch docker containers to download data to run CryoGrid models using the topoSUB module. 

The following datasets are downloaded: 
- ERA5 data - takes a very long time
- Elevation data from the Coperincus DEM (30m resolution)

Data is downloaded to an S3 bucket.

## Configuring a project

The project configuration is specified in the `./requests/some_project_name.yaml` file.

