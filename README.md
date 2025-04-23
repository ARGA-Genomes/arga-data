# Australian Reference Genome Atlas (ARGA)
ARGA will allow researchers to easily discover and access genomic data for Australian species in one place, faciliatating research and informing decision making in areas including conservation, biosecurity and agriculture.

## ARGA Data
This repo is for Python and related code for data ingestion and pre-ingestion munging, prior to loading DwCA data into the Pipelines workflow.

## Setup
Set up can be initiated by being in the base directory and running `deploy.py`, which should create a virtual environment and add a link to the src folder required to run the scripts. Further are provided as part of that script after it completes.

## Processing Data
The configuration files in `dataSources/location/database` are how processing understands the procedure. To add a new source, you'll need to create a new sourceConfig file. To access that source, you can call one of the source processing tools with the syntax `location-database`. For example, to process the genbankSummary data source which is part of the ncbi, your source is called `ncbi-genbankSummary`, which is case sensitive.

The tools currently available are:
 - listSources.py
 - newSource.py
 - purgeSource.py
 - download.py
 - process.py
 - convert.py
 - getFields.py

`listSources` shows you a list of currently available sources.
`newSource` creates a new source folder and basic config to kickstart adding a new source.
`purgeSource` deletes a source that is no longer required.
`download` is for simply downloading the data, which will vary based on your database type.
`processing` is for running initial processing on files outlined in the source config.
`convert` will then convert the old file to the new file mappings, as well as enrich and augment if required.
`getFields` will read the pre-conversion file and give examples of field names and how they'll be mapped with the appropriate mapping file.

## Issues repository
- [List of issues](https://github.com/ARGA-Genomes/arga-data/issues)
