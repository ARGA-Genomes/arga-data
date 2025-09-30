# Australian Reference Genome Atlas (ARGA)
ARGA will allow researchers to easily discover and access genomic data for Australian species in one place, faciliatating research and informing decision making in areas including conservation, biosecurity and agriculture.

## ARGA Data
This repo is for Python and related code for data ingestion and pre-ingestion munging, prior to loading data into arga-backend.

## Setup
Set up can be initiated by being in the base directory and running `deploy.py`, which should create a virtual environment and add a link to the src folder required to run the scripts. Further help is provided as part of that script after it completes. This process is platform independant.

## Data Sources
This pipeline works with config files in `dataSources` folder of the main directory. At the top level, folders exist for each of the currently available data locations, such as `ncbi`. Within each location folder a database folder exists, which further divides the location into separate databases. For example, the `ncbi` folder mentioned previously has a subfolder named `nucleotide`, which is the nucleotide data available from the ncbi data location. At this level a config file exists which outlines how the dataset should be run to produce a mapped output file ready for ingestion, as well as a scripts folder for storing specific database scripts, and where a data folder will be created during pipeline process to store data.

In some cases the database is further divided into subsections, for situations such as an extremely large database that is best run in smaller sections, or where different types of databases are retrieved using identical methods but produce slightly different results depending on input. Subsections are listed in the `config.json` file and will cause created data files to be placed in a data folder within a subsection folder. For the previously used `nucleotide` database within the `ncbi` location, there are many subsections such as `invertebrate` which allow full processing of part of the entire database.

In order to reference these locations/databases/subsections you can use the chain of (location)-(database)-(subsection). Continuing with the previous example, to access the `invertebrate` subsection of the ncbi's nucleotide database, you would refer to the source as `ncbi-nucleotide-invertebrate`. Additionally, you may omit the last section of any reference to attempt to process all referenced sections. This means that by referring to the nucleotide source as `ncbi-nucleotide` and omitting the subsection the pipeline will attempt to process ALL subsections, and if a database has no subsections this is how you would refer to it. By taking this a step further, you are able to just reference `ncbi` when using the available source related tools to attempt to process ALL databases and all of their subsequent subsections.

## Tools
All interaction with this pipeline are best done through the available command line tools. Many of these tools make use of the syntax mentioned above to interact with specific sources, although some tools have their own syntax, which you can discover by using the help flag (-h) when running any of the tools. A brief summary of the tools are;
- newSource: Create a new source in the data sources folder, creating a barebones config file depending on the type of database provided.
- listSoures: Print a list of currently available locations/databases/subsection.
- purgeSource: Removes a source in the data sources folder and cleans it up.
- download: Run the download process outlined in the config file.
- process: Run the processing process outlined in the config file.
- package: Package up the converted file and the downloading/processing/converting metadata into a zip file.
- update: Run download/process/convert/package sequentially, limited by the update information in the config
- sampleData: Collect a sample of processed data.

## Data Storage Redirection
A global `config.toml` file exists in the base directory for general global settings. The overwrites section can be replicated within any level of the data sources folder and the deeper `config.json` files will use that config. For example, many of the databases are quite large and so modifying the `storage` overwrite allows all downloading/processing files to be placed in a new location. To do this for all databases within the `ncbi` location, a `config.toml` file can be created within the `ncbi` folder with the overwrites section of the global config but either a relative or absolute path defined as the value, and all the databases will respect it. If instead you only wanted the `nucleotide` database to put it's data in a different location, you could instead place `config.toml` file within the `nucleotide` folder. This would allow all other `ncbi` databases to place their data as normal (whatever is outlined in the global `config.toml`), but have the `nucleotide` database place it's downloading/processing data in a separate location.

## Issues repository
- [List of issues](https://github.com/ARGA-Genomes/arga-data/issues)
