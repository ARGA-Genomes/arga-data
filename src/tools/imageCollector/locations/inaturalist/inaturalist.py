from pathlib import Path
import pandas as pd
import lib.commonFuncs as cmn
from lib.tools.bigFileWriter import BigFileWriter
import numpy as np
from lib.tools.downloader import Downloader
from lib.tools.zipping import extract
from lib.tools.progressBar import SteppableProgressBar
from lib.tools.logger import Logger

def downloadAndExtract(downloadDir: Path) -> Path:
    outputFile = downloadDir / "inaturalist-open-data-latest.tar.gz"

    if not outputFile.exists():
        downloader = Downloader()
        downloader.download("https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz", outputFile, verbose=True)
    
    outputPath = extract(outputFile)
    return outputPath

def getPhotoIDs(dataFolder: Path) -> Path:
    Logger.info("Getting Photo IDs")
    createdFolder = dataFolder.parents[1] / "createdFiles"
    # createdFolder.mkdir(exist_ok=True)

    observations = dataFolder / "observations.csv"
    photos = dataFolder / "photos.csv"
    taxa = dataFolder / "taxa.csv"

    taxonFile = createdFolder / "taxonCount.csv"
    if not taxonFile.exists():
        Logger.info("Creating taxonCount")
        values = pd.read_csv(observations, sep="\t", usecols=["taxon_id"])
        counts = values.value_counts()
        df = counts.to_frame("counts")
        df = df.reset_index()

        del values

        df["taxon_id"] = df["taxon_id"].astype(int)

        taxonomy = pd.read_csv(taxa, sep="\t", dtype=object)
        taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)
        df = df.merge(taxonomy, "left", on="taxon_id")
        df.to_csv(taxonFile, index=False)
    else:
        df = pd.read_csv(taxonFile, dtype=object)

    speciesFile = createdFolder / "speciesCount.csv"
    if not speciesFile.exists():
        Logger.info("Creating speciesCount")
        df = df[df["rank"].isin(["species", "subspecies"])]
        df.to_csv(speciesFile, index=False)
    else:
        df = pd.read_csv(speciesFile)

    taxonIDFile = createdFolder / "taxonIDs.txt"
    if not taxonIDFile.exists():
        Logger.info("Creating taxonIDs")
        ids = df["taxon_id"].astype(str).tolist()
        with open(taxonIDFile, "w") as fp:
            fp.write("\n".join(ids))
    else:
        with open(taxonIDFile) as fp:
            ids = fp.read().split("\n")

    obsvIDs = createdFolder / "observationIDs.txt"
    if not obsvIDs.exists():
        Logger.info("Creating observationIDs")
        series = pd.read_csv(observations, sep="\t", usecols=["observation_uuid"])["observation_uuid"]
        
        with open(obsvIDs, "w") as fp:
            fp.write("\n".join(series.tolist()))
        
    photoIDs = createdFolder / "photoIDs.txt"

    lengthBefore = 0
    lengthAfter = 0

    chunkGen = cmn.chunkGenerator(photos, 1024*1024*4, sep="\t", usecols=["photo_uuid", "observation_uuid"])
    readLines = 1024*1024*8

    Logger.info("Processing chunks")
    # print("HERE")
    # progress = SteppableProgressBar(50, len(list(chunkGen)))
    # print(progress.taskCount)
    for idx, df in enumerate(chunkGen, start=1):
        lengthBefore += len(df)

        with open(obsvIDs) as fp:
            observationIDs = fp.readlines(readLines)

            subIdx = 1
            while observationIDs:
                print(f"At chunk: {idx} | subIdx: {subIdx}", end="\r")
                observationIDs = [x.rstrip("\n") for x in observationIDs] # Clean off trailing newline

                df = df[df["observation_uuid"].isin(observationIDs)].copy()
                photoUUIDs = df["photo_uuid"].tolist()
                lengthAfter += len(photoUUIDs)

                with open(photoIDs, "a") as fp2:
                    fp2.write("\n".join(photoUUIDs))
                    fp2.write("\n")

                observationIDs = fp.readlines(readLines)
                subIdx += 1

    print(f"\nBefore: {lengthBefore} | After: {lengthAfter}")
    return photoIDs

def collect():
    parentDir = Path(__file__).parent
    extractedFolder = downloadAndExtract(parentDir)
    dataFolder = [key for key, _ in sorted({folder: int(folder.name[-8:]) for folder in extractedFolder.iterdir()}.items(), key=lambda x: x[1])][-1]
    photoIDs = getPhotoIDs(dataFolder)

    observations = dataFolder / "observations.csv" # Large
    observers = dataFolder / "observers.csv"
    photos = dataFolder / "photos.csv" # Large
    taxa = dataFolder / "taxa.csv"

    # Prepare taxonomy for getting species name
    taxonomy = pd.read_csv(taxa, dtype=object, sep="\t")
    taxonomy.drop(taxonomy[taxonomy["rank"] != "species"].index, inplace=True)
    taxonomy.drop(["ancestry", "rank_level", "rank", "active"], axis=1, inplace=True)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)

    # Prepare observers for getting creator name
    observers = pd.read_csv(observers, dtype=object, sep="\t")
    observers["creator"] = observers["name"].fillna(observers["login"])
    observers.drop(["name", "login"], axis=1, inplace=True)

    writer = BigFileWriter(parentDir / "inaturalist.csv", "subfiles")

    photosGen = cmn.chunkGenerator(photos, 1024*1024*2, "\t")
    for idx, df in enumerate(photosGen, start=1):
        df.drop(df[df["license"] == "CC-BY-NC-ND"].index, inplace=True)
        df.drop_duplicates("photo_uuid", inplace=True)
        df.drop_duplicates("observation_uuid", inplace=True)

        with open(photoIDs) as fp:
            ids = fp.read().split("\n")[:-1]

        df = df[df["photo_uuid"].isin(ids)] # Filter based on accepted photo uuids
        del ids
 
        # Add empty taxon_id and observed_on columns for filling with observations
        df["taxon_id"] = np.NaN
        df["observed_on"] = np.NaN

        df.set_index("observation_uuid", inplace=True)

        print(" "*100, end="\r") # Clear stdout
        obsvGen = cmn.chunkGenerator(observations, 1024*1024, "\t")        
        for subIdx, obsv in enumerate(obsvGen, start=1):
            print(f"At chunk: {idx} | subIdx: {subIdx}", end="\r")
            obsv.drop(obsv[obsv["quality_grade"] != "research"].index, inplace=True)
            obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id"], axis=1, inplace=True)
            obsv.drop_duplicates("observation_uuid", inplace=True)
            obsv.set_index("observation_uuid", inplace=True)

            for column in ("taxon_id", "observed_on"):
                df[[column]] = df[[column]].fillna(obsv[column])
        
        df.reset_index(drop=True, inplace=True) # Reset index and drop observation uuid
        df.drop(df[df["taxon_id"].isna()].index, inplace=True) # Remove NaN entries to allow conversion to int
        df["taxon_id"] = df["taxon_id"].astype(int) # Fixes an issue where taxon_id is sometimes float

        df = pd.merge(df, taxonomy, "left", on="taxon_id")
        df = pd.merge(df, observers, "left", on="observer_id")

        df["license"] = "© " + df["creator"] + ", some rights reserved (" + df["license"] + ")"
        df["identifier"] = "https://inaturalist-open-data.s3.amazonaws.com/photos/" + df["photo_id"] + "/original." + df["extension"]

        df.drop(["position", "taxon_id", "observer_id", "photo_id"], axis=1, inplace=True)

        # Renaming fields
        df.rename({
            "extension": "format",
            "photo_uuid": "datasetID",
            "name": "taxonName",
            "observed_on": "created"
        }, axis=1, inplace=True)

        df["type"] = "image"
        df["source"] = "iNaturalist"
        df["publisher"] = "iNaturalist"
        
        writer.writeDF(df)
    
    print()
    writer.oneFile(False)

if __name__ == "__main__":
    # Make sure you download inaturalist dump from https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz
    # Once extracted, update the "dataFolder" variable string to the new folder name
    
    collect()
