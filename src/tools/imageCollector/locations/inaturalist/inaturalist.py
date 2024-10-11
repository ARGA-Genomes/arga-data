from pathlib import Path
import pandas as pd
import lib.common as cmn
from lib.bigFileWriter import BigFileWriter
import numpy as np
from lib.tools.downloader import Downloader
from lib.tools.zipping import extract
from lib.tools.progressBar import SteppableProgressBar
from lib.tools.logger import Logger

def createTaxonIDs(observations: Path, taxa: Path, outputPath: Path) -> None:
    values = pd.read_csv(observations, sep="\t", usecols=["taxon_id"])
    counts = values.value_counts()
    df = counts.to_frame("counts")
    df = df.reset_index()

    del values

    df["taxon_id"] = df["taxon_id"].astype(int)

    taxonomy = pd.read_csv(taxa, sep="\t", dtype=object)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)
    df = df.merge(taxonomy, "left", on="taxon_id")
    df = df[df["rank"].isin(["species", "subspecies"])]
    ids = df["taxon_id"].astype(str).tolist()

    with open(outputPath, "w") as fp:
        fp.write("\n".join(ids))

def createObservationIDs(observations: Path, outputFilePath: Path) -> None:
    series = pd.read_csv(observations, sep="\t", usecols=["observation_uuid"])["observation_uuid"]
    
    with open(outputFilePath, "w") as fp:
        fp.write("\n".join(series.tolist()))

def createPhotoIDs(photos: Path, observationIDs: Path, outputFilePath: Path) -> None:
    lengthBefore = 0
    lengthAfter = 0
    linesToRead = 1024*1024*65

    chunkGen = cmn.chunkGenerator(photos, 1024*1024*4, sep="\t", usecols=["photo_uuid", "observation_uuid"])
    for idx, df in enumerate(chunkGen, start=1):
        lengthBefore += len(df)

        with open(observationIDs) as fp:
            ids = fp.readlines(linesToRead)

        subIdx = 1
        while ids:
            idx = [i.rstrip("\n") for i in ids]

            print(f"At chunk: {idx} | sub-chunk: {subIdx}", end="\r")
            df = df[df["observation_uuid"].isin(ids)].copy()
            photoUUIDs = df["photo_uuid"].tolist()
            lengthAfter += len(photoUUIDs)

            with open(outputFilePath, "a") as outputFP:
                outputFP.write("\n".join(photoUUIDs))
                outputFP.write("\n")

            subIdx += 1
            ids = fp.readlines(linesToRead)

    print(f"\nBefore: {lengthBefore} | After: {lengthAfter}")

def collect():
    parentDir = Path(__file__).parent
    createdFiles = parentDir / "createdFiles"
    if not createdFiles.exists():
        createdFiles.mkdir()

    downloadPath = parentDir / "inaturalist-open-data-latest.tar.gz"

    if not downloadPath.exists():
        downloader = Downloader()
        downloader.download("https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz", downloadPath, verbose=True)

    extractedFolder = extract(downloadPath)
    dataFolder = [key for key, _ in sorted({folder: int(folder.name[-8:]) for folder in extractedFolder.iterdir()}.items(), key=lambda x: x[1])][-1]

    observations = dataFolder / "observations.csv" # Large
    observers = dataFolder / "observers.csv"
    photos = dataFolder / "photos.csv" # Large
    taxa = dataFolder / "taxa.csv"

    # Create a list of valid taxon IDs
    taxonIDFile = createdFiles / "taxonIDs.txt"
    if not taxonIDFile.exists():
        Logger.info("Creating Taxon IDs")
        createTaxonIDs(observations, taxa, taxonIDFile)

    # Create a list of observation IDs
    observationIDs = createdFiles / "observationIDs.txt"
    if not observationIDs.exists():
        Logger.info("Creating Observation IDs")
        createObservationIDs(observations, observationIDs)

    # Create a list of photo IDs
    photoIDs = createdFiles / "photoIDs.txt"
    if not photoIDs.exists():
        Logger.info("Creating Photo IDs")
        createPhotoIDs(photos, observationIDs, photoIDs)

    # Prepare taxonomy for getting species name
    taxonomy = pd.read_csv(taxa, dtype=object, sep="\t")
    taxonomy.drop(taxonomy[taxonomy["rank"] != "species"].index, inplace=True)
    taxonomy.drop(["ancestry", "rank_level", "rank", "active"], axis=1, inplace=True)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)

    # Prepare observers for getting creator name
    observers = pd.read_csv(observers, dtype=object, sep="\t")
    observers["creator"] = observers["name"].fillna(observers["login"])
    observers = observers.drop(["name", "login"], axis=1)

    writer = BigFileWriter(parentDir / "inaturalist.csv", "subfiles")
    photosGen = cmn.chunkGenerator(photos, 1024*1024*2, "\t")

    Logger.info("Building photos")
    for idx, df in enumerate(photosGen, start=1):
        df = df.drop(df[df["license"] == "CC-BY-NC-ND"].index)
        df = df.drop_duplicates("photo_uuid")
        df = df.drop_duplicates("observation_uuid")

        with open(photoIDs) as fp:
            ids = fp.read().rstrip("\n").split("\n")

        df = df[df["photo_uuid"].isin(ids)] # Filter based on accepted photo uuids
        del ids
 
        # Add empty taxon_id and observed_on columns for filling with observations
        df["taxon_id"] = np.NaN
        df["observed_on"] = np.NaN

        df = df.set_index("observation_uuid")

        print(" "*100, end="\r") # Clear stdout
        obsvGen = cmn.chunkGenerator(observations, 1024*1024, "\t")
        for subIdx, obsv in enumerate(obsvGen, start=1):
            print(f"At chunk: {idx} | subIdx: {subIdx}", end="\r")
            obsv = obsv.drop(obsv[obsv["quality_grade"] != "research"].index)
            obsv = obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id"], axis=1)
            obsv = obsv.drop_duplicates("observation_uuid")
            obsv = obsv.set_index("observation_uuid")

            df.taxon_id = df.taxon_id.fillna(obsv.taxon_id)
            df.observed_on = df.observed_on.fillna(obsv.observed_on)
        
        df = df.reset_index(drop=True) # Reset index and drop observation uuid
        df = df.drop(df[df["taxon_id"].isna()].index) # Remove NaN entries to allow conversion to int
        df["taxon_id"] = df["taxon_id"].astype(int) # Fixes an issue where taxon_id is sometimes float

        df = pd.merge(df, taxonomy, "left", on="taxon_id")
        df = pd.merge(df, observers, "left", on="observer_id")

        df["license"] = "© " + df["creator"] + ", some rights reserved (" + df["license"] + ")"
        df["identifier"] = "https://inaturalist-open-data.s3.amazonaws.com/photos/" + df["photo_id"] + "/original." + df["extension"]

        df = df.drop(["position", "taxon_id", "observer_id", "photo_id"], axis=1)

        # Renaming fields
        df = df.rename({
            "extension": "format",
            "photo_uuid": "datasetID",
            "name": "taxonName",
            "observed_on": "created"
        }, axis=1)

        df["type"] = "image"
        df["source"] = "iNaturalist"
        df["publisher"] = "iNaturalist"
        
        writer.writeDF(df)
        # break
    
    print()
    writer.oneFile(False)

if __name__ == "__main__":
    # Make sure you download inaturalist dump from https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz
    # Once extracted, update the "dataFolder" variable string to the new folder name
    
    collect()
