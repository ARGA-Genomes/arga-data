from pathlib import Path
import pandas as pd
import lib.common as cmn
from lib.bigFiles import DFWriter
import numpy as np
import lib.downloading as dl
import lib.zipping as zp

observationsFile = "observations.csv" # Large
observersFile = "observers.csv"
photosFile = "photos.csv" # Large
taxaFile = "taxa.csv"

def downloadFile(dataDir: Path) -> Path:
    zipFilePath = dataDir / "inaturalist.tar.gz"
    extractedFile = zipFilePath.with_suffix("").with_suffix("")

    if extractedFile.exists():
        print("File alrady exists, skipping preparation")
        return next(extractedFile.iterdir())
    
    if not zipFilePath.exists():
        dl.download("https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz", zipFilePath, verbose=True)
    else:
        print("File found locally, skipping download")

    outputFile = zp.extract(zipFilePath)
    return next(outputFile.iterdir())

def collectPhotoIDs(datafolderPath: Path) -> Path:
    photoIDsPath = datafolderPath / "photoIDs.csv"
    if photoIDsPath.exists():
        print("Photo IDs already exists, skipping collection")
        return photoIDsPath
    
    print("Getting taxon counts")
    df = pd.read_csv(datafolderPath / observationsFile, sep="\t", usecols=["taxon_id"], dtype=object)
    df = df["taxon_id"].value_counts().to_frame("counts") # Overwrite df with value counts Series
    df = df.reset_index()

    print("Reducing to species and subspecies")
    taxonomy = pd.read_csv(datafolderPath / taxaFile, sep="\t", dtype=object)
    df = df.merge(taxonomy, "left", on="taxon_id")
    del taxonomy

    df = df[df["rank"].isin(["species", "subspecies"])] # Reduce to only species or subspecies
    df = df["taxon_id"].to_frame("taxon_id") # Reduce df to a series of just taxon ids, which is only species or subspecies

    print("Getting observation uuids")
    observations = pd.read_csv(datafolderPath / observationsFile, sep="\t", usecols=["taxon_id", "observation_uuid"])
    df = df.merge(observations, "left", on="taxon_id")["observation_uuid"] # Update df to only uuids of valid taxon ids
    del observations

    print("Getting photo uuids")
    sections = []
    chunkGen = cmn.chunkGenerator(datafolderPath / photosFile, 1024*1024*4, sep="\t", usecols=["photo_uuid", "observation_uuid"])
    for chunk in chunkGen:
        chunk = chunk[chunk["observation_uuid"].isin(observations)]
        sections.append(chunk["photo_uuid"])

    print("Combining into one file")
    df = pd.concat(sections)
    df.to_csv(photoIDsPath, index=False)

def run(dataDir: Path):
    inaturalistFolder = downloadFile(dataDir)
    photoIDsPath = collectPhotoIDs(inaturalistFolder)

    # Prepare taxonomy for getting species name
    taxonomy = pd.read_csv(inaturalistFolder / taxaFile, dtype=object, sep="\t")
    taxonomy = taxonomy.drop(taxonomy[taxonomy["rank"] != "species"].index)
    taxonomy = taxonomy.drop(["ancestry", "rank_level", "rank", "active"], axis=1)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)

    # Prepare observers for getting creator name
    observers = pd.read_csv(inaturalistFolder / observersFile, dtype=object, sep="\t")
    observers["creator"] = observers["name"].fillna(observers["login"])
    observers = observers.drop(["name", "login"], axis=1)

    writer = DFWriter(Path("./inaturalist.csv"))

    photosGen = cmn.chunkGenerator(inaturalistFolder / photosFile, 1024*1024*2, "\t")
    for idx, df in enumerate(photosGen, start=1):
        df = df.drop(df[df["license"] == "CC-BY-NC-ND"].index)
        df = df.drop_duplicates(["photo_uuid", "observation_uuid"])

        with open(photoIDsPath) as fp:
            ids = fp.read().split("\n")[:-1]

        df = df[df["photo_uuid"].isin(ids)] # Filter based on accepted photo uuids
        del ids
 
        # Add empty taxon_id and observed_on columns for filling with observations
        df["taxon_id"] = np.NaN
        df["observed_on"] = np.NaN

        df = df.set_index("observation_uuid")

        print(" "*100, end="\r") # Clear stdout
        obsvGen = cmn.chunkGenerator(inaturalistFolder / observersFile, 1024*1024, "\t", usecols=["quality_grade", "observation_uuid", "taxon_id", "observed_on"])        
        for subIdx, obsv in enumerate(obsvGen, start=1):
            print(f"At: chunk {idx} | sub chunk {subIdx}", end="\r")
            obsv = obsv.drop(obsv[obsv["quality_grade"] != "research"].index)
            # obsv = obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id"], axis=1)
            obsv = obsv.drop_duplicates("observation_uuid")
            obsv = obsv.set_index("observation_uuid")

            for column in ("taxon_id", "observed_on"):
                df[column] = df[column].fillna(obsv[column])
        
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
        
        writer.write(df)
    
    print()
    writer.combine(True)
