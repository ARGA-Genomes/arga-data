from pathlib import Path
import pandas as pd
import lib.common as cmn
from lib.bigFileWriter import BigFileWriter
import numpy as np

def run():
    dataFolder = Path("./inaturalist-open-data-20230827")

    observations = dataFolder / "observations.csv" # Large
    observers = dataFolder / "observers.csv"
    photos = dataFolder / "photos.csv" # Large
    taxa = dataFolder / "taxa.csv"

    photoIDs = Path("./createdFiles/photoIDs.txt")

    # Prepare taxonomy for getting species name
    taxonomy = pd.read_csv(taxa, dtype=object, sep="\t")
    taxonomy.drop(taxonomy[taxonomy["rank"] != "species"].index, inplace=True)
    taxonomy.drop(["ancestry", "rank_level", "rank", "active"], axis=1, inplace=True)
    taxonomy["taxon_id"] = taxonomy["taxon_id"].astype(int)

    # Prepare observers for getting creator name
    observers = pd.read_csv(observers, dtype=object, sep="\t")
    observers["creator"] = observers["name"].fillna(observers["login"])
    observers.drop(["name", "login"], axis=1, inplace=True)

    writer = BigFileWriter(Path("./inaturalist.csv"), "subfiles")

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
            print(f"At: chunk {idx} | sub chunk {subIdx}", end="\r")
            obsv.drop(obsv[obsv["quality_grade"] != "research"].index, inplace=True)
            obsv.drop(["latitude" , "longitude", "positional_accuracy", "quality_grade", "observer_id"], axis=1, inplace=True)
            obsv.drop_duplicates("observation_uuid", inplace=True)
            obsv.set_index("observation_uuid", inplace=True)

            for column in ("taxon_id", "observed_on"):
                df[column].fillna(obsv[column], inplace=True)
        
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
    # downloadURL = "https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz"

    run()
