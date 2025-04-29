import pandas as pd
from pathlib import Path
import lib.common as cmn

def run():
    baseDir = Path(__file__)

    dataFolder = baseDir / "inaturalist-open-data-20230827"
    createdFolder = baseDir / "createdFiles"

    observations = dataFolder / "observations.csv"
    photos = dataFolder / "photos.csv"
    taxa = dataFolder / "taxa.csv"

    taxonFile = createdFolder / "taxonCount.csv"
    if not taxonFile.exists():
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
        df = df[df["rank"].isin(["species", "subspecies"])]
        df.to_csv(speciesFile, index=False)
    else:
        df = pd.read_csv(speciesFile)

    taxonIDFile = createdFolder / "taxonIDs.txt"
    if not taxonIDFile.exists():
        ids = df["taxon_id"].astype(str).tolist()
        with open(taxonIDFile, "w") as fp:
            fp.write("\n".join(ids))
    else:
        with open(taxonIDFile) as fp:
            ids = fp.read().split("\n")

    obsvIDs = createdFolder / "observationIDs.txt"
    if not obsvIDs.exists():
        chunkGen = cmn.chunkGenerator(observations, 1024*1024*4, "\t", usecols=["taxon_id", "observation_uuid"])

        for idx, df in enumerate(chunkGen, start=1):
            print(f"At chunk: {idx}", end="\r")

            with open(obsvIDs, "a") as fp:
                fp.write("\n".join(df["observation_uuid"].tolist()))
                fp.write("\n")

        print()

    photoIDs = createdFolder / "photoIDs.txt"

    lengthBefore = 0
    lengthAfter = 0

    chunkGen = cmn.chunkGenerator(photos, 1024*1024*4, sep="\t", usecols=["photo_uuid", "observation_uuid"])
    readBytes = 1024*1024*256

    for idx, df in enumerate(chunkGen, start=1):
        lengthBefore += len(df)

        with open(obsvIDs) as fp:
            observationIDs = fp.readlines(readBytes)

            subIdx = 1
            print(" "*50, end="\r")
            while observationIDs:
                print(f"At chunk {idx}, subchunk {subIdx}", end="\r")
                observationIDs = [x.rstrip("\n") for x in observationIDs] # Clean off trailing newline

                df = df[df["observation_uuid"].isin(observationIDs)].copy()
                photoUUIDs = df["photo_uuid"].tolist()
                lengthAfter += len(photoUUIDs)

                with open(photoIDs, "a") as fp2:
                    fp2.write("\n".join(photoUUIDs))
                    fp2.write("\n")

                observationIDs = fp.readlines(readBytes)
                subIdx += 1

    print()
    print(lengthBefore, lengthAfter)

if __name__ == "__main__":
    run()
