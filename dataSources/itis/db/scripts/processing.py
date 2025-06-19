from pathlib import Path
import pandas as pd
import sqlite3

def unpack(sqlFolderPath: Path, outputFolder: Path):
    subfolder = next(sqlFolderPath.iterdir())
    db = sqlite3.connect(subfolder / "ITIS.sqlite", isolation_level=None, detect_types=sqlite3.PARSE_COLNAMES)
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    outputFolder.mkdir(exist_ok=True)
    tables = cursor.fetchall()
    for tableName in tables:
        tableName = tableName[0]
        table = pd.read_sql_query(f"SELECT * from {tableName}", db)
        table.to_csv(outputFolder / f"{tableName}.csv", index=False)

    cursor.close()
    db.close()

def combine(folderPath: Path, outputFilePath: Path):
    df = pd.read_csv(folderPath / "taxonomic_units.csv", low_memory=False)
    df = df.drop([
        "unit_ind1",
        "unit_name1",
        "unit_ind2",
        "unit_name2",
        "unit_ind3",
        "unit_name3",
        "unit_ind4",
        "unit_name4",
        "n_usage"
    ], axis=1)

    kingdoms = pd.read_csv(folderPath / "kingdoms.csv", usecols=["kingdom_id", "kingdom_name"])
    df = df.merge(kingdoms, "left", on="kingdom_id")

    taxonTypes = pd.read_csv(folderPath / "taxon_unit_types.csv", usecols=["kingdom_id", "rank_id", "rank_name"])
    df = df.merge(taxonTypes, "left", on=["kingdom_id", "rank_id"])

    authors = pd.read_csv(folderPath / "taxon_authors_lkp.csv", usecols=["taxon_author_id", "taxon_author", "kingdom_id"])
    df = df.merge(authors, "left", on=["taxon_author_id", "kingdom_id"])
    authors = authors.rename({"taxon_author_id": "hybrid_author_id", "taxon_author": "hybrid_author"}, axis=1)
    df = df.merge(authors, "left", on=["hybrid_author_id", "kingdom_id"])

    synonyms = pd.read_csv(folderPath / "synonym_links.csv", usecols=["tsn", "tsn_accepted"])
    synonyms["taxonomic_status"] = "synonym"

    subDF = df[["tsn", "complete_name"]]
    subDF = subDF.rename({"tsn": "tsn_accepted", "complete_name": "accepted_name"}, axis=1)
    synonyms = synonyms.merge(subDF, "left", on="tsn_accepted")

    df = df.merge(synonyms, "left", on="tsn")
    df["taxonomic_status"] = df["taxonomic_status"].fillna("valid name")

    comments = pd.read_csv(folderPath / "comments.csv")
    commentLinks = pd.read_csv(folderPath / "tu_comments_links.csv", usecols=["tsn", "comment_id"])
    commentLinks = commentLinks.merge(comments, "left", on="comment_id")

    df = df.drop(["kingdom_id", "rank_id", "tsn_accepted"], axis=1)
    df["parent_tsn"] = df["parent_tsn"].astype("Int64")

    df["nomenclatural_code"] = "ICZN"
    df["scientific_name"] = df["complete_name"] + df["taxon_author"]

    df.to_csv(outputFilePath, index=False)
