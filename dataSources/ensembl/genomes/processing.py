import json
import pandas as pd
import requests
from pathlib import Path
import lib.downloading as dl
import lib.zipping as zp
from bs4 import BeautifulSoup

def download(url: str, outputDir: Path, overwrite: bool = False) -> Path:
    localFile = Path(outputDir / f"{'_'.join(url.rsplit('/', 2)[-2:])}")

    if not localFile.exists() or overwrite:
        localFile.unlink(True)

        success = dl.download(url, localFile, verbose=True)

        if not success:
            return None

    if zp.canBeExtracted(localFile):
        if not zp.extractsTo(localFile, outputDir).exists() or overwrite:
            return zp.extract(localFile, overwrite=overwrite)
    
    return localFile

def speciesDownload(outputFilePath: Path) -> None:
    url = "https://rest.ensembl.org/info/species?"
    request = requests.get(url, headers={ "Content-Type" : "application/json"})
 
    if not request.ok:
        request.raise_for_status()
        return
    
    data = request.json()
    df = pd.DataFrame.from_records(data["species"])
    df.to_csv(outputFilePath, index=False)

def flatten(filePath: Path, outputFilePath: Path) -> None:
    with open(filePath) as fp:
        data = json.load(fp)

    records = []
    for record in data:
        organism = record.pop("organism")
        record["organism_name"] = organism.pop("name")

        assembly = record.pop("assembly")
        assembly.pop("sequences")

        release = record.pop("data_release")

        records.append(organism | assembly | release | record)

    pd.DataFrame.from_records(records).to_csv(outputFilePath, index=False)

def enrich(filePath: Path, subsection: str, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath, sep="\t", dtype=object, index_col=False)

    baseURL = f"http://ftp.ensemblgenomes.org/pub/{subsection}/current/mysql/"
    outputFolder = Path(outputFilePath.parent / "enrichFiles")
    outputFolder.mkdir(exist_ok=True)

    records = []
    for _, row in df.iterrows():
        db = row["core_db"]
        id = row["species_id"]

        tempBase = baseURL + db + "/"
        metaURL = tempBase + "meta.txt.gz"
        statsURL = tempBase + "genome_statistics.txt.gz"

        meta = download(metaURL, outputFolder)
        metaDF = pd.read_csv(meta, header=None, sep="\t", index_col=0, names=["id", "column", "value"], dtype=object)
        
        relevant = metaDF[metaDF["id"] == id]
        relevantData = dict(zip(relevant.column, relevant.value))
        record = {"name": row["#name"]} | {key.replace(".", "_"): value for key, value in relevantData.items()}

        stats = download(statsURL, outputFolder)
        statsDF = pd.read_csv(stats, header=None, sep="\t", index_col=0, names=["column", "value", "id", "n", "timestamp"], dtype=object)
        relevant = statsDF[statsDF["id"] == id]
        relevantData = dict(zip(relevant.column, relevant.value))
        record |= relevantData

        records.append(record)

    enrichDF = pd.DataFrame.from_records(records)
    uniqueCols = enrichDF.columns.difference(df.columns)
    df = df.merge(enrichDF[uniqueCols], how="outer", left_on="#name", right_on="name")
    df.to_csv(outputFilePath, index=False)

def combine(metadataPath: Path, statsPath: Path, outputFilePath: Path) -> None:
    metadata = pd.read_csv(metadataPath)
    stats = pd.read_csv(statsPath)

    uniqueColumns = stats.columns.difference(metadata.columns)
    pd.merge(metadata, stats[uniqueColumns], how="outer", left_on="display_name", right_on="#name").to_csv(outputFilePath, index=False)

def collectVGP(outputFilePath: Path):
    def cleanText(text: str) -> str:
        return text.strip(" \n")
    
    url = "https://projects.ensembl.org/vgp/"

    pageData = requests.get(url)
    soup = BeautifulSoup(pageData.text, "html.parser")

    table = soup.find("table")
    tableHeader = table.find("thead")
    columns = [cleanText(header.text) for header in tableHeader.find_all("th")]

    tableBody = table.find("tbody")
    rowData = []
    for row in tableBody.find_all("tr"):
        data = {}
        for header, element in zip(columns, row.find_all("td")):
            if header == "Image":
                continue

            if header in ("Species", "Accession"):
                data[header] = cleanText(element.text)
            elif header in ("Annotation", "Proteins", "Transcripts", "Softmasked genome"):
                for link in element.find_all("a"):
                    data[f"{header.replace(' ', '_')}_{cleanText(link.text.lower())}"] = link["href"]
            elif header in ("Repeat library", "Other data"):
                for link in element.find_all("a"):
                    data[cleanText(link.text.lower())] = link["href"]
            elif header == "View in browser":
                link = element.find("a")
                data |= collectStats(link["href"])
            else:
                print(f"Unknown header: {header}")

        rowData.append(data)

    pd.DataFrame.from_records(rowData).to_csv(outputFilePath, index=False)

def collectStats(url: str) -> dict:
    pageData = requests.get(url)
    soup = BeautifulSoup(pageData.text, "html.parser")

    data = {}
    for table in soup.find_all("table"):
        body = table.find("tbody")
        for row in body.find_all("tr"):
            key, value = row.find_all("td")
            data[key.find("b").text.replace(" ", "_")] = value.text.replace(",", "") if value.text.replace(",", "").isdigit() else value.text

    return data
