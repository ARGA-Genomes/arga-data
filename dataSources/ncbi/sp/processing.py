from pathlib import Path
import requests
import lib.xml as xml
from bs4 import BeautifulSoup
from lib.bigFileWriter import BigFileWriter
import pandas as pd
from lib.progressBar import SteppableProgressBar
import lib.downloading as dl

def retrieve(outputFilePath: Path):
    baseURL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    query = "\"+sp.\"+AND+country%3DAustralia+NOT+viruses+NOT+bacteria+NOT+archaea"

    response = requests.get(f"{baseURL}esearch.fcgi?db=nuccore&term={query}&usehistory=y")
    soup = BeautifulSoup(response.content, "xml")

    count = soup.find("Count").text
    query = soup.find("QueryKey").text
    webenv = soup.find("WebEnv").text

    chunkSize = 10000
    totalCalls = (int(count) / chunkSize).__ceil__()

    writer = BigFileWriter(outputFilePath.parent / "nuccore.csv")
    writer.subfileDir.mkdir(exist_ok=True)
    tempFile = writer.subfileDir / "download.xml"

    for call in range(totalCalls):
        dl.download(f"{baseURL}efetch.fcgi?db=nuccore&WebEnv={webenv}&query_key={query}&retstart={call*chunkSize}&retmax={chunkSize}&rettype=fasta&retmode=xml", tempFile, chunkSize=256*1024*1024)

        records = []
        for element in xml.xmlGenerator(tempFile):
            records.append(xml.flattenElement(element))

        writer.writeDF(pd.DataFrame.from_records(records))
        tempFile.unlink()

        print(f"Completed section {call+1} / {totalCalls}")

    writer.oneFile()
