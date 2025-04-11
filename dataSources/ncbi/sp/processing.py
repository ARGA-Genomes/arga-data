from pathlib import Path
import requests
import lib.xml as xml
from bs4 import BeautifulSoup
from io import BytesIO
from lib.bigFileWriter import BigFileWriter
import pandas as pd
from lib.progressBar import SteppableProgressBar
import concurrent.futures as cf

def _worker(ids: list[str]) -> list[dict]:
    xmlData = requests.get(f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id={','.join(ids)}&rettype=fasta&retmode=xml")
    return [xml.flattenElement(element) for element in xml.xmlGenerator(BytesIO(xmlData.content))]

def retrieve(outputFilePath: Path):
    response = requests.get(f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nuccore&term=\"+sp.\"+AND+country%3DAustralia+NOT+viruses+NOT+bacteria+NOT+archaea&retmax=500000")
    soup = BeautifulSoup(response.content, "xml")
    ids = [id.text for id in soup.find_all("Id")]
    
    recordsPerCall = 200
    totalCalls = (len(ids) / recordsPerCall).__ceil__()
    progress = SteppableProgressBar(totalCalls)

    records = []
    with cf.ThreadPoolExecutor() as executor:
        futures = (executor.submit(_worker, ids[call*recordsPerCall:(call+1)*recordsPerCall]) for call in range(totalCalls))
        for future in cf.as_completed(futures):
            records.extend(future.result())
            progress.update()

    pd.DataFrame.from_dict(records).to_csv(outputFilePath.parent / "nuccore.csv", index=False)
