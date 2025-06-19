import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd

def build(folderPath: Path, tableName: str) -> None:
    url = "http://reefgenomics.org/sitemap.html"
    rawHTML = requests.get(url)
    soup = BeautifulSoup(rawHTML.text, 'html.parser')

    for header in soup.find_all("h2"):
        section = header.text.split()[1]
        if section != tableName:
            continue

        table = header.find_next("table")
        rows = table.find_all("tr")
        headers = [header.text for header in rows.pop(0).find_all("th")]
        rowData = []

        for row in rows:
            entries = row.find_all("td")
            link = entries[1].find("a").get("href")
            rowData.append({header: value.text for header, value in zip(headers, entries)} | {"link": link})

        pd.DataFrame.from_records(rowData).to_csv(folderPath / f"{tableName}.csv", index=False)
