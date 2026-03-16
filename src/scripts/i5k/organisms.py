import requests
import pandas as pd
import concurrent.futures
from pathlib import Path
from bs4 import BeautifulSoup, ResultSet

def _getSoup(suffix: str) -> BeautifulSoup:
    baseURL = "https://i5k.nal.usda.gov"
    response = requests.get(baseURL + suffix)
    return BeautifulSoup(response.text, "html.parser")

def _parseAnalysisRow(tableRow: ResultSet[any]) -> dict:
    columns = tableRow.find_all("td")

    subHref = columns[0].find("a").get("href")
    program = columns[1].get_text()
    constructed = columns[2].get_text()

    analysis = {
        program: {
            "date": constructed
        }
    }

    subSoup = _getSoup(subHref)
    subTable = subSoup.find("table")
    for subRow in subTable.find_all("tr"):
        subKey = subRow.find("th").get_text()
        subValue = subRow.find("td")

        if subKey == "Organism":
            continue

        if subKey == "Data Source":
            source = subValue.find_all("dd")
            sourceName = source[0].get_text()
            sourceURI = ""

            if len(source) > 1:
                try:
                    sourceURI = source[1].find("a").get("href")
                except AttributeError:
                    sourceURI = source[1].get_text()

            analysis[program]["source"] = {
                    "source name": sourceName,
                    "source uri": sourceURI
            }
            continue

        analysis[program][subKey] = subValue.get_text().replace("\n", " ")

    return analysis

def _parseOrganism(organismLink: ResultSet[any]) -> dict:
    name = organismLink.get_text()
    href = organismLink.get("href")

    organism = {"name": name, "analysis": {}}

    soup = _getSoup(href)
    for idx, table in enumerate(soup.find_all("tbody")): # Summary, Analysis, Assembly stats, Other information
        for row in table.find_all("tr"):
            if idx == 1: # Analysis handling
                organism["analysis"] |= _parseAnalysisRow(row)
                continue

            key = row.find("th")
            value = row.find("td")

            if idx == 3: # Other information
                if key.get_text() == "Links":
                    organism["Links"] = {link.get_text(): link.get("href") for link in value.find_all("a")}
                    continue

            if key.get_text() in ("Resource Type", "Organism Image", "Image Credit"):
                continue

            organism[key.get_text()] = value.get_text().replace("\n", " ")

    organism["analysis"] = organism.pop("analysis") # Move analysis to end of dict
    return organism

def retrieve(outputFilePath: Path) -> None:
    organisms = []
    links = []

    for i in range(6):
        soup = _getSoup(f"/organisms?page={i}")
        table = soup.find("tbody")
        links.extend(table.find_all("a"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = (executor.submit(_parseOrganism, link) for link in links)
        try:
            for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                print(f"Scraped organism: {idx}/{len(links)}", end="\r")
                organisms.append(future.result())

        except KeyboardInterrupt:
            executor.shutdown(cancel_futures=True)
            exit()
    
    pd.DataFrame.from_records(organisms).to_csv(outputFilePath, index=False)
