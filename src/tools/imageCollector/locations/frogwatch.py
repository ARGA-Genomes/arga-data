import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
import pandas as pd
from lib.progressBar import ProgressBar

def run(dataDir: Path):
    baseURL = "https://museum.wa.gov.au"
    endpoint = "/views/ajax"

    params = {
        "js": 1,
        "view_name": "wam_species_info_sheets_frogwath",
        "view_display_id": "block_1",
        "view_path": "node/4332",
        "view_base_path": None,
        "view_dom_id": 1,
        "pager_element": 0
    }

    allFrogs = []

    page = 1
    session = requests.Session()
    while True:
        print(f"Collecting page: {page}", end="\r")
        response = session.get(baseURL + endpoint, params=params | {"page": page})
        # print(response.content)
        data = json.loads(response.text.replace("\\x3c", "<").replace("\\x3e", ">").replace("\\x26amp;", "&").replace("\\x26#039", "'"))

        soup = BeautifulSoup(data["display"], "html.parser")
        frogs = soup.find_all("a")[::2]
        frogs = [frog["href"] for frog in frogs]
        allFrogs.extend(frogs[:-1]) # Last link is a next page href

        if "page=1" in frogs[-1]: # Last link of last page page links back to page 1
            break

        page += 1

    print("Collecting images from each frog")
    progress = ProgressBar(len(allFrogs))
    imageData = []
    for frog in allFrogs:
        response = session.get(baseURL + frog)
        soup = BeautifulSoup(response.content, "html.parser")

        images = soup.find("div", {"class": "galleria fullwidth royalSlider rsDefault wamgallery"})
        for image in images.find_all("img"):
            imageURL = image["src"].replace("article_thumb", "page_full_gallery")
            response = requests.head(imageURL)
            dataType, dataFormat = response.headers["Content-Type"].split("/")

            imageData.append({
                "type": dataType,
                "format": dataFormat,
                "identifier": imageURL,
                "references": Path(imageURL).stem,
                "title": image["title"],
                "created": response.headers["Last-Modified"],
                "publisher": "WA Museum",
                "source": "https://museum.wa.gov.au/",
                "taxonName": frog.rsplit("/", 1)[-1],
                "size": response.headers["Content-Length"]
            })
        progress.update()

    df = pd.DataFrame.from_records(imageData)
    df.to_csv(dataDir / "frogwatch.csv", index=False)
