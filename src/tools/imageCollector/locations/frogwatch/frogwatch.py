import requests
from bs4 import BeautifulSoup

def collect():
    url = "https://museum.wa.gov.au/explore/frogwatch/frogs"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    container = soup.find("div", {"class": "view-content masonry"})
    for item in container.find_all("div", recursive=False):
        endpoint = item.find("a")["href"]

        print(endpoint)

if __name__ == "__main__":
    collect()