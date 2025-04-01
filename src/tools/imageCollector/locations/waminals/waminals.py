import requests
from bs4 import BeautifulSoup

def collect():
    baseURL = "https://museum.wa.gov.au"
    page = 0

    
    headers = {
        "User-Agent": "Mozilla/5.0 (Android 4.4; Mobile; rv:41.0) Gecko/41.0 Firefox/41.0"
    }

    response = requests.get(f"{baseURL}/online-collections/waminals?page={page}")
    soup = BeautifulSoup(response.content, "html.parser")

    container = soup.find("div", {"class": "view-content"})
    for div in container.find_all("div", recursive=False):
        child = div.find("div")
        endpoint = child["about"]
        # print(endpoint)
        childResponse = requests.get(f"{baseURL}{endpoint}", headers=headers)
        # childResponse = session.get(f"{baseURL}{endpoint}")
        
        childSoup = BeautifulSoup(childResponse.content, "html.parser")

        imageContainer = childSoup.find("div", {"class": "rsContainer"})
        print(imageContainer)
        # for imageContainer in childSoup.find("div", {"class": "rsSlide"}):
        #     image = imageContainer.find("image")
        #     print(image["src"])

        # print(childSoup)
        data = {}
        for table in childSoup.find_all("table"):
            for row in table.find_all("tr"):
                key = row.find("th").text.replace(u"\xa0", " ").strip(": ")
                value = row.find("td").text
                data[key] = value

        print(data)
        return

if __name__ == "__main__":
    collect()
