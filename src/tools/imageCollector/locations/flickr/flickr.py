import requests
import pandas as pd
import concurrent.futures
from pathlib import Path
from lib.tools.progressBar import SteppableProgressBar

def buildURL(apiKey, method, **kwargs):
    baseURL = "https://api.flickr.com/services/rest/?method="
    url = f"{baseURL}{method}&api_key={apiKey}"
    suffix = "&format=json&nojsoncallback=1"

    parameters = "&".join(f"{key}={value}" for key, value in kwargs.items())
    if parameters:
        url = f"{url}&{parameters}"

    return f"{url}{suffix}"

def cleanDimension(value) -> int:
    if value is None:
        return 1
    return int(value)

def processPhoto(apiKey: str, licenses: dict, photo: dict) -> dict:
    photoID = photo["id"]

    response = requests.get(buildURL(apiKey, "flickr.photos.getInfo", photo_id=photoID))
    if response.status_code != 200:
        print(f"Info error with id: {photoID}")
        return {}
    
    photoInfo = response.json()["photo"]

    response = requests.get(buildURL(apiKey, "flickr.photos.getSizes", photo_id=photoID))
    if response.status_code != 200:
        print(f"Size error with id: {photoID}")
        return {}
    
    photoSizes = response.json()["sizes"]
    image = sorted(photoSizes["size"], key=lambda x: cleanDimension(x["width"]) * cleanDimension(x["height"]), reverse=True)[0]
    
    tags = [tag["raw"] for tag in photoInfo["tags"]["tag"]]

    return {
        "type": "image",
        "format": image["source"].rsplit(".", 1)[-1],
        "identifier": image["source"],
        "references": image["url"].rsplit("/", 3)[0],
        "title": photoInfo["title"]["_content"],
        "description": photoInfo["description"]["_content"],
        "created": photoInfo["dates"]["taken"],
        "creator": photoInfo["owner"]["username"],
        "contributor": "",
        "publisher": photoInfo["owner"]["username"],
        "audience": "",
        "source": "flickr.com",
        "license": licenses[int(photoInfo["license"])],
        "rightsHolder": "",
        "datasetID": photoID,
        "taxonName": "",
        "width": image["width"],
        "height": image["height"],
        "tags": tags
    }

def collectUserPhotos(user: str, apiKey: str, licenses: dict) -> list[dict]:
    print(f"Getting photos for {user}")
    photosPerCall = 500

    response = requests.get(buildURL(apiKey, "flickr.people.getPhotos", user_id=user, per_page=1))
    data = response.json()

    totalPhotos = data["photos"]["total"]
    totalCalls = (totalPhotos / photosPerCall).__ceil__()

    photos = []
    for call in range(totalCalls):
        response = requests.get(buildURL(apiKey, "flickr.people.getPhotos", user_id=user, per_page=photosPerCall, page=call+1))
        photoData = response.json()["photos"]
        photoList = photoData.get("photo", [])
        progress = SteppableProgressBar(50, len(photoList), f"Processing page {call+1} / {totalCalls}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = (executor.submit(processPhoto, apiKey, licenses, photo) for photo in photoList)
        
            try:
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    progress.update()
                    if result:
                        photos.append(result)

            except (KeyboardInterrupt, ValueError):
                print("\nExiting...")
                executor.shutdown(cancel_futures=True)
                exit()

    return photos

def collect():
    baseDir = Path(__file__).parent

    with open(baseDir / "flickrkey.txt") as fp:
        apiKeyData = fp.read()
    apiKey, _ = apiKeyData.rstrip("\n").split("\n")

    with open(baseDir / "flickrusers.txt") as fp:
        users = fp.read()
    userList = users.rstrip("\n").split("\n")

    print("Getting license information")
    response = requests.get(buildURL(apiKey, "flickr.photos.licenses.getInfo"))
    licenseData = response.json()
    licenses = {licenseInfo["id"]: licenseInfo["name"] for licenseInfo in licenseData["licenses"]["license"]}

    for user in userList:
        if user.startswith("#"):
            continue

        photos = collectUserPhotos(user, apiKey, licenses)
        df = pd.DataFrame.from_records(photos)
        df.to_csv(baseDir / "userImages" / f"{user}.csv", index=False)

if __name__ == "__main__":
    # In the flickr folder make sure you have a flickrkey.txt and flickrusers.txt
    # users should have the userid of eadch user to collect from on each line
    # key should have the api key as the first line and the secret as the second
    
    collect()
