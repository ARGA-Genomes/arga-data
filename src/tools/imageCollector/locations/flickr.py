import requests
import pandas as pd
# import concurrent.futures as cf
from pathlib import Path
from lib.tomlFiles import TomlLoader
from lib.progressBar import ProgressBar

# def processPhoto(session: requests.Session, apiKey: str, licenses: dict, photo: dict) -> dict:
#     photoID = photo["id"]

#     response = session.get(buildURL(apiKey, "flickr.photos.getInfo", photo_id=photoID))
#     if response.status_code != 200:
#         print(f"Info error with id: {photoID}")
#         return {}
    
#     photoInfo = response.json()["photo"]

#     response = requests.get(buildURL(apiKey, "flickr.photos.getSizes", photo_id=photoID))
#     if response.status_code != 200:
#         print(f"Size error with id: {photoID}")
#         return {}
    
#     photoSizes = response.json()["sizes"]
#     image = sorted(photoSizes["size"], key=lambda x: (1 if x["width"] is None else int(x["width"])) * (1 if x["height"] is None else int(x["height"])), reverse=True)[0]
    
#     tags = [tag["raw"] for tag in photoInfo["tags"]["tag"]]

#     return {
#         "type": "image",
#         "format": image["source"].rsplit(".", 1)[-1],
#         "identifier": image["source"],
#         "references": image["url"].rsplit("/", 3)[0],
#         "title": photoInfo["title"]["_content"],
#         "description": photoInfo["description"]["_content"],
#         "created": photoInfo["dates"]["taken"],
#         "creator": photoInfo["owner"]["username"],
#         "contributor": "",
#         "publisher": photoInfo["owner"]["username"],
#         "audience": "",
#         "source": "flickr.com",
#         "license": licenses[int(photoInfo["license"])],
#         "rightsHolder": "",
#         "datasetID": photoID,
#         "taxonName": "",
#         "width": image["width"],
#         "height": image["height"],
#         "tags": tags
#     }

def run():
    baseDir = Path(__file__).parents[1]
    dataDir = baseDir / "data"
    secrets = TomlLoader(baseDir / "secrets.toml")

    photosPerCall = 500
    session = requests.Session()
    baseURL = "https://api.flickr.com/services/rest/?method="
    params = {
        "api_key": secrets.flickr.key,
        "format": "json",
        "nojsoncallback": 1
    }

    # Get licenses
    print("Getting license information")
    response = session.get(baseURL + "flickr.photos.licenses.getInfo", params=params)
    licenseData = response.json()
    licenses = {licenseInfo["id"]: licenseInfo["name"] for licenseInfo in licenseData["licenses"]["license"]}

    for user in secrets.flickr.users:
        if user.startswith("_"):
            continue

        print(f"Getting photos for {user}")
        response = session.get(baseURL + "flickr.people.getPhotos", params=params | {"user_id": user, "per_page": 1})
        data = response.json()

        totalPhotos = data["photos"]["total"]
        totalCalls = (totalPhotos / photosPerCall).__ceil__()
        progress = ProgressBar(totalPhotos)

        userPhotoParams = params | {"user_id": user, "per_page": photosPerCall}

        photos = []
        for call in range(totalCalls):
            response = session.get(baseURL + "flickr.people.getPhotos", params=userPhotoParams | {"page": call})
            photoData = response.json()["photos"]
            photoList = photoData.get("photo", [])

            for photo in photoList:
                photoID = photo["id"]
                photoParams = params | {"photo_id": photoID}

                response = session.get(baseURL + "flickr.photos.getInfo", params=photoParams)
                if response.status_code != 200:
                    print(f"Info error with id: {photoID}")
                    print(response.content)
                    return
                
                photoInfo = response.json()["photo"]
                response = session.get(baseURL + "flickr.photos.getSizes", params=photoParams)
                if response.status_code != 200:
                    print(f"Size error with id: {photoID}")
                    print(response.content)
                    return
                
                photoSizes = response.json()["sizes"]
                image = sorted(photoSizes["size"], key=lambda x: (1 if x["width"] is None else int(x["width"])) * (1 if x["height"] is None else int(x["height"])), reverse=True)[0]
                tags = [tag["raw"] for tag in photoInfo["tags"]["tag"]]

                photos.append({
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
                })

                progress.update()

            # with cf.ProcessPoolExecutor() as executor:
            #     futures = (executor.submit(processPhoto, session, secrets.flickr.key, licenses, photo) for photo in photoList)
            
            #     try:
            #         for future in cf.as_completed(futures):
            #             result = future.result()
            #             progress.update()
            #             if result:
            #                 photos.append(result)

            #     except (KeyboardInterrupt, ValueError):
            #         print("\nExiting...")
            #         executor.shutdown(cancel_futures=True)
            #         exit()

        df = pd.DataFrame.from_records(photos)
        df.to_csv(dataDir / f"flickr_{user}.csv", index=False)
