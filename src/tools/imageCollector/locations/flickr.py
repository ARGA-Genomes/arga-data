import requests
import pandas as pd
from pathlib import Path
from lib.tomlFiles import TomlLoader
from lib.progressBar import ProgressBar

def run(dataDir: Path):
    secrets = TomlLoader(dataDir.parent / "secrets.toml")

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
        print(f"Found {totalPhotos} photos")

        totalCalls = (totalPhotos / photosPerCall).__ceil__()
        progress = ProgressBar(totalPhotos)

        userPhotoParams = params | {
            "user_id": user,
            "per_page": photosPerCall,
            "extras": "description, license, date_taken, owner_name, original_format, tags, media, path_alias, url_sq, url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o"
        }

        photos = []
        for call in range(totalCalls):
            response = session.get(baseURL + "flickr.people.getPhotos", params=userPhotoParams | {"page": call})
            data: dict = response.json().get("photos", {})
            photoList: list[dict] = data.get("photo", [])

            for photo in photoList:
                if "url_o" not in photo:
                    largest = 0
                    suffix = ""

                    for imageSuffix in ("sq", "t", "s", "q", "m", "n", "z", "c", "l"):
                        size = photo.get(f"height_{imageSuffix}", 0) * photo.get(f"width_{imageSuffix}", 0)
                        if size >= largest:
                            largest = size
                            suffix = imageSuffix
                else:
                    suffix = "o"

                photos.append({
                    "type": "image",
                    "format": photo.get("originalformat", photo.get(f"url_{suffix}", "")),
                    "identifier": photo.get(f"url_{suffix}", ""),
                    "references": photo.get(f"url_{suffix}", "").rsplit("/", 1)[-1].split("_", 1)[0],
                    "title": photo.get("title", ""),
                    "description": photo.get("description", {}).get("_content", "").replace("\n", " "),
                    "created": photo.get("datetaken", ""),
                    "creator": photo.get("ownername", ""),
                    "contributor": "",
                    "publisher": photo.get("ownername", ""),
                    "audience": "",
                    "source": "flickr.com",
                    "license": licenses[int(photo.get("license", 0))],
                    "rightsHolder": "",
                    "datasetID": photo.get("id", ""),
                    "taxonName": "",
                    "width": photo.get(f"width_{suffix}", 0),
                    "height": photo.get(f"height_{suffix}", 0),
                    "tags": photo.get("tags", "")
                })
                
                progress.update()

        df = pd.DataFrame.from_records(photos)
        df.to_csv(dataDir / f"flickr_{user}.csv", index=False)
