import requests
import json

def build(outputFilePath):
    url = "https://files.jgi.doe.gov/search/?q=*&a=false&h=false&d=asc&p=1&x=10&api_version=2"

    # response = requests.get(url)
    # with open(outputFilePath.parent / "data.csv", 'w') as fp:
    #     json.dump(response.json(), fp, indent=4)

    with open(outputFilePath.parent / "data.csv") as fp:
        data = json.load(fp)

    print(data["organisms"][0])