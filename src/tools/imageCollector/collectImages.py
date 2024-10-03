from locations.vicMuseum import vicMuseum
from locations.flickr import flickr
from locations.inaturalist import inaturalist
from argparse import ArgumentParser
from lib.tools.logger import Logger

if __name__ == "__main__":
    functionMap = {
        "flickr": flickr,
        "inaturalist": inaturalist,
        "vicMuseum": vicMuseum
    }
        
    parser = ArgumentParser()
    parser.add_argument("source", choices=list(functionMap), help="Photos to download")
    args = parser.parse_args()

    Logger.info(f"Collecting {args.source}")
    functionMap[args.source].collect()
