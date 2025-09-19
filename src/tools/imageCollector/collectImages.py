from locations import flickr, inaturalist, vicMuseum, qm
import argparse
from pathlib import Path

if __name__ == "__main__":

    options = {
        "vicmuseum": ("Vic Museum", vicMuseum),
        "flickr": ("Flickr", flickr),
        "inaturalist": ("iNaturalist", inaturalist),
        "qm": ("Queensland Museum", qm)
    }

    allOptions = "all"

    parser = argparse.ArgumentParser(description="Collect images")
    parser.add_argument("source", nargs="*", choices=list(options.keys()) + [allOptions], default=allOptions)
    args = parser.parse_args()

    choices = options.keys() if args.source == allOptions else args.source
    dataDir = baseDir = Path(__file__).parents[0] / "data"
    for choice in choices:
        name, library = options[choice]
        print(f"Collecting {name}...")
        library.run(dataDir)
