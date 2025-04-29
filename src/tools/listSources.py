import argparse
from lib.data.sources import SourceManager

if __name__ == "__main__":
    manager = SourceManager()
    locations = manager.getLocations()

    parser = argparse.ArgumentParser(description="List available datasets for each source")
    parser.add_argument("-a", "--all", action="store_true", help="Show all databases as well as location sources.")
    parser.add_argument("-s", "--specific", choices=list(locations), help="Pick a single data source to show, gives all databases too.")

    args = parser.parse_args()

    if args.specific is None:
        locationItems = locations.items()
    else:
        locationItems = [(args.specific, locations[args.specific])]
        args.all = True

    print("*" * 40)
    for locationName, location in locationItems:
        print(locationName)

        if not args.all:
            continue

        for database in location.getDatabases():
            print(f"{2*' '}- {database}")
        print("*" * 40)

    if not args.all: # Print missing trailing spacer
        print("*" * 40)
