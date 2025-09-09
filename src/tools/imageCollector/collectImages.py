from locations import flickr, inaturalist, vicMuseum
import argparse

# print("Collecting Vic Museum...")
# vicMuseum.run()

# print("Collecting flickr...")
# # In the flickr folder make sure you have a flickrkey.txt and flickrusers.txt
# # users should have the userid of eadch user to collect from on each line
# # key should have the api key as the first line and the secret as the second
# flickr.run()

# print("Collecting inaturalist...")
# # Make sure you download inaturalist dump from https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz
# # Once extracted, update the "dataFolder" variable string to the new folder name
# inaturalist.run()

if __name__ == "__main__":

    options = {
        "vicmuseum": ("Vic Museum", vicMuseum),
        "flickr": ("Flickr", flickr),
        "inaturalist": ("iNaturalist", inaturalist)
    }

    allOptions = "all"

    parser = argparse.ArgumentParser(description="Collect images")
    parser.add_argument("source", nargs="*", choices=list(options.keys()) + [allOptions], default=allOptions)
    args = parser.parse_args()

    choices = options.keys() if args.source == allOptions else args.source
    for choice in choices:
        name, library = options[choice]
        print(f"Collecting {name}...")
        library.run()
