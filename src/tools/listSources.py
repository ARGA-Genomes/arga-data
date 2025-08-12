import argparse
from lib.data.sources import SourceManager

def breaker():
    print("-"*24)

if __name__ == "__main__":
    manager = SourceManager()

    parser = argparse.ArgumentParser(description="List available datasets for each source")
    parser.add_argument("source", help="Data location to view", metavar="SOURCE", nargs="?", default="")
    parser.add_argument("-d", "--databases", help="Show databases", action="store_true")
    parser.add_argument("-s", "--subsections", help="Show subsections", action="store_true")

    args = parser.parse_args()
    sources = manager.matchSources(args.source)
    breaker()
    for locationName, databases in sources.items():
        print(locationName)

        if not args.databases:
            breaker()
            continue

        for databaseName, subsections in databases.items():
            print(f"{2*' '}- {databaseName}")

            if not args.subsections:
                continue

            for subsection in subsections:
                if not subsection:
                    continue

                print(f"{4*' '}- {subsection}")

        breaker()
