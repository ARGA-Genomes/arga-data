import argparse
from lib.sourceManager import SourceManager
from lib.sourceObjs.dbTypes import DBType

# Create a dwc file from source

if __name__ == '__main__':
    sources = SourceManager()

    parser = argparse.ArgumentParser(description="Prepare for DwC conversion")
    parser.add_argument("source", choices=sources.choices())

    # Settings for picking files from location source
    parser.add_argument('-s', '--start', default=0, type=int, help="Starting file in sequence to start preparing from")
    parser.add_argument('-q', '--quantity', default=1, type=int, help="Quantity of files to prepare from a source location")
    
    args = parser.parse_args()

    db = sources.getDB(args.source)
    dbType = db.getDBType()

    if dbType == DBType.SPECIFIC:
        db.createDwC()

    elif dbType == DBType.LOCATION or dbType == DBType.SCRIPT:
        db.createDwC(args.start, args.quantity)