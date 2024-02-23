from lib.sourceObjs.argParseWrapper import SourceArgParser

if __name__ == '__main__':
    parser = SourceArgParser(description="Manually update a source")
    
    sources, selectedFiles, overwrite, kwargs = parser.parse_args()
    for source in sources:
        source.update()
