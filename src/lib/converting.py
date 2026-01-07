from lib.processing.mapping import Map
from lib.processing.files import StackedFile

class Converter:

    _mapFileName = "map.json"

    def __init__(self, outputFile: StackedFile, prefix: str, entityInfo: tuple[str, str], chunkSize: int):
        self.outputFile = outputFile
        self.prefix = prefix
        self.entityInfo = entityInfo
        self.chunkSize = chunkSize

