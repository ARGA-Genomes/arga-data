from xml.etree import cElementTree as ET
from pathlib import Path
from enum import Enum
from typing import Generator

class ElementProperty(Enum):
    TAG = "tag"
    TEXT = "text"
    ATTR = "attributes"
    CHLD = "children"

class ElementContainer:
    def __init__(self, element: ET.Element, parent: 'ElementContainer' = None):
        self.tag = element.tag
        self.text = element.text
        self.attributes = element.attrib.copy()

        self.parent = parent
        self.children: list[ElementContainer] = []
    
    def addChild(self, container: 'ElementContainer') -> None:
        self.children.append(container)

    def getData(self) -> dict[ElementProperty, any]:
        return {
            ElementProperty.TAG: self._cleanText(self.tag),
            ElementProperty.TEXT: self._cleanText(self.text),
            ElementProperty.ATTR: self.attributes,
            ElementProperty.CHLD: [child.getData() for child in self.children]
        }
    
    def _cleanText(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        
        return text.translate(text.maketrans("\t\n\r", "   ")).strip()

def _basicParse(data: dict[ElementProperty, any]) -> dict:

    def flatten(data: dict[ElementProperty, any], prefixList: list[str]) -> dict:
        fullPrefixList = prefixList + [data[ElementProperty.TAG]]
        prefix = "_".join(fullPrefixList)

        retval = {prefix: data[ElementProperty.TEXT]}
        for attrName, attrValue in data[ElementProperty.ATTR].items():
            retval[f"{prefix}_{attrName}"] = attrValue

        for child in data[ElementProperty.CHLD]:
            retval |= flatten(child, fullPrefixList)

        return retval

    return flatten(data, [])
 
def parse(inputPath: Path, dataParser: callable = _basicParse) -> list[dict]:
    return next(parseChunks(inputPath, 0, dataParser))

def parseChunks(inputPath: Path, entriesPerChunk: int, dataParser: callable = _basicParse) -> Generator[list[dict], None, None]:
    entries = max(entriesPerChunk, 0)

    context = ET.iterparse(inputPath, events=("start", "end"))
    _, root = next(context)
    _, mainElement = next(context)

    currentElement = ElementContainer(mainElement)
    mainTag = currentElement.tag

    data = []
    for event, element in context:
        if event == "start":
            nextElement = ElementContainer(element, currentElement)

            if currentElement is not None:
                currentElement.addChild(nextElement)
            
            currentElement = nextElement

        elif event == "end":
            if element == root:
                break

            if currentElement.tag == mainTag:
                data.append(dataParser(currentElement.getData()))
                currentElement = None
                
            else:
                currentElement = currentElement.parent

            element.clear()
            root.clear()

        if data and len(data) == entries:
            yield data
            data.clear()

    yield data
