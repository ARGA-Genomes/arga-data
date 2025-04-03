from xml.etree import cElementTree as ET
from pathlib import Path
from typing import Generator
import concurrent.futures as cf
from lib.bigFileWriter import BigFileWriter
import pandas as pd

class ElementContainer:
    def __init__(self, element: ET.Element, parent: 'ElementContainer' = None):
        self.tag = self._cleanText(element.tag)
        self.text = self._cleanText(element.text)
        self.attributes = element.attrib.copy()

        self.parent = parent
        self.children: list[ElementContainer] = []
    
    def addChild(self, container: 'ElementContainer') -> None:
        self.children.append(container)
    
    def _cleanText(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        
        return text.translate(text.maketrans("\t\n\r", "   ")).strip()

def flattenElement(element: ElementContainer) -> dict[str, any]:

    def flatten(e: ElementContainer, prefixList: list[str]) -> dict:
        fullPrefixList = prefixList + [e.tag]
        prefix = "_".join(fullPrefixList)

        retval = {prefix: e.text}
        for attrName, attrValue in e.attributes.items():
            retval[f"{prefix}_{attrName}"] = attrValue

        for child in e.children:
            retval |= flatten(child, fullPrefixList)

        return retval

    return flatten(element, [])

def xmlGenerator(inputPath: Path) -> Generator[ElementContainer, None, None]:
    context = ET.iterparse(inputPath, events=("start", "end"))
    _, root = next(context)
    _, mainElement = next(context)

    currentElement = ElementContainer(mainElement)
    mainTag = currentElement.tag

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
                yield currentElement
                currentElement = None
            else:
                currentElement = currentElement.parent

            element.clear()
            root.clear()

def basicXMLProcessor(inputPath: Path, outputPath: Path, entriesPerSection: int = 0) -> None:
    iterator = xmlGenerator(inputPath)
    writer = BigFileWriter(outputPath, "xmlChunks")
    records = []

    for idx, element in enumerate(iterator, start=1):
        print(f"At record: {idx}", end="\r")
        records.append(flattenElement(element))

        if len(records) == entriesPerSection:
            writer.writeDF(pd.DataFrame.from_records(records))
            records.clear()

    if records:
        writer.writeDF(pd.DataFrame.from_records(records))
        records.clear()
        
    print()
    writer.oneFile()
