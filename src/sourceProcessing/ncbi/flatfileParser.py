from pathlib import Path
from enum import Enum

class Section(Enum):
    INFO = "INFO"
    REFERENCE = "REFERENCE"
    COMMENT = "COMMENT"
    FEATURES = "FEATURES"
    ORIGIN = "ORIGIN"

class FlatFileParser:
    def __init__(self):
        self.seqBaseURL = "https://ftp.ncbi.nlm.nih.gov/genbank/"
        self.genbankBaseURL = "https://www.ncbi.nlm.nih.gov/nuccore/"
        self.fastaSuffix = "?report=fasta&format=text"

    def parse(self, filePath: Path, verbose: bool = False) -> list[dict]:
        with open(filePath) as fp:
            try:
                data = fp.read()
            except UnicodeDecodeError:
                print(f"Failed to read file: {filePath}")
                return []

        # Cut off header of file
        firstLocusPos = data.find("LOCUS")
        header = data[:firstLocusPos]
        data = data[firstLocusPos:]

        records = []
        for idx, entry in enumerate(data.split("//\n")[:-1], start=1): # Split into separate loci, exclude empty entry after last
            if verbose:
                print(f"Parsing entry: {idx}", end="\r")

            entryData = self.parseEntry(entry)

            # Attach seq file path and fasta file
            # output["seq_file"] = f"{self.seqBaseURL}{filePath.name}.gz"
            version = entryData.get("version", "")
            if version:
                entryData["genbank_url"] = f"{self.genbankBaseURL}{version}"
                entryData["fasta_url"] = f"{self.genbankBaseURL}{version}{self.fastaSuffix}"

            # Add specimen field
            for value in (entryData.get("specimen_voucher", None), entryData.get("isolate", None), f"NCBI_{entryData['accession']}_specimen"):
                if value is not None:
                    entryData["specimen"] = value
                    break

            records.append(entryData)

        if verbose:
            print()

        return records
    
    def parseEntry(self, entryBlock: str) -> dict:
        sections = self.getSections(entryBlock, allowDigits=False)

        entryData = {}
        for section in sections:
            heading, data = section.split(" ", 1)
            # headings = ["LOCUS", "DEFINITION", "ACCESSION", "VERSION", "DBLINK", "KEYWORDS", "SOURCE", "ORGANISM"]

            if heading == "LOCUS":
                parameters = ["locus", "basepairs", "", "type", "shape", "seq_type", "date"]
                entryData |= {param: value.strip() for param, value in zip(parameters, data.split()) if param}

            elif heading in ("DEFINITION", "ACCESSION", "VERSION", "COMMENT"):
                entryData[heading.lower()] = self.flattenBlock(data)

            elif heading == "DBLINK":
                dbs = {}
                key = "unknown_db"

                for line in data.split("\n"):
                    if not line.strip():
                        continue

                    if ":" in line: # Line has a new key in it
                        key, line = line.split(":")
                        key = key.strip().lower()
                        dbs[key] = []

                    if key is "unknown_db": # Found a line before a db name
                        dbs[key] = [] # Create a list for values with no db name
                        
                    dbs[key].extend([element.strip() for element in line.split(",")])

                entryData |= dbs

            elif heading == "KEYWORDS":
                if data.strip() == ".":
                    entryData["keywords"] = ""
                else:
                    entryData["keywords"] = self.flattenBlock(data)

            elif heading == "SOURCE":
                source, leftover = self.getSections(data, 2)
                organism, higherClassification = leftover.split("\n", 1)

                entryData["source"] = source.strip()
                entryData["organism"] = organism.split()[1].strip()
                entryData["higher_classification"] = self.flattenBlock(higherClassification)

            elif heading == "REFERENCE":
                reference = {}
                refInfo = self.getSections(data, 2)
                basesInfo = refInfo.pop(0)
                basesInfo = basesInfo.strip(" \n").split(" ", 1)

                if len(basesInfo) == 1: # Only reference number
                    reference["bases"] = []
                elif "bases" not in basesInfo[1]: # Bases not specified
                    reference["bases"] = []
                else:
                    baseRanges = basesInfo[1].strip().lstrip("(bases").rstrip(")")
                    bases = []
                    for baseRange in baseRanges.split(";"):
                        if not baseRange:
                            continue

                        try:
                            basesFrom, basesTo = baseRange.split("to")
                            bases.append(f"({basesFrom.strip()}) to {basesTo.strip()}")
                        except:
                            print(f"{data}, ERROR: {baseRange}")

                    reference["bases"] = bases

                for section in refInfo:
                    sectionName, sectionData = section.strip().split(" ", 1)
                    reference[sectionName.lower()] = self.flattenBlock(sectionData)

                if "references" not in entryData:
                    entryData["references"] = []

                entryData["references"].append(reference)

            elif heading == "FEATURES":
                featureBlocks = self.getSections(data, 5)
                genes = {}
                otherProperties = {}

                for block in featureBlocks[1:]:
                    sectionHeader, sectionData = block.lstrip().split(" ", 1)

                    propertyList = self.flattenBlock(sectionData, "//").split("///")
                    properties = {"bp_range": propertyList[0]}
                    for property in propertyList[1:]: # base pair range is first property in list
                        splitProperty = property.split("=")
                        if len(splitProperty) == 2:
                            key, value = splitProperty
                        else:
                            key = splitProperty[0]
                            value = key

                        properties[key] = value.strip('"')

                    if sectionHeader == "source":
                        value = properties.pop("organism", None)
                        if value is not None:
                            properties["features_organism"] = value

                        entryData |= properties

                    else:
                        gene = properties.get("gene", None)
                        if gene is not None:  # If section is associated with a gene
                            properties.pop("gene")
                            if gene not in genes:
                                genes[gene] = {}

                            if sectionHeader not in genes[gene]:
                                genes[gene][sectionHeader] = []

                            genes[gene][sectionHeader].append(properties)

                        else: # Not associated with a gene
                            if sectionHeader not in otherProperties:
                                otherProperties[sectionHeader] = []

                            otherProperties[sectionHeader].append(properties)
                
                entryData["genes"] = genes
                entryData["other_properties"] = otherProperties

            elif heading == "ORIGIN":
                pass # Skip adding of origin to output

            else:
                print(f"Unhandled heading: {heading}")

        return entryData

    def getSections(self, textBlock: str, whitespace: int = 0, allowDigits=True) -> list[str]:
        sections = []
        sectionStart = 0
        searchPos = 0
        textBlock = textBlock.rstrip("\n") # Make sure block doesn't end with newlines

        while True:
            nextNewlinePos = textBlock.find("\n", searchPos)
            nextPlus1 = nextNewlinePos + 1

            if nextNewlinePos < 0: # No next new line
                sections.append(textBlock[sectionStart:])
                break

            if len(textBlock[nextPlus1:nextPlus1+whitespace+1].strip()) == 0: # No header on next line
                searchPos = nextPlus1
                continue

            if not allowDigits and textBlock[nextPlus1+whitespace+1].isdigit(): # Check for digit leading next header
                searchPos = nextPlus1
                continue

            sections.append(textBlock[sectionStart:nextNewlinePos])
            sectionStart = searchPos = nextPlus1

        return sections
    
    def flattenBlock(self, textBlock: str, joiner: str = " ") -> str:
        return joiner.join(line.strip() for line in textBlock.split("\n") if line)
