from pathlib import Path
import pandas as pd

def splitLine(line: str, endingDivider: bool = True) -> list[str]:
    cleanLine = line.rstrip('\n').rstrip()
    if endingDivider:
        cleanLine = cleanLine.rstrip('|')
    return [element.strip() for element in cleanLine.split('|')]

def compileBiocollections(fileDir: Path, outputFilePath: Path) -> None:
    collCodes = fileDir / "Collection_codes.txt"
    instCodes = fileDir /"Institution_codes.txt"
    uInstCodes = fileDir / "Unique_institution_codes.txt"

    for ref, filePath in enumerate((collCodes, instCodes, uInstCodes)):
        data = []

        with open(filePath, encoding='utf-8') as fp:
            line = fp.readline()
            headers = splitLine(line)
            line = fp.readline() # Blank line 2 in every file, call again
            line = fp.readline()
            while line:
                data.append(splitLine(line, True))
                line = fp.readline()

        # cull extra data that doesn't map to a header
        df = pd.DataFrame([line[:len(headers)] for line in data], columns=headers) 

        if ref == 0:
            output = df.copy()
        else:
            output = pd.merge(output, df, 'left')

    output.dropna(how='all', axis=1, inplace=True)
    output.to_csv(outputFilePath, index=False)