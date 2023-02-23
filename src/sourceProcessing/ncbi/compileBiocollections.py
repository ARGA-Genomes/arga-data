import lib.config as cfg
import pandas as pd

def splitLine(line, endingDivider=True):
    cleanLine = line.rstrip('\n').rstrip()
    if endingDivider:
        cleanLine = cleanLine.rstrip('|')
    return [element.strip() for element in cleanLine.split('|')]

def process(outputFile):
    biocolls = cfg.folderPaths.data / "ncbi" / "biocollections"
    collCodes = biocolls / "Collection_codes.txt"
    instCodes = biocolls /"Institution_codes.txt"
    uInstCodes = biocolls / "Unique_institution_codes.txt"

    for ref, file in enumerate((collCodes, instCodes, uInstCodes)):
        data = []

        with open(file) as fp:
            line = fp.readline()
            headers = splitLine(line)
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
    output.to_csv(biocolls / outputFile, index=False)

if __name__ == '__main__':
    process("biocollections.csv")