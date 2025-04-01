from lib.crawler import Crawler
import pandas as pd
from pathlib import Path

def build(outputFilePath: Path) -> None:
    url = "http://download.cncb.ac.cn/gsa/"
    regexMatch = ".*\\.gz"

    crawler = Crawler(outputFilePath.parent, url, regexMatch)
    files, leftoverFolders = crawler.crawl()

    if leftoverFolders:
        print("Exited early, writing file/folder progress to file")

        with open(outputFilePath.parent / "files.txt", 'w') as fp:
            fp.write("\n".join(files))

        with open(outputFilePath.parent / "folders.txt", 'w') as fp:
            fp.write("\n".join(leftoverFolders))

        exit()

    data = []
    prefixes = {
        "PRJCA": "project_accession",
        "SAMC": "sample_accession",
        "CRX": "experiment_accession",
        "CRA": "experiment_accession",
        "CRR": "run_accession"
    }

    for file in files:
        path = file.strip(url)
        info = {}
        for item in path.split("/"):
            if item.endswith(".gz"): # Reached file name, no new column
                break

            for prefix, column in prefixes.items():
                if item.startswith(prefix):
                    info[column] = item
                    break

        info |= {"url": file}
        data.append(info)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)
