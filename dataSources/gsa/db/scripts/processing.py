from lib.crawler import Crawler
import pandas as pd
from pathlib import Path
import lib.downloading as dl

def build(outputFilePath: Path) -> None:
    url = "http://download.cncb.ac.cn/gsa/"
    regexMatch = ".*\\.gz"

    crawler = Crawler(outputFilePath.parent)
    crawler.run(url, regexMatch)

    data = []
    prefixes = {
        "PRJCA": "project_accession",
        "SAMC": "sample_accession",
        "CRX": "experiment_accession",
        "CRA": "experiment_accession",
        "CRR": "run_accession"
    }

    print(crawler.getFileURLs())
    # for filesToDownload in crawler.getFileURLs():
    #     dl.download()
    #     path = file.strip(url)
    #     info = {}
    #     for item in path.split("/"):
    #         if item.endswith(".gz"): # Reached file name, no new column
    #             break

    #         for prefix, column in prefixes.items():
    #             if item.startswith(prefix):
    #                 info[column] = item
    #                 break

    #     info |= {"url": file}
    #     data.append(info)

    # df = pd.DataFrame.from_records(data)
    # df.to_csv(outputFilePath, index=False)
