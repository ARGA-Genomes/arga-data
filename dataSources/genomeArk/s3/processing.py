from pathlib import Path
from lib.crawler import Crawler

def collect(outputFolder: Path):
    crawler = Crawler(outputFolder)
    crawler.run("https://s3.amazonaws.com/genomeark/index.html?prefix=species/", ignoreProgress=True)
