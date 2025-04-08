from pathlib import Path
import requests
import lib.downloading as dl

def retrieve(outputFilePath: Path):
    nucResponse = requests.get("https://www.ncbi.nlm.nih.gov/nuccore/?term=%22+sp.%22+AND+country%3DAustralia+NOT+viruses+NOT+bacteria+NOT+archaea")
    phid = nucResponse.headers.get("ncbi-phid").split(".")[0]
    nucFile = outputFilePath.parent / "sequence.fasta.xml"

    if not nucFile.exists():
        dl.download(f"https://www.ncbi.nlm.nih.gov/sviewer/viewer.cgi?tool=portal&save=file&log$=seqview&db=nuccore&report=fasta_xml&query_key=1&filter=all&extrafeat=undefined&ncbi_phid={phid}", nucFile)

    

# "downloading": [
#         {
#             "url": ",
#             "name": "sequence.fasta.xml"
#         },
#         {
#             "url": "https://www.ncbi.nlm.nih.gov/portal/utils/file_backend.cgi?Db=biosample&HistoryId=MCID_67f3130652b4f418ca522321&QueryKey=2&Sort=WITHDATA&Filter=all&CompleteResultCount=1486&Mode=file&View=fullxml&p$l=Email&portalSnapshot=%2Fprojects%2FBioSample%2Fbiosample%401.38&BaseUrl=&PortName=live&RootTag=BioSampleSet&FileName=&ContentType=xml",
#             "name": "biosample_result.xml"
#         }
#     ],
