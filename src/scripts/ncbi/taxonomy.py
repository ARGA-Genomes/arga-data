from pathlib import Path
import pandas as pd
from enum import Enum
import logging
from lib.progressBar import ProgressBar

class DumpFile(Enum):
    NODES = "nodes.dmp"
    NAMES = "names.dmp"
    DIVISION = "division.dmp"
    GENETIC_CODES = "gencode.dmp"
    DELETED_NODES = "delnodes.dmp"
    MERGED_NODES = "merged.dmp"
    CITATIONS = "citations.dmp"

headings = {
    DumpFile.NODES: [
        "tax_id",
        "parent_tax_id",
        "rank",
        "embl_code",
        "division_id",
        "inherited_div_flag",
        "genetic_code_id",
        "inherited_GC_flag",
        "mitochondrial_genetic_code_id",
        "inherited_MGC_flag",
        "GenBank_hidden_flag",
        "hidden_subtree_root_flag",
        "comments"
    ],
    DumpFile.NAMES: [
        "tax_id",
        "name_txt",
        "unique_name",
        "name_class"
    ],
    DumpFile.DIVISION: [
        "division_id",
        "division_cde",
        "division_name",
        "comments"
    ],
    DumpFile.GENETIC_CODES: [
        "genetic_code_id",
        "abbreviation",
        "name",
        "cde",
        "starts"
    ],
    DumpFile.DELETED_NODES: [
        "tax_id"
    ],
    DumpFile.MERGED_NODES: [
        "old_tax_id",
        "new_tax_id",
    ],
    DumpFile.CITATIONS: [
        "cit_id",
        "cit_key",
        "pubmed_id",
        "medline_id",
        "url",
        "text",
        "taxid_list"
    ]
}

class Node:
    __slots__ = headings[DumpFile.NODES]

    inheritedAttrs = {
        "inherited_div_flag": "division_id",
        "inherited_GC_flag": "genetic_code_id",
        "inherited_MGC_flag": "mitochondrial_genetic_code_id",
    }

    hiddenAttrs = [
        "GenBank_hidden_flag",
        "hidden_subtree_root_flag"
    ]

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.hiddenAttrs or k in self.inheritedAttrs:
                v = bool(int(v))

            setattr(self, k, v)

    def resolveInherit(self, nodes: dict[int, 'Node']) -> None:
        for flagAttr, valueAttr in self.inheritedAttrs.items():
            if getattr(self, flagAttr):
                parentNode = nodes[self.parent_tax_id]
                parentNode.resolveInherit(nodes)
                setattr(self, flagAttr, False)
                setattr(self, valueAttr, getattr(parentNode, valueAttr))

    def package(self) -> dict:
        return {attr: getattr(self, attr) for attr in self.__slots__ if attr not in self.inheritedAttrs and attr not in self.hiddenAttrs}

def resolveInheritance(data: pd.DataFrame) -> pd.DataFrame:
    nodes: dict[str, Node] = {}
    nodeProgress = ProgressBar(len(data), processName="Creating Nodes")
    for _, row in data.iterrows():
        node = Node(**row.to_dict())
        nodes[node.tax_id] = node
        nodeProgress.update()

    records = []
    recordProgress = ProgressBar(len(nodes), processName="Resolving Inheritance")
    for node in nodes.values():
        node.resolveInherit(nodes)
        records.append(node.package())
        recordProgress.update()

    return pd.DataFrame.from_records(records)

def flattenNames(df: pd.DataFrame) -> pd.DataFrame:
    data = {}
    flattenProgress = ProgressBar(len(df), processName="Flattening Names")
    for _, row in df.iterrows():
        taxID = row["tax_id"]
        text = row["name_txt"]
        nameClass = row["name_class"]

        if taxID not in data:
            data[taxID] = {"tax_id": taxID}

        data[taxID][nameClass] = text
        
        flattenProgress.update()

    return pd.DataFrame.from_dict(data, orient="index")

def parse(dumpFolder: Path, outputFile: Path) -> None:

    def loadDF(dumpFile: DumpFile) -> pd.DataFrame:
        with open(dumpFolder / dumpFile.value) as fp:
            records = [line.strip("\t|\n").split("\t|\t") for line in fp.readlines()]

        return pd.DataFrame.from_records(records, columns=headings[dumpFile])

    df = loadDF(DumpFile.NODES)
    df = resolveInheritance(df)

    # df = df[["tax_id", "parent_tax_id", "rank"]]

    names = loadDF(DumpFile.NAMES)
    names = flattenNames(names)

    df = df.merge(names, "left", on="tax_id")

    divisions = loadDF(DumpFile.DIVISION)
    divisions = divisions.drop(["comments"], axis=1)
    df = df.merge(divisions, "left", on="division_id")

    divisionMap = {
        "INV": "ICZN",
        "BCT": "",
        "MAM": "ICZN",
        "PHG": "",
        "PLN": "ICN",
        "PRI": "ICZN",
        "ROD": "ICZN",
        "SYN": "",
        "UNA": "",
        "VRL": "",
        "VRT": "ICZN",
        "ENV": "ICN",
    }

    df["nomenclatural_code"] = df["division_cde"].apply(lambda x: divisionMap[x])

    def cleanAuthority(authority: any, scientificName: any, synonym: any) -> str:
        authority: str = str(authority).strip()

        for item in (scientificName, synonym):
            itemStr = str(item)

            if authority.startswith(itemStr):
                authority = authority[len(itemStr):].strip()

        if not authority:
            return authority
        
        if authority[0] == "(" and authority[-1] == ")":
            authority = authority[1:-1]

        return authority

    df["authority"] = df.apply(lambda x: cleanAuthority(x["authority"], x["scientific name"], x["synonym"]), axis=1)

    df["taxonomic_status"] = ""
    df["nomenclatural_act"] = "names usage"
    df["ARGA_curated"] = False
    df["present_on_ARGA_backbone"] = False

    df = df.drop(["in-part"], axis=1)
    df = df.rename({col: col.replace(" ", "_") for col in df.columns}, axis=1)
    df.to_csv(outputFile, index=False)
