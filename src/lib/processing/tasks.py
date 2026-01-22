from pathlib import Path
import logging
from lib.processing.files import DataFile, StackedFile
from lib.processing.scripts import OutputScript
import lib.downloading as dl
from lib.crawler import Crawler
from lib.converting import Converter
from datetime import date
import lib.processing.parsing as parse

class Task:
    def __init__(self, outputs: list[DataFile] = []):
        self._runMetadata = {}
        self._outputs = outputs

    def getOutputs(self) -> list[DataFile]:
        return self._outputs

    def run(self, overwrite: bool, verbose: bool) -> bool:
        return True
    
    def setAdditionalMetadata(self, metadata: dict) -> None:
        self._runMetadata.update(metadata)

    def getMetadata(self) -> dict:
        return self._runMetadata

class UrlRetrieve(Task):

    _url = "url"
    _name = "name"
    _properties = "properties"

    def __init__(self, workingDir: Path, config: dict, username: str, password: str):
        self.workingDir = workingDir
        self.username = username
        self.password = password

        self.url = config.get(self._url, None)
        if self.url is None:
            raise Exception("No url provided for source") from AttributeError

        fileName = config.get(self._name, None)
        if fileName is None:
            raise Exception("No filename provided to download to") from AttributeError
    
        properties = config.get(self._properties, {})
        self.file = DataFile(self.workingDir / fileName, properties)

        super().__init__([self.file])

    def run(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.path} already exists")
            return False
        
        self.file.delete()
        return dl.download(self.url, self.file.path, verbose=verbose, auth=dl.buildAuth(self.username, self.password))

class CrawlRetrieve(Task):

    _url = "url"
    _regex = "regex"
    _link = "link"
    _maxDepth = "maxDepth"
    _properties = "properties"
    _filenameURLParts = "urlPrefix"

    def __init__(self, workingDir: Path, config: dict, username: str, password: str, overwrite: bool):
        self.workingDir = workingDir
        self.username = username
        self.password = password

        url = config.pop(self._url, None)
        regex = config.pop(self._regex, None)
        link = config.pop(self._link, "")
        maxDepth = config.pop(self._maxDepth, -1)
        filenameURLParts = config.pop(self._filenameURLParts, 1)
        properties = config.pop(self._properties, {})

        crawler = Crawler(self.workingDir, dl.buildAuth(self.username, self.password))
        crawler.run(url, regex, maxDepth, overwrite)
        urlList = crawler.getFileURLs(link)

        self.downloads: list[tuple[str, DataFile]] = []
        for url in urlList:
            fileName = "_".join(url.split("/")[-filenameURLParts:])
            self.downloads.append((url, DataFile(self.workingDir / fileName, properties)))

        super().__init__([downloadFile for _, downloadFile in self.downloads])

    def run(self, overwrite: bool, verbose: bool) -> bool:
        downloadsRun = False
        for downloadURL, downloadFile in self.downloads:
            if not overwrite and downloadFile.exists():
                continue

            downloadFile.delete()
            dl.download(downloadURL, downloadFile.path, auth=dl.buildAuth(self.username, self.password), verbose=verbose)
            downloadsRun = True

        return downloadsRun

class ScriptRunner(Task):

    _parallel = "parallel"
    _modulePath = "path"
    _functionName = "function"
    _inputs = "inputs"
    _args = "args"
    _kwargs = "kwargs"
    _outputs = "outputs"

    def __init__(self, workingDir: Path, config: dict, dirLookup: parse.DirLookup, fileLookup: parse.DataFileLookup):
        parallel = config.pop(self._parallel, False)

        modulePath = config.pop(self._modulePath, "")
        if not modulePath:
            raise Exception("No `path` specified in script config") from AttributeError

        modulePath = parse.parseArg(modulePath, workingDir, dirLookup, fileLookup)

        functionName = config.pop(self._functionName, "")
        if not functionName:
            raise Exception("No `function` specified in script config") from AttributeError

        outputs = config.pop(self._outputs, [])
        if not outputs:
            raise Exception("No `outputs` specified in script config") from AttributeError        

        # Split script if necessary for parallel tasks
        lookups: list[parse.DataFileLookup] = []
        if parallel:
            for input in fileLookup.getFiles(parse.FileSelect.INPUT):
                individualFileLookup = parse.DataFileLookup([input], fileLookup.getFiles(parse.FileSelect.DOWNLOAD), fileLookup.getFiles(parse.FileSelect.PROCESS))
                lookups.append(individualFileLookup)
        else:
            lookups.append(fileLookup)

        # Store script objects for run time
        self.scripts: list[tuple[OutputScript, list, dict]] = []
        allOutputs = []
        for lookup in lookups:

            parsedOutputs = []
            for output in outputs:
                parsed = parse.parseArg(output, workingDir, dirLookup, lookup)

                if isinstance(parsed, Path):
                    parsedOutputs.append(DataFile(workingDir / parsed.name))
                elif isinstance(parsed, DataFile):
                    parsedOutputs.append(parsed.move(workingDir))
                else:
                    parsedOutputs.append(DataFile(workingDir / parsed))

            lookup.extend(parse.FileSelect.OUTPUT, parsedOutputs)

            args = parse.parseList(config.get(self._args, []), workingDir, dirLookup, lookup)
            kwargs = parse.parseDict(config.get(self._kwargs, {}), workingDir, dirLookup, lookup)

            self.scripts.append((OutputScript(modulePath, functionName, parsedOutputs, lookup.getFiles(parse.FileSelect.INPUT), dirLookup.paths()), args, kwargs))
            allOutputs.extend(parsedOutputs)

        super().__init__(allOutputs)

    def run(self, overwrite: bool, verbose: bool) -> bool:
        for script, args, kwargs in self.scripts:
            success, _ = script.run(overwrite, verbose, args, kwargs)
            if not success:
                return False

        return True
