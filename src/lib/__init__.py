import logging
import lib.settings as settings
from pathlib import Path
from datetime import datetime
import sys

logLevelLookup = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}

def createLogger() -> logging.Logger:
    logger = logging.getLogger()

    globalSettings = settings.load()
    logToConsole = globalSettings.logging.logToConsole
    logLevel = globalSettings.logging.logLevel

    level = logLevelLookup.get(logLevel, None)
    if level is None:
        raise Exception(f"Invalid logging level '{logLevel}', please adjust to one of [{', '.join(logLevelLookup.keys())}] in config.toml")

    logger.setLevel(level)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

    logFolder: Path = globalSettings.folders.logs
    logFolder.mkdir(parents=True, exist_ok=True)

    logFileName = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logFilePath = logFolder / f"{logFileName}.log"

    fileHandler = logging.FileHandler(filename=logFilePath)
    fileHandler.setFormatter(formatter)
    fileHandler.setLevel(logging.INFO)
    logger.addHandler(fileHandler)

    if logToConsole:
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

    return logger

createLogger()
