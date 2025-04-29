import logging
import lib.config as cfg
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

def createLogger(logToConsole: bool = True, logLevel: str = "debug") -> logging.Logger:
    logger = logging.getLogger()
    
    level = logLevelLookup.get(logLevel, None)
    if level is None:
        raise Exception(f"Invalid logging level '{logLevel}', please adjust to one of [{', '.join(logLevelLookup.keys())}] in config.toml")

    logger.setLevel(level)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

    logFolder: Path = cfg.Folders.logs
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
