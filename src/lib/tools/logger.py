import logging
import lib.config as cfg
from pathlib import Path
from datetime import datetime
import sys

def createLogger(logToConsole: bool = True, logLevel: str = "debug") -> logging.Logger:
    logLevelLookup = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    level = logLevelLookup.get(logLevel, None)
    if level is None:
        raise Exception(f"Invalid logging level '{logLevel}', please adjust to one of [{', '.join(logLevelLookup.keys())}] in config.toml")

    logFolder: Path = cfg.Folders.logs
    logFileName = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logFilePath = logFolder / f"{logFileName}.log"

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

    logger = logging.getLogger()
    logger.setLevel(level)

    fileHandler = logging.FileHandler(filename=logFilePath)
    fileHandler.setFormatter(formatter)
    fileHandler.setLevel(logging.INFO)
    logger.addHandler(fileHandler)

    if logToConsole:
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)
    
    return logger
