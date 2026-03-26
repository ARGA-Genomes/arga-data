import logging
from lib.settings import Settings
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
    settings = Settings()

    level = logLevelLookup.get(settings.Logging.LOG_LEVEL, None)
    if level is None:
        raise Exception(f"Invalid logging level '{level}', please adjust to one of [{', '.join(logLevelLookup.keys())}] in config.toml")

    logger.setLevel(level)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

    settings.logsDir.mkdir(parents=True, exist_ok=True)

    logFileName = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logFilePath = settings.logsDir / f"{logFileName}.log"

    fileHandler = logging.FileHandler(filename=logFilePath)
    fileHandler.setFormatter(formatter)
    fileHandler.setLevel(logging.INFO)
    logger.addHandler(fileHandler)

    if settings.Logging.LOG_TO_CONSOLE:
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

    return logger

createLogger()
