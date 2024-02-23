import logging
import lib.config as cfg
from pathlib import Path
from datetime import datetime
import sys

class SystemLogger(logging.Logger):
    def __init__(self):
        super().__init__("processing", logging.DEBUG)

        # Log file information
        logFolder: Path = cfg.Folders.logs
        logFileName = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        logFilePath = logFolder / f"{logFileName}.log"

        # Configure logger
        self.setLevel(logging.DEBUG)

        # Setup handlers
        # formatter = logging.Formatter("[%(asctime)s] %(module)s - %(levelname)s: %(message)s", "%H:%M:%S")
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

        fileHandler = logging.FileHandler(filename=logFilePath)
        fileHandler.setFormatter(formatter)
        fileHandler.setLevel(logging.INFO)
        self.addHandler(fileHandler)

        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        self.addHandler(streamHandler)

        self.info("Logger initialised")

Logger = SystemLogger()