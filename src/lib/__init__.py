import lib.logger as lg
from lib.config import globalConfig as cfg

lg.createLogger(logToConsole=cfg.settings.logToConsole, logLevel=cfg.settings.logLevel)