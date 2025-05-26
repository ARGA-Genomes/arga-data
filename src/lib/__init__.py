import lib.logger as lg
from lib.config import globalConfig as gcfg

lg.createLogger(logToConsole=gcfg.settings.logToConsole, logLevel=gcfg.settings.logLevel)