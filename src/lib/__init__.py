import lib.logger as lg
import lib.config as cfg

lg.createLogger(logToConsole=cfg.Settings.logToConsole, logLevel=cfg.Settings.logLevel)