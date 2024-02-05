from enum import Enum
import lib.config as cfg
import json
from pathlib import Path
from datetime import datetime, timedelta, date, time

class Property(Enum):
    UPDATE_TYPE     = "updateType"
    UPDATE_VALUE    = "updateValue"
    REPEAT_INTERVAL = "repeatInterval"
    TIME            = "time"
    METHOD          = "method"
    SCRIPT          = "script"

class UpdateType(Enum):
    DAILY   = "daily"
    WEEKLY  = "weekly"
    MONTHLY = "monthly"

class UpdateMethod(Enum):
    PARTIAL = "partial"
    FULL    = "full"

class Updater:
    def __init__(self, location: str, name: str, properties: dict):
        self.location = location
        self.name = name
        self.properties = properties
        self._loadProperties()

    def __repr__(self) -> str:
        return f"{self.location}-{self.name}:\n  Properties: {self.properties}"
    
    def getTimeTilUpdate(self) -> int:
        updateTime = datetime.combine(self._getNextUpdate(), self.time)
        interval = (updateTime - datetime.now()).total_seconds()
        return int(interval) if interval > 0 else 0

    def _getProperty(self, property: Property, acceptedValues: list = []) -> (str | int):
        value = self.properties.get(property.value, None)

        if value is None:
            raise Exception(f"Property '{property.value}' not provided") from AttributeError
        
        if acceptedValues and value not in acceptedValues:
            raise Exception(f"Invalid '{property.value}' value: {value}") from AttributeError
        
        return value

    def _loadProperties(self) -> None:
        self.repeatInterval = self._getProperty(Property.REPEAT_INTERVAL)
        self.time = time(hour=self._getProperty(Property.TIME, list(range(1, 25))))
        self.method = self._getProperty(Property.METHOD, UpdateMethod._value2member_map_.keys())
        self.script = self._getProperty(Property.SCRIPT) if self.method == UpdateMethod.PARTIAL else None

    def _getLastUpdate(self) -> (datetime | None):
        lastUpdateFile: Path = cfg.folders.datasources / self.location / self.name / "lastUpdates.json"
        if not lastUpdateFile.exists():
            return None
        
        with open(lastUpdateFile) as fp:
            lastUpdate = json.load(fp)

        lastDownloaded = lastUpdate.get("downloaded", None)
        if lastDownloaded is None:
            return None
        
        return datetime.fromisoformat(lastDownloaded)

    def _getNextUpdate(self) -> date:
        raise NotImplementedError

class DailyUpdater(Updater):
    def _getNextUpdate(self) -> date:
        lastUpdate = self._getLastUpdate()
        if lastUpdate is None:
            return datetime.today().date()

        return lastUpdate.date() + timedelta(days=self.repeatInterval)

class WeeklyUpdater(Updater):
    days = [
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday"
    ]

    def _loadProperties(self) -> None:
        super()._loadProperties()

        day = self._getProperty(Property.UPDATE_VALUE)
        try:
            self.updateValue = self.days.index(day)
        except ValueError:
            raise Exception(f"Invalid day: {day}")

    def _getNextUpdate(self) -> date:
        lastUpdate = self._getLastUpdate()
        if lastUpdate is None:
            return datetime.today().date()
        
        today = datetime.today()
        interval = 7 * self.repeatInterval
        daysFromUpdate = (self.updateValue - today.weekday()) % interval
        if daysFromUpdate == 0 and lastUpdate.date() == today:
            daysFromUpdate = interval

        return (today + timedelta(days=daysFromUpdate)).date()

class MonthlyUpdater(Updater):
    def _loadProperties(self) -> None:
        super()._loadProperties()

        self.updateValue = self._getProperty(Property.UPDATE_VALUE)

    def _getNextUpdate(self) -> date:
        lastUpdate = self._getLastUpdate()
        if lastUpdate is None:
            return datetime.today().date()
        
        today = datetime.today()

        dayOffset = self.updateValue - today.day
        if dayOffset > 0:
            self.repeatInterval -= 1

        nextMonth = (today.month + self.repeatInterval) % 12
        nextYear = today.year + 1 if nextMonth < today.month else today.year
        return date(year=nextYear, month=nextMonth, day=self.updateValue)

def createUpdater(location: str, name: str, properties: dict) -> Updater:
    updaters: dict[UpdateType, Updater] = {
        UpdateType.DAILY: DailyUpdater,
        UpdateType.WEEKLY: WeeklyUpdater,
        UpdateType.MONTHLY: MonthlyUpdater
    }

    updateType = properties.get(Property.UPDATE_TYPE.value, None)
    if updateType is None:
        raise Exception(f"Please provide an update type.")

    updater = updaters.get(UpdateType(updateType), None)
    if updater is None:
        raise Exception(f"Unknown update type: {updateType}")
    
    return updater(location, name, properties)
