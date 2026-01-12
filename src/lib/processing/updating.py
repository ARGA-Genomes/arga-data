from datetime import datetime, timedelta, date
from abc import ABC
from typing import Any

class Update(ABC):

    _repeatFreq = "repeatFreq"

    def __init__(self, properties: dict):
        self._freq = self._getAndAssert(properties, self._repeatFreq, int)

    def _getAndAssert(self, properties: dict, property: str, type: type) -> Any:
        value = properties.get(property)

        assert value is not None, f"No property '{property}' found."
        assert isinstance(value, type), f"Property '{property}' should be of type '{type}'."

        return value

    def updateReady(self, lastUpdate: datetime) -> bool:
        raise NotImplementedError
    
class DailyUpdate(Update):
    def updateReady(self, lastUpdate: datetime) -> bool:
        return (lastUpdate.date() + timedelta(days=self._freq)) < datetime.now()
    
class WeeklyUpdate(Update):

    _repeatDay = "repeatDay"

    _days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday"
    ]

    def __init__(self, properties: dict):
        super().__init__(properties)

        day = self._getAndAssert(properties, self._repeatDay, str)
        if day not in self._days:
            raise Exception(f"Invalid weekday '{day}'.")
        
        self._day = self._days.index(day)

    def updateReady(self, lastUpdate: datetime) -> bool:
        today = datetime.today()
        delta = today - lastUpdate

        if lastUpdate.date() == today.date(): # Rerun on same date as last update
            return False

        if self._day == today.weekday(): # Weekdays match
            return delta.days > ((7 * (self._freq - 1)) + 1)

        return False
    
class MonthlyUpdate(Update):

    _repeatDate = "repeatDate"

    _monthDays = [
        31,
        28,
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31
    ]

    def __init__(self, properties: dict):
        super().__init__(properties)

        self._date = self._getAndAssert(self._repeatDate, int)

    def updateReady(self, lastUpdate: datetime) -> date:
        today = datetime.today()
        daysThisMonth = self._monthDays[today.month]

        if ((lastUpdate.month + 1) % 12) != today.month: # Confirm we're in a new month
            return False

        if self._date == today.day: # If dates match
            return True

        if self._date > daysThisMonth: # If today is last day of month
            return today.day == daysThisMonth

        return False # New month, not the same date, and date does not exceed length of this month
