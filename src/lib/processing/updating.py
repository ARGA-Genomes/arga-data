from datetime import datetime, timedelta, date
from abc import ABC

class Update(ABC):
    def __init__(self, properties: dict):
        raise NotImplementedError

    def updateReady(self, lastUpdate: datetime) -> bool:
        raise NotImplementedError
    
class DailyUpdate(Update):
    def __init__(self, properties: dict):
        self.repeat = properties.get("repeat", 3)

    def updateReady(self, lastUpdate: datetime) -> bool:
        return (lastUpdate.date() + timedelta(days=self.repeat)) < datetime.now()
    

class WeeklyUpdate(Update):
    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday"
    ]

    def __init__(self, properties: dict):
        self.repeat = properties.get("repeat", 2)
        self.day = properties.get("day", "sunday")
        self.dayInt = self.days.index(self.day)

    def updateReady(self, lastUpdate: datetime) -> bool:
        today = datetime.today()
        delta = today - lastUpdate

        return (delta.days > ((7 * (self.repeat - 1)) + 1)) and (today.weekday() == self.dayInt)
    
class MonthlyUpdate(Update):
    def __init__(self, properties: dict):
        self.repeat = properties.get("repeat", 1)
        self.date = properties.get("date", 1)

    def updateReady(self, lastUpdate: datetime) -> date:
        today = datetime.today()
        delta = today - lastUpdate

        return (delta.days > (27 * self.repeat)) and (today.day == self.date)
