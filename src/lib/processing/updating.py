from datetime import datetime, timedelta, date

class _Update:
    def __init__(self, properties: dict):
        raise NotImplementedError

    def updateReady(self, lastUpdate: datetime) -> bool:
        raise NotImplementedError

class _DailyUpdate(_Update):
    def __init__(self, properties: dict):
        self.repeat = properties.get("repeat", 3)

    def updateReady(self, lastUpdate: datetime) -> bool:
        return (lastUpdate.date() + timedelta(days=self.repeat)) < datetime.now()
    
class _WeeklyUpdate(_Update):
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
    
class _MonthlyUpdate(_Update):
    def __init__(self, properties: dict):
        self.repeat = properties.get("repeat", 1)
        self.date = properties.get("date", 1)

    def updateReady(self, lastUpdate: datetime) -> date:
        today = datetime.today()
        delta = today - lastUpdate

        return (delta.days > (27 * self.repeat)) and (today.day == self.date)

class UpdateManager:
    updaters = {
        "daily": _DailyUpdate,
        "weekly": _WeeklyUpdate,
        "monthly": _MonthlyUpdate
    }

    def __init__(self, updateConfig: dict):
        self.updateConfig = updateConfig
        updaterType = updateConfig.get("type", "weekly")

        if updaterType not in self.updaters:
            raise Exception(f"Unknown update type: {updaterType}")

        self.update: _Update = self.updaters[updaterType](updateConfig)
        
    def isUpdateReady(self, lastUpdate: datetime | None) -> bool:
        if lastUpdate is None:
            return True
        
        return self.update.updateReady(lastUpdate)
