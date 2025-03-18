defaultBarLength: int = 50
defaultProcessName: str = "Progress"
defaultDecimalPlaces: int = 2

class ProgressBar:
    def __init__(self, barLength: int = defaultBarLength, processName: str = defaultProcessName, decimalPlaces: float = defaultDecimalPlaces):
        self.barLength = barLength
        self.processName = processName
        self.decimalPlaces = decimalPlaces

        self._loading = "-\\|/"
        self._pos = 0

    def _getLoadStage(self) -> str:
        self._pos = (self._pos + 1) % len(self._loading)
        return self._loading[self._pos]
    
    def _getBar(self, completion: int) -> str:
        # completion = atTask / self.taskCount
        length = min(int(self.barLength * completion), self.barLength)
        percentage = f"{min(100, completion*100):.02f}%"
        percentageLength = len(percentage)
        percentagePos = (self.barLength - percentageLength + 1) // 2
        secondHalfStart = percentageLength + percentagePos
        return f"[{min(length, percentagePos) * '='}{max(percentagePos - length, 0) * '-'}{percentage}{max(length - secondHalfStart, 0) * '='}{((self.barLength - secondHalfStart) - max(length - secondHalfStart, 0)) * '-'}]"

    def update(self, completion: float, extraInfo: str = "") -> int:
        output = f"> {self.processName}{' - ' if extraInfo else ''}{extraInfo} ({self._getLoadStage()}): {self._getBar(completion)}"
        print(output, end="\r")
        return len(output)

class UpdatableProgressBar(ProgressBar):
    def __init__(self, taskCount: int, barLength: int = defaultBarLength, processName: str = defaultProcessName, newlineOnComplete: bool = True, decimalPlaces: int = defaultDecimalPlaces):
        self.taskCount = taskCount
        self.newlineOnComplete = newlineOnComplete

        self._completed = False
    
        super().__init__(barLength, processName, decimalPlaces)

    def update(self, atTask: int, extraInfo: str = "") -> int:
        if self._completed:
            return
        
        outputLen = super().update(atTask / self.taskCount, extraInfo)

        if atTask == self.taskCount:
            self._completed = True

            if self.newlineOnComplete:
                print()

        return outputLen

class SteppableProgressBar(UpdatableProgressBar):
    def __init__(self, taskCount: int, barLength: int = defaultBarLength, callsPerUpdate: int = 1, processName: str = defaultProcessName, newlineOnComplete: bool = True, decimalPlaces: int = defaultDecimalPlaces, initialRender: bool = True):
        super().__init__(taskCount, barLength, processName, newlineOnComplete, decimalPlaces)

        self._callsPerUpdate = callsPerUpdate
        self._atTask = 0

        if initialRender:
            self._atTask -= 1
            self.update()

    def update(self, extraInfo: str = "") -> int:
        self._atTask += 1
        if self._atTask % self._callsPerUpdate == 0:
            return super().update(self._atTask, extraInfo)
        return 0
