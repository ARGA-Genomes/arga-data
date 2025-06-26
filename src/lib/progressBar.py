class ProgressBar:
    def __init__(self, totalTasks: int, length: int = 50, callsPerUpdate: int = 1, processName: str = "Progress", decimalPlaces: int = 2):
        self._totalTasks = totalTasks
        self._length = length
        self._callsPerUpdate = callsPerUpdate
        self._processName = processName
        self._decimalPlaces = decimalPlaces

        self._atTask = 0
        self._loading = "-\\|/"

    def _getBar(self, taskNum: int) -> str:
        completion = taskNum / self._totalTasks
        completedLength = min(int(self.barLength * completion), self.barLength)
        percentage = f"{min(100, completion*100):.0{self._decimalPlaces}f}%"
        percentageLength = len(percentage)
        percentagePos = (self.barLength - percentageLength + 1) // 2
        secondHalfStart = percentageLength + percentagePos
        return f"[{min(completedLength, percentagePos) * '='}{max(percentagePos - completedLength, 0) * '-'}{percentage}{max(completedLength - secondHalfStart, 0) * '='}{((self.barLength - secondHalfStart) - max(completedLength - secondHalfStart, 0)) * '-'}]"

    def _render(self, extraInfo: str) -> int:
        output = f"> {self.processName}{' - ' if extraInfo else ''}{extraInfo} ({self._loading[self._atTask % len(self._loading)]}): {self._getBar()}"
        print(output, end="\r")
        return len(output)

    def update(self, extraInfo: str = "") -> int:
        if self._atTask == self._totalTasks:
            return 0
        
        self._atTask += 1
        if self._atTask % self._callsPerUpdate == 0 or self._atTask == self._totalTasks:
            return self._render(extraInfo)

        return 0
