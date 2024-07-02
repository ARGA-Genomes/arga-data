class ProgressBar:
    def __init__(self, barLength: int, processName: str = "Progress"):
        self.barLength = barLength
        self.processName = processName
        self._loading = "-\\|/"
        self._pos = 0

    def render(self, completion: float) -> None:
        length = int(completion * self.barLength)
        print(f"> {self.processName} ({self._loading[self._pos]}): [{length * '='}{(self.barLength - length) * '-'}]", end="\r")
        self._pos = (self._pos + 1) % 4

        if length == self.barLength: # Completed:
            print()
    