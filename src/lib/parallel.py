from multiprocessing import Process, Queue
from threading import Thread
from enum import Enum
import logging
from lib.progressBar import ProgressBar

class WorkerType(Enum):
    THREAD = 0
    PROCESS = 1

class _ParallelManager:
    def __init__(self, workerCount: int, workerType: WorkerType):
        self.workerCount = workerCount
        self.workers = []
        self.queue = None

    def run(self, func: callable, variable: list, *args) -> None:
        tasksPerWorker = (len(variable) / self.workerCount).__ceil__()
        self.queue = Queue()

        for workerNum in self.workerCount:
            variableItems = variable[workerNum*tasksPerWorker:(workerNum+1)*tasksPerWorker]
            self._runWorker(func, variableItems, *args)

        progress = ProgressBar(len(variable))

def parallel()