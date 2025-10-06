from typing import TypeVar, Generic

from arclet.entari import logger as log_m
from collections import UserDict, defaultdict

logger = log_m.log.wrapper("[Database]")

K = TypeVar("K")
V = TypeVar("V")


class CountSetitemDict(UserDict[K, V], Generic[K, V]):
    def __init__(self):
        super().__init__()
        self.counters = defaultdict(lambda: 0)

    def __setitem__(self, key: K, value: V):
        self.counters[key] += 1
        super().__setitem__(key, value)

    def get_count(self, key: K):
        return self.counters[key]

    def reset_count(self, key: K):
        self.counters[key] = 0
