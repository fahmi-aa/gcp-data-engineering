from abc import ABC, abstractmethod
from typing import List

from payload import Payload
from vehicle import Vehicle


class Connector(ABC):
    vehicles: List[Vehicle]

    @abstractmethod
    def send(self, payload: Payload) -> None:
        pass


