from typing import List
from connector import Connector
from payload import Payload
from vehicle import Vehicle

class TestConnector(Connector):
    def __init__(self) -> None:
        pass

    def send(self, payload: Payload) -> None:
        print(payload)