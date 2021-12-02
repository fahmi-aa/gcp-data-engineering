from abc import ABC, abstractmethod
from typing import List
from google.cloud.pubsub import PublisherClient
import json

from dto import Payload
from vehicle import Vehicle


class Connector(ABC):
    vehicles: List[Vehicle]

    @abstractmethod
    def send(self, payload: Payload) -> None:
        pass


class PubSubConnector(Connector):
    def __init__(self, project_id: str, topic_id: str) -> None:
        self.client = PublisherClient()
        self.topic = self.client.topic_path(project_id, topic_id)

    def send(self, payload: Payload) -> None:
        payload = json.dumps(payload.toJson()).encode("utf-8")
        future = self.client.publish(self.topic, payload)

        future.result(timeout=10)

class TestConnector(Connector):
    def __init__(self) -> None:
        pass

    def send(self, payload: Payload) -> None:
        print(payload)
