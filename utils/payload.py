from dataclasses import dataclass
import datetime as dt
from point import Point

@dataclass
class Payload:
    timestamp: dt.datetime
    id: int
    type: str
    location: Point

    def toJson(self):
        return {
            "timestamp": int(self.timestamp.timestamp()),
            "id": self.id,
            "type": self.type,
            "x": self.location.x,
            "y": self.location.y
        }

    def __repr__(self) -> str:
        return f"Payload(timestamp={self.timestamp}, id={self.id}, type={self.type}, location={self.location})"