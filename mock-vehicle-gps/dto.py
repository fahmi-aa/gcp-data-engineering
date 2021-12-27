from dataclasses import dataclass
import datetime as dt

@dataclass
class Point:
    x: float
    y: float

    def __add__(self, other):
        return Point(
            x=self.x + other.x,
            y=self.y + other.y
        )

    def __repr__(self) -> str:
        return f"Point(x={self.x}, y={self.y})"

@dataclass
class Payload:
    timestamp: dt.datetime
    id: int
    location: Point

    def toJson(self):
        return {
            "timestamp": int(self.timestamp.timestamp()),
            "id": self.id,
            "x": self.location.x,
            "y": self.location.y
        }

    def __repr__(self) -> str:
        return f"Payload(timestamp={self.timestamp}, id={self.id}, type={self.type}, location={self.location})"