from dataclasses import dataclass
import datetime as dt
import utm

@dataclass
class Point:
    x: float
    y: float

    def __add__(self, other):
        return Point(
            x=self.x + other.x,
            y=self.y + other.y
        )

    def to_latlon(self):
        x, y = utm.to_latlon(self.x, self.y, 48, "M")
        return Point(x, y)

    def __repr__(self) -> str:
        return f"Point(x={self.x}, y={self.y})"

@dataclass
class Payload:
    timestamp: dt.datetime
    id: int
    location: Point
    fuel: float

    def to_json(self):
        return {
            "timestamp": int(self.timestamp.timestamp()),
            "id": self.id,
            "x": self.location.x,
            "y": self.location.y,
            "fuel_level": self.fuel
        }

    def __repr__(self) -> str:
        return f"Payload(timestamp={self.timestamp}, id={self.id}, location={self.location})"
