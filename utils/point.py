from dataclasses import dataclass

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
