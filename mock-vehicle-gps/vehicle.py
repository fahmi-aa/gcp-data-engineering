import math
from dto import Point
import random
from datetime import datetime, timedelta


class Vehicle:
    id: int
    type: str
    location: Point
    current_orientation: float
    speed_rate: float

    def __init__(self, vehicle_id: int, vehicle_type: str, init_point: Point, speed_rate: float) -> None:
        self.id = vehicle_id
        self.type = vehicle_type
        self.location = init_point
        self.current_orientation = random.random() * math.pi * 2 - math.pi
        self.speed_rate = speed_rate

    def get_new_data(self) -> [datetime, Point]:
        delay = random.randint(0, 30)
        return datetime.now() - timedelta(seconds=delay), self._get_new_location()

    def _get_new_location(self) -> Point:
        self.current_orientation += self._get_turn_radiant()
        displacement = self._get_displacement()
        displacement_point = Point(
            x=displacement * math.sin(self.current_orientation),
            y=displacement * math.cos(self.current_orientation)
        )

        self.location += displacement_point
        return self.location

    def _get_turn_radiant(self) -> float:
        TURN_RANGE = 45
        delta_degree = (random.random() * TURN_RANGE) - (TURN_RANGE / 2)
        return delta_degree / 180 * math.pi

    def _get_displacement(self) -> float:
        return random.random() * self.speed_rate
