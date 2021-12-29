import datetime as dt
from typing import List
from connector import Connector
from dto import Payload
from vehicle import Vehicle


class Fleet:
    vehicles: List[Vehicle]
    connector: Connector

    def __init__(self, vehicles: List[Vehicle], connector: Connector) -> None:
        self.vehicles = vehicles
        self.connector = connector

    def add_vehicle(self, vehicle: Vehicle) -> None:
        self.vehicles.append(vehicle)

    def send_all(self):
        for vehicle in self.vehicles:
            time, location = vehicle.get_new_data()
            payload = Payload(
                time,
                vehicle.id,
                location
            )
            self.connector.send(payload.to_json())
