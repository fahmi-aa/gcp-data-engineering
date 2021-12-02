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
            payload = Payload(
                dt.datetime.now(),
                vehicle.id,
                vehicle.type,
                vehicle.get_new_location()
            )
            self.connector.send(payload)