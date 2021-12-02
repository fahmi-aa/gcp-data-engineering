from typing import List
from dto import Point
from vehicle import Vehicle
from fleet import Fleet
from connector import PubSubConnector

vehicles: List[Vehicle] = []

for i in range(10):
    vehicles.append(Vehicle(i, "Dump Truck", Point(0,0), 200))

fleet = Fleet(vehicles, PubSubConnector("de-porto", "equipment-gps"))

for i in range(20):
    fleet.send_all()
