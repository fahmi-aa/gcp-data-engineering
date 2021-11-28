from typing import List
from point import Point
from vehicle import Vehicle
from fleet import Fleet
from test_connector import TestConnector

vehicles: List[Vehicle] = []

for i in range(20):
    vehicles.append(Vehicle(i, "Dump Truck", Point(0,0), 200))

fleet = Fleet(vehicles, TestConnector())

for i in range(20):
    fleet.send_all()
