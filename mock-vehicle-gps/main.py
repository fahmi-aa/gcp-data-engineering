import argparse
from datetime import datetime
import time
from typing import List
from dto import Point
from vehicle import Vehicle
from fleet import Fleet
from connector import PubSubConnector

def run(vehicle_num: int, interval: float, project: str, topic_id: str):
    vehicles: List[Vehicle] = []
    for i in range(vehicle_num):
        vehicles.append(Vehicle(i, "Dump Truck", Point(0,0), 200))

    fleet = Fleet(vehicles, PubSubConnector(project, topic_id))

    while True:
        fleet.send_all()
        print(f"{datetime.now().isoformat()[:-7]}: Data sent")

        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--equipment_num",
        type=int,
        default=21,
        help="Number of equipments that has gps"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=15.0,
        help="Interval of equipment sending data to server"
    )
    parser.add_argument(
        "--project",
        type=str,
        default="de-porto",
        help="Project ID"
    )
    parser.add_argument(
        "--topic_id",
        type=str,
        default="equipment-gps",
        help="Pubsub topic ID"
    )
    args, _ = parser.parse_known_args()

    run(args.equipment_num, args.interval, args.project, args.topic_id)
