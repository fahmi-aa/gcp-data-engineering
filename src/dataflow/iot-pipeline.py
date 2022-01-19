import json
import datetime as dt
import logging as log

import apache_beam as beam
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders.coders import TupleCoder, FloatCoder


class FuelDiff(beam.DoFn):
    STATE = BagStateSpec("fuel", TupleCoder((FloatCoder(), FloatCoder())))

    def process(self, element, state=beam.DoFn.StateParam(STATE)):
        key = element[0]
        location = element[1]["location"]
        fuel_level = element[1]["fuel_level"]
        timestamp = element[1]["timestamp"]

        state_content = [x for x in state.read()]
        log.info(f"state: {state_content}")
        log.info(f"element: key: {key}, timestamp: {timestamp}, fuel_level: {fuel_level}")
        if not state_content:
            state.add((timestamp, fuel_level))
            yield {"id": key,
                   "timestamp": timestamp,
                   "location": location,
                   "fuel_level": fuel_level,
                   "fuel_diff": None}
        else:
            log.info("else called")
            lag_content = state_content[0]
            lag_timestamp = lag_content[0]
            lag_fuel = lag_content[1]
            log.info(f"{lag_content}, {lag_timestamp}, {lag_fuel}")

            state.clear()
            state.add(timestamp, fuel_level)

            output = {"id": key,
                      "timestamp": timestamp,
                      "location": location,
                      "fuel_level": fuel_level,
                      "fuel_diff": fuel_level - lag_fuel}
            if timestamp >= lag_timestamp:
                yield output
            else:
                output["fuel_diff"] = -output["fuel_diff"]
                yield output


def debug(payload):
    log.info(payload)
    return payload


def payload_epoch_to_utc(payload):
    payload["timestamp"] = dt.datetime.utcfromtimestamp(payload["timestamp"]).isoformat()
    return payload


def to_wkt_point(payload):
    payload["location"] = f'POINT({payload["y"]} {payload["x"]})'
    del payload["x"]
    del payload["y"]

    return payload


def int_to_date_year(pval):
    pval["year"] = dt.date(pval["year"], 1, 1)
    return pval


def enrich_payload(payload, equipments):
    id = payload["id"]
    for equipment in equipments:
        if id == equipment["id"]:
            payload["type"] = equipment["type"]
            payload["brand"] = equipment["brand"]
            payload["year"] = equipment["year"]

            break

    log.info(payload)
    yield payload


bq_table = "de-porto:de_porto.iot_log"
options = GoogleCloudOptions(streaming=True, save_main_session=True)

p = beam.Pipeline(options=options)

periodic_side_pipeline = (
        p
        | "periodic" >> PeriodicImpulse(fire_interval=3600, apply_windowing=True)
        | "ApplyGlobalWindow" >> beam.WindowInto(window.GlobalWindows(),
                                                 trigger=trigger.Repeatedly(trigger.AfterProcessingTime(60)),
                                                 accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | "map to read request" >>
        beam.Map(lambda x: beam.io.gcp.bigquery.ReadFromBigQueryRequest(table="de-porto:de_porto.equipment"))
        | beam.io.ReadAllFromBigQuery()
        | "number to date year1" >> beam.Map(int_to_date_year)
)

main_pipeline = (
        p
        | "read" >> beam.io.ReadFromPubSub(topic="projects/de-porto/topics/equipment-gps")
        | "bytes to dict" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
        | "To wkt point" >> beam.Map(to_wkt_point)
        | "To BQ Row" >> beam.Map(payload_epoch_to_utc)
        | "debug1" >> beam.Map(debug)
        | "timestamp" >> beam.Map(lambda src: window.TimestampedValue(
            src,
            dt.datetime.fromisoformat(src["timestamp"]).timestamp()
        ))
)

fuel_theft_pipeline = (
        main_pipeline
        | "To fuel KV pair" >> beam.Map(lambda x: (x["id"], {"timestamp": x["timestamp"], "location": x["location"], "fuel_level": x["fuel_level"]}))
        | "debug2" >> beam.Map(debug)
        | "Get Fuel Diff" >> beam.ParDo(FuelDiff())
        | "debug3" >> beam.Map(debug)
        | "filter theft" >> beam.Filter(lambda x: x["fuel_diff"] > 10)
        | "store fuel theft data" >> beam.io.WriteToBigQuery("de-porto:de_porto.fuel_theft")
)

store_pipeline = (
        main_pipeline
        | "windowing" >> beam.WindowInto(window.FixedWindows(30))
        | "enrich data" >> beam.FlatMap(enrich_payload, equipments=beam.pvalue.AsIter(periodic_side_pipeline))
        | "store iot data" >> beam.io.WriteToBigQuery(bq_table)
)

result = p.run()
result.wait_until_finish()

"""
python3 src/dataflow/iot-pipeline.py \
--runner DataflowRunner \
--region asia-southeast2 \
--project de-porto \
--temp_location gs://de-porto/temp \
--staging_location gs://de-porto/staging

build Dataflow template
python3 src/dataflow/iot-pipeline.py \
--runner DataflowRunner \
--project de-porto \
--region asia-southeast2 \
--temp_location gs://de-porto/temp \
--staging_location gs://de-porto/staging \
--template_location gs://de-porto/dataflow-template/iot-pipeline
"""
