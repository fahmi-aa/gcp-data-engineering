import json
import datetime as dt
import logging as log

import apache_beam as beam
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders.coders import TupleCoder, FloatCoder

PROJECT_ID = "de-porto"
BIGQUERY_DATASET = "de_porto"
PUBSUB_TOPIC = "equipment-gps"
JOB_NAME = f"iot-{str(dt.datetime.now().timestamp())[:10]}"

class FuelDiff(beam.DoFn):
    STATE = BagStateSpec("fuel", TupleCoder((FloatCoder(), FloatCoder())))

    def process(self, element, timestamp_param=beam.DoFn.TimestampParam, state=beam.DoFn.StateParam(STATE)):
        key = element[0]
        location = element[1]["location"]
        fuel_level = element[1]["fuel_level"]
        datetime = element[1]["timestamp"]
        timestamp = float(timestamp_param)

        state_content = [x for x in state.read()]
        log.info(f"state: {state_content}")
        if not state_content:
            state.add((timestamp, fuel_level))
            yield {"id": key,
                   "timestamp": datetime,
                   "location": location,
                   "fuel_level": fuel_level,
                   "fuel_diff": None}
        else:
            lag_content = state_content[0]
            lag_timestamp = lag_content[0]
            lag_fuel = lag_content[1]
            log.info(f"{lag_content}, {lag_timestamp}, {lag_fuel}")

            state.clear()
            state.add((timestamp, fuel_level))

            output = {"id": key,
                      "timestamp": datetime,
                      "location": location,
                      "fuel_level": fuel_level,
                      "fuel_diff": fuel_level - lag_fuel}
            if timestamp >= lag_timestamp:
                yield output
            else:
                output["fuel_diff"] = -output["fuel_diff"]
                yield output


def theft_filter(payload):
    if payload["fuel_diff"] == None:
        return False
    else:
        return payload["fuel_diff"] < -10


def logInfo(payload):
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


def left_join(payload, equipments):
    id = payload["id"]
    for equipment in equipments:
        if id == equipment["id"]:
            payload["type"] = equipment["type"]
            payload["brand"] = equipment["brand"]
            payload["year"] = equipment["year"]

            break

    yield payload


options = GoogleCloudOptions(streaming=True, save_main_session=True, job_name=JOB_NAME)

p = beam.Pipeline(options=options)

periodic_side_pipeline = (
        p
        | "Periodic Impulse" >> PeriodicImpulse(fire_interval=3600, apply_windowing=True)
        | "Fixed Window" >> beam.WindowInto(window.GlobalWindows(),
                                                 trigger=trigger.Repeatedly(trigger.AfterProcessingTime(60)),
                                                 accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | "Bigquery Request" >>
            beam.Map(lambda x: beam.io.gcp.bigquery.ReadFromBigQueryRequest(table=f"{PROJECT_ID}:{BIGQUERY_DATASET}.equipment"))
        | beam.io.ReadAllFromBigQuery()
        | "Year Number to Date" >> beam.Map(int_to_date_year)
)

main_pipeline = (
        p
        | "Read From Pubsub" >> beam.io.ReadFromPubSub(topic=f"projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}")
        | "Bytes to Dict" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
        | "To WKT Point" >> beam.Map(to_wkt_point)
        | "To Bigquery Row" >> beam.Map(payload_epoch_to_utc)
        | "Apply Watermark" >> beam.Map(lambda src: window.TimestampedValue(
            src,
            dt.datetime.fromisoformat(src["timestamp"]).timestamp()
        ))
)

fuel_theft_pipeline = (
        main_pipeline
        | "To Fuel Location KV" >> beam.Map(
            lambda x: (x["id"], {"timestamp": x["timestamp"], "location": x["location"], "fuel_level": x["fuel_level"]}))
        | "Get Fuel Diff" >> beam.ParDo(FuelDiff())
        | "Fuel Theft Filter" >> beam.Filter(theft_filter)
        | "Store Fuel Theft Data" >> beam.io.WriteToBigQuery(f"{PROJECT_ID}:{BIGQUERY_DATASET}.fuel_theft")
)

store_pipeline = (
        main_pipeline
        | "Side Input Windowing" >> beam.WindowInto(window.FixedWindows(30))
        | "Left Join" >> beam.FlatMap(left_join, equipments=beam.pvalue.AsIter(periodic_side_pipeline))
        | "Store IoT Data" >> beam.io.WriteToBigQuery(f"{PROJECT_ID}:{BIGQUERY_DATASET}.iot_log")
)

result = p.run()

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
