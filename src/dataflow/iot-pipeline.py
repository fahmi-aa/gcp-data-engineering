import json

import apache_beam as beam
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
import datetime as dt
from apache_beam.options.pipeline_options import GoogleCloudOptions
import logging as log


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
        | "windowing" >> beam.WindowInto(window.FixedWindows(30))
)

final_pipeline = (
        main_pipeline
        | "enrich data" >> beam.FlatMap(enrich_payload, equipments=beam.pvalue.AsIter(periodic_side_pipeline))
        | "store" >> beam.io.WriteToBigQuery(bq_table)
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
