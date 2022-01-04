import json
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
import datetime as dt
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging as log

def debug(payload):
    log.info(payload)
    return payload

def payload_epoch_to_utc(payload):
    payload["timestamp"] = dt.datetime.utcfromtimestamp(payload["timestamp"]).isoformat()
    return payload

def to_wkt_point(payload):
    payload["location"] = f'POINT({payload["y"]} {payload["y"]}'
    del payload["x"]
    del payload["y"]

    return payload

def enrich_payload(payload, equipments):
    id = payload["id"]
    log.info(equipments)
    for equipment in equipments:
        log.info(equipment)
        if id == equipment["id"]:
            payload["type"] = equipment["type"]
            payload["brand"] = equipment["brand"]
            payload["year"] = equipment["year"]

            break

    log.info(payload)
    yield payload


bq_table = "de-porto:de_porto.iot_log"
# options = PipelineOptions(streaming=True, save_main_session=True, worker_machine_type="n1-standard-1")
options = GoogleCloudOptions(streaming=True, save_main_session=True, job_name="iot")

p = beam.Pipeline(options=options)

side_pipeline = (
    p
    | "periodic" >> PeriodicImpulse(fire_interval=3600, apply_windowing=True)
    | "map to read request" >>
        beam.Map(lambda x:beam.io.gcp.bigquery.ReadFromBigQueryRequest(table="de-porto:de_porto.equipment"))
    | beam.io.ReadAllFromBigQuery()
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
    | "enrich data" >> beam.Map(enrich_payload, equipments=beam.pvalue.AsIter(side_pipeline))
    | "store" >> beam.io.WriteToBigQuery(bq_table)
)

result = p.run()
result.wait_until_finish()

# python3.7 pubsub-to-bq.py \
# --runner DataflowRunner \
# --region asia-southeast2 \
# --project de-porto \
# --temp_location gs://de-porto/temp \
# --staging_location gs://de-porto/staging
