import json
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
import datetime as dt
from apache_beam.options.pipeline_options import PipelineOptions

def debug(payload):
    print(payload)
    return payload

def payload_epoch_to_utc(payload):
    # import datetime as dt
    payload["timestamp"] = dt.datetime.utcfromtimestamp(payload["timestamp"]).isoformat()
    return payload

def enrich_payload(payload, equipments):
    id = payload["id"]
    for equipment in equipments:
        print(equipment)
        if id == equipment["id"]:
            payload["type"] = equipment["type"]
            payload["brand"] = equipment["brand"]
            payload["year"] = equipment["year"]

    return payload


bq_table = "de-porto:de_porto.gps_pubsub"
options = PipelineOptions(streaming=True, save_main_session=True)

p = beam.Pipeline(options=options)

side_pipeline = (
    p
    | "periodic" >> PeriodicImpulse(0, 999999999999, 30, True)
    | "map to read request" >> beam.Map(lambda x: beam.io.ReadFromBigQuery(table=bq_table))
    # | "debug" >> beam.Map(debug)
    | beam.io.ReadAllFromBigQuery()
)
# python3.7 pubsub-to-bq.py \
# --runner DataflowRunner \
# --region asia-southeast2 \
# --project de-porto \
# --temp_location gs://de-porto/temp \
# --staging_location gs://de-porto/staging
# --max_num_workers 1

pipeline = (
    p
    | "read" >> beam.io.ReadFromPubSub(topic="projects/de-porto/topics/equipment-gps")
    | "bytes to dict" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
    # | "timestamp" >> beam.Map(lambda src: window.TimestampedValue(src, src["timestamp"]))
    | "windowing" >> beam.WindowInto(window.FixedWindows(30))
    | "To BQ Row" >> beam.Map(payload_epoch_to_utc)
    | "debug1" >> beam.Map(debug)
    | "enrich data" >> beam.Map(enrich_payload, equipments=beam.pvalue.AsDict(side_pipeline))
    | "store" >> beam.io.WriteToBigQuery(bq_table)
)

result = p.run()
result.wait_until_finish()
