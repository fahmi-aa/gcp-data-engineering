import json
import apache_beam as beam
import datetime as dt
from google.cloud.bigquery.table import Row
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

options = PipelineOptions(streaming=True)

p = beam.Pipeline(options=options)
pipeline = (
    p
    | "read" >> beam.io.ReadFromPubSub(topic="projects/de-porto/topics/equipment-gps")
    | "bytes to dict" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
    # | "windows" >> beam.WindowInto(beam.window.SlidingWindows(30, 5))
    # | "print" >> beam.Map(lambda x: print(x))
    | "To BQ Row" >> beam.Map(lambda x: {"timestamp": dt.datetime.utcfromtimestamp(x["timestamp"]).isoformat(), "id": x["id"], "type": x["type"], "x": x["x"], "y": x["y"]})
    | "store" >> beam.io.WriteToBigQuery("de-porto:de_porto.gps_pubsub")
)

result = p.run()
result.wait_until_finish()