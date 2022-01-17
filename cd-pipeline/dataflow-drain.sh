JOB_ID=$(gcloud dataflow jobs list --filter="name=iot" --status=active --limit=1 --format="value(JOB_ID)")
JOB_ID=${JOB_ID:-"NONE"}

gcloud dataflow jobs drain "${JOB_ID}" --region asia-southeast2