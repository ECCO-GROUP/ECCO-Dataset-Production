#!/bin/bash
set -eo pipefail

S3_TASKLIST_ROOT="s3://.../tasks/" # Replace with your S3 path

JOB_DEFINITION=ecco-dataset-production-job-definition_no_spot
JOB_QUEUE=edpq_no_spot

echo "Copying to S3..."
aws s3 sync "./first-middle-last-tasklists/" "$S3_TASKLIST_ROOT"

for tasklist in $(aws s3 ls $S3_TASKLIST_ROOT | awk '{print $4}'); do
    aws batch submit-job \
      --job-name              $(echo $tasklist | sed 's/_task//;s/.json//') \
      --job-queue             $JOB_QUEUE \
      --job-definition        $JOB_DEFINITION \
      --container-overrides   "{\"environment\": [{\"name\": \"TASKLIST\", \"value\": \"$S3_TASKLIST_ROOT$tasklist\" } ] }"
done
