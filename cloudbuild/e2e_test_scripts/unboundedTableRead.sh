echo Testing Unbounded job which Succeeds..

now=$(date "+%Y-%m-%d")
REFRESH_INTERVAL=5

#Create a random JOB_ID
JOB_ID=$(printf '%s' $(echo "$RANDOM" | md5sum) | cut -c 1-25)
echo JOB ID: "$JOB_ID"

# Running this job async to make sure it exits so that dynamic data can be added
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar=$JAR_LOCATION --cluster=$CLUSTER_NAME --region=$REGION --async -- --gcp-project $ARG_PROJECT --bq-dataset $ARG_DATASET --bq-table $ARG_TABLE_UNBOUNDED_TABLE --streaming --agg-prop name --ts-prop ts --refresh-interval $REFRESH_INTERVAL

# Dynamically adding the data. This is timed 2.5 min wait for read and 5 min refresh time.
python cloudbuild/python_scripts/insertDynamicPartitions.py -- --now_timestamp="$now" --arg_project==$ARG_PROJECT --arg_dataset==$ARG_DATASET --arg_table=$ARG_TABLE_UNBOUNDED_TABLE --refresh_interval=$REFRESH_INTERVAL

#Kill the dataproc job
gcloud dataproc jobs kill "$JOB_ID" --region=$REGION

# Now check the success of the job
python3 cloudbuild/python_scripts/parse_logs.py -- --job_id=$JOB_ID --project_id=$PROJECT_ID --cluster_name=$CLUSTER_NAME --no_workers=$NO_WORKERS --region=$REGION --arg_project=$ARG_PROJECT --arg_dataset=$ARG_DATASET --arg_table=$ARG_TABLE_UNBOUNDED_TABLE
ret=$?
if [ $ret -neq 0 ]
then
   echo Run Failed
   exit 1
else
   echo Run Succeeds
fi
