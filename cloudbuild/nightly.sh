#!/bin/bash

# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN install -DskipTests
    exit
    ;;
  # Run e2e tests
  e2etest)
    # gcloud config set project $PROJECT_ID
    # Create a random JOB_ID
    # JOB_ID=$(printf '%s' $(echo "$RANDOM" | md5) | cut -c 1-25)
    # echo JOB ID: "$JOB_ID"
    # We won't run this async as we can wait for a bounded job to succeed or fail.
    # gcloud dataproc jobs submit flink --id "$JOB_ID" --jar=$JAR_LOCATION --cluster=$CLUSTER_NAME --region=$REGION -- --gcp-project $ARG_PROJECT_SIMPLE_TABLE --bq-dataset $ARG_DATASET_SIMPLE_TABLE --bq-table $ARG_TABLE_SIMPLE_TABLE --agg-prop name
    # Now check the success of the job
    JOB_ID="52315ac9c9804cbebc9167b22a486fe9"
    python3 cloudbuild/python_scripts/parse_logs.py -- --job_id $JOB_ID --project_id $PROJECT_ID --cluster_name $CLUSTER_NAME --no_workers $NO_WORKERS --region $REGION --arg_project $ARG_PROJECT_SIMPLE_TABLE --arg_dataset $ARG_DATASET_SIMPLE_TABLE --arg_table $ARG_TABLE_SIMPLE_TABLE
    ret=$?
    if [ $ret -ne 0 ]
    then
       echo Run Failed
       exit 1
    else
       echo Run Succeeds
    fi
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
