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
    gcloud config set project $PROJECT_ID
    # Create a random JOB_ID
    JOB_ID=$(printf '%s' $(echo "$RANDOM" | md5) | cut -c 1-25)
    JOB_ID=2015dff3e51c86f4edeb53cbc
    echo JOB ID: "$JOB_ID"
    # We won't run this async as we can wait for a bounded job to succeed or fail.
    gcloud dataproc jobs submit flink --id "$JOB_ID" --jar=$JAR_LOCATION --cluster=$CLUSTER_NAME --region=$REGION -- --gcp-project $ARG_PROJECT_SIMPLE_TABLE --bq-dataset $ARG_DATASET_SIMPLE_TABLE --bq-table $ARG_TABLE_SIMPLE_TABLE --agg-prop name
    # Now check the success of the job
    # python parse_logs/parseLogs.py "$JOB_ID" $PROJECT_ID $CLUSTER_NAME $NO_WORKERS $REGION $ARG_PROJECT $ARG_DATASET $ARG_TABLE
    # ret=$?
    # if [ $ret -ne 0 ]
    # then
    #    echo Run Failed
    #    exit 1
    # else
    #    echo Run Succeeds
    # fi
    
    # gcloud dataproc jobs submit flink --jar=$JAR_LOCATION --cluster=$CLUSTER_NAME --region=asia-east2 -- --gcp-project testproject-398714 --bq-dataset babynames --bq-table names_2014 --agg-prop name
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
