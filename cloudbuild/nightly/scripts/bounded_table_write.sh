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
PROPERTIES=$1

# We won't run this async as we can wait for a bounded job to succeed or fail.
# Create the table from the source table schema.
python3 cloudbuild/nightly/scripts/python-scripts/create_sink_table.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source_table_name "$SOURCE_TABLE_NAME" --destination_table_name "$DESTINATION_TABLE_NAME"
# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$DESTINATION_TABLE_NAME"
# Run the sink JAR JOB
bq --location=$REGION cp -a -f -n "$PROJECT_NAME":"$DATASET_NAME"."$SOURCE_TABLE_NAME" "$PROJECT_NAME":"$DATASET_NAME"."$DESTINATION_TABLE_NAME"
#gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --properties="$PROPERTIES" -- --gcp-project "$PROJECT_NAME" --bq-dataset "$DATASET_NAME" --bq-table "$TABLE_NAME"