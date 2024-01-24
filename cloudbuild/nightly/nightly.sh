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
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/nightly/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN clean install -DskipTests
    gcloud storage cp "$MVN_JAR_LOCATION" "$GCS_JAR_LOCATION"
    exit
    ;;

  # Create the cluster
  create_clusters)
    #  Get the timestamp to append to cluster name.
    timestamp=$(date +"%Y%m%d%H%M%S")
    # 1. Create the first cluster for bounded read.
    # Modify the cluster name, staging and temp bucket names for all tests.
    CLUSTER_NAME_SMALL_TEST="$CLUSTER_NAME_SMALL_TEST"-"$timestamp"
    TEMP_BUCKET_SMALL_TEST="$TEMP_BUCKET_SMALL_TEST"-"$timestamp"
    STAGING_BUCKET_SMALL_TEST="$STAGING_BUCKET_SMALL_TEST"-"$timestamp"
    source cloudbuild/nightly/scripts/create_dataproc_cluster.sh "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$NUM_WORKERS_SMALL_TEST" "$TEMP_BUCKET_SMALL_TEST" "$STAGING_BUCKET_SMALL_TEST"
    # 2. Create the second cluster for unbounded read.
    # Modify the cluster name, staging and temp bucket names for all tests.
    CLUSTER_NAME_UNBOUNDED_TABLE_TEST="$CLUSTER_NAME_UNBOUNDED_TABLE_TEST"-"$timestamp"
    TEMP_BUCKET_UNBOUNDED_TABLE_TEST="$TEMP_BUCKET_UNBOUNDED_TABLE_TEST"-"$timestamp"
    STAGING_BUCKET_UNBOUNDED_TABLE_TEST="$STAGING_BUCKET_UNBOUNDED_TABLE_TEST"-"$timestamp"
    source cloudbuild/nightly/scripts/create_dataproc_cluster.sh "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST" "$NUM_WORKERS_UNBOUNDED_TABLE_TEST" "$TEMP_BUCKET_UNBOUNDED_TABLE_TEST" "$STAGING_BUCKET_UNBOUNDED_TABLE_TEST"
    exit
    ;;

  # Run the small table read bounded e2e test.
  e2e_bounded_read_small_table_test)
    # Run the simple bounded table test.
    source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SIMPLE_TABLE" "$AGG_PROP_NAME_SIMPLE_TABLE" "" "bounded"
    exit
    ;;

  # Run the nested schema table read bounded e2e test.
  e2e_bounded_read_nested_schema_table_test)
    # Run the complex schema table test.
    source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_COMPLEX_SCHEMA_TABLE" "$AGG_PROP_NAME_COMPLEX_SCHEMA_TABLE" "" "bounded"
    exit
    ;;

  # Run the query read bounded e2e test.
  e2e_bounded_read_query_test)
    # Run the query test.
    source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "" "" "$QUERY" "bounded"
    exit
    ;;

  # Run the large table O(GB's) read bounded e2e test.
  e2e_bounded_read_large_table_test)
    # Run the large table test.
    source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_LARGE_TABLE" "$AGG_PROP_NAME_LARGE_TABLE" "" "bounded"
    exit
    ;;

  # Run the e2e tests unbounded partitioned table
  e2e_unbounded_read_test)
    # Run the unbounded source test.
    source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_UNBOUNDED_TABLE" "$AGG_PROP_NAME_UNBOUNDED_TABLE" "" "unbounded"
    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
