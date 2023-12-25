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
readonly STEP=$1

cd /workspace

case $STEP in

  cancel_dataproc_jobs)
    # 1. Cancel first cluster
    python3 cloudbuild/cancel-dataproc/python-scripts/cancel_dataproc_jobs.py -- --project_id="$PROJECT_ID" --cluster_name="$CLUSTER_NAME_SMALL_TEST" --cluster_region_name="$REGION_SMALL_TEST"
    # 2. Cancel the second cluster
    python3 cloudbuild/cancel-dataproc/python-scripts/cancel_dataproc_jobs.py -- --project_id="$PROJECT_ID" --cluster_name="$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" --cluster_region_name="$REGION_UNBOUNDED_TABLE_TEST"

    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
