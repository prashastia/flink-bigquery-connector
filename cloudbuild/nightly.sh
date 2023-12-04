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
    # 1. Run the simple table test.
    source cloudbuild/e2e_test_scripts/simpleTableRead.sh
    # 2. Run the complex schema table test.
    source cloudbuild/e2e_test_scripts/complexSchemaRead.sh
    # 3. Run the large row table test.
    source cloudbuild/e2e_test_scripts/largeRowRead.sh
    # 4. Run the large table test.
    source cloudbuild/e2e_test_scripts/largeTableRead.sh
    # 5. Run the unbounded source test.
    source cloudbuild/e2e_test_scripts/unboundedTableRead.sh
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
