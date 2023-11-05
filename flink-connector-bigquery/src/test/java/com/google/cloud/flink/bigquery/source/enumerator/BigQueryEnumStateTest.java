/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.source.enumerator;

import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQueryEnumStateTest {
    /** Test to check the initialisation of {@link BigQuerySourceEnumState} class. */
    @Test
    public void testInitialisation() {
        BigQuerySourceEnumState state = BigQuerySourceEnumState.initialState();
        assertThat(state.getAssignedSourceSplits()).isEmpty();
        assertThat(state.getCompletedTableStreams()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isEmpty();
        assertThat(state.getRemaniningTableStreams()).isEmpty();
        assertThat(state.isInitialized()).isFalse();
    }

    /** Test to check the equality of {@link BigQuerySourceEnumState} class. */
    @Test
    public void testEquality() {
        List<String> remaniningTableStreams1 = Lists.newArrayList("Stream2", "Stream3");
        List<String> completedTableStreams1 = Lists.newArrayList("Stream0");
        List<BigQuerySourceSplit> remainingSourceSplits1 =
                Lists.newArrayList(new BigQuerySourceSplit("Stream1", 10L));
        Map<String, BigQuerySourceSplit> assignedSourceSplits1 = new HashMap<>();

        List<String> remaniningTableStreams2 = new ArrayList<String>(remaniningTableStreams1);
        List<String> completedTableStreams2 = new ArrayList<String>(completedTableStreams1);
        List<BigQuerySourceSplit> remainingSourceSplits2 =
                Lists.newArrayList(new BigQuerySourceSplit("Stream1", 10L));
        Map<String, BigQuerySourceSplit> assignedSourceSplits2 = new HashMap<>();

        BigQuerySourceEnumState state1 =
                new BigQuerySourceEnumState(
                        remaniningTableStreams1,
                        completedTableStreams1,
                        remainingSourceSplits1,
                        assignedSourceSplits1,
                        false);
        BigQuerySourceEnumState state2 =
                new BigQuerySourceEnumState(
                        remaniningTableStreams2,
                        completedTableStreams2,
                        remainingSourceSplits2,
                        assignedSourceSplits2,
                        false);

        assertThat(state1).isEqualTo(state2);
        assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
        assertThat(state1.toString()).isEqualTo(state2.toString());

        List<String> completedTableStreams3 = new ArrayList<String>(completedTableStreams2);
        completedTableStreams3.remove(0);
        BigQuerySourceEnumState state3 =
                new BigQuerySourceEnumState(
                        remaniningTableStreams2,
                        completedTableStreams3,
                        remainingSourceSplits2,
                        assignedSourceSplits2,
                        false);
        assertThat(state1).isNotEqualTo(state3);
        assertThat(state2).isNotEqualTo(state3);
    }
}
