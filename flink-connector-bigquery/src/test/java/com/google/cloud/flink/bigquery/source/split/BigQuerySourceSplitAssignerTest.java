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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.common.truth.Truth8;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceSplitAssignerTest {

    private BigQueryReadOptions readOptions;

    @Before
    public void beforeTest() throws IOException {
        this.readOptions =
                StorageClientFaker.createReadOptions(
                        0, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
    }

    @Test
    public void testAssignment() {
        // initialize the assigner with default options since we are faking the bigquery services
        BigQuerySourceSplitAssigner assigner =
                new BigQuerySourceSplitAssigner(
                        this.readOptions, BigQuerySourceEnumState.initialState());
        // request the retrieval of the bigquery table info
        assigner.open();

        // should retrieve the first split representing the firt stream
        Optional<BigQuerySourceSplit> maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isPresent();
        // At this point of time
        // remainingSourceSplits = 0
        // alreadyProcessedTableStreams = 0
        // remainingTableStreams = 1
        // assignedSourceSplits = 1
        // Coz, out of 2 TableStreams, one is converted into a Split and assigned.
        BigQuerySourceEnumState stateAfterGetNext = assigner.snapshotState(0);
        assertThat(stateAfterGetNext.getRemaniningTableStreams().size()).isEqualTo(1);
        assertThat(stateAfterGetNext.getAssignedSourceSplits().size()).isEqualTo(1);
        // should retrieve the second split representing the second stream
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isPresent();
        BigQuerySourceSplit split = null;
        if (maybeSplit.isPresent()) split = maybeSplit.get();
        // no more splits should be available
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isEmpty();
        assertThat(assigner.noMoreSplits()).isTrue();
        // lets check on the enum state
        BigQuerySourceEnumState state = assigner.snapshotState(0);
        assertThat(state.getRemaniningTableStreams()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isEmpty();
        // add some splits back
        assigner.addSplitsBack(Lists.newArrayList(split));
        // check again on the enum state
        state = assigner.snapshotState(0);
        assertThat(state.getRemaniningTableStreams()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isNotEmpty();
        // empty it again and check
        assigner.getNext();
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isEmpty();
        assertThat(assigner.noMoreSplits()).isTrue();
    }

    /**
     * Test to check invalid query is handled by {@link BigQuerySourceSplitAssigner}
     *
     * @throws IOException when createInvalidQueryReadOptions() fails
     */
    @Test
    public void testInvalidQueryRead() throws IOException {
        BigQueryReadOptions invalidQueryReadOptions =
                StorageClientFaker.createInvalidQueryReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        // initialize the assigner with default options since we are faking the bigquery services
        BigQuerySourceSplitAssigner assigner =
                new BigQuerySourceSplitAssigner(
                        invalidQueryReadOptions, BigQuerySourceEnumState.initialState());
        // request the retrieval of the bigquery table info
        IllegalStateException failedQueryException =
                assertThrows(IllegalStateException.class, assigner::open);
        assertThat(failedQueryException.getMessage())
                .contains("The BigQuery query execution failed with errors");
    }

    /**
     * Test to check if a valid query is executed properly, new schema is generated and assigner
     * handles this split effectively.
     *
     * @throws IOException when setQueryAndExecutionProject() fails
     */
    @Test
    public void testQueryRead() throws IOException {
        // Creating read options with a query to check if a succesfull query runs properly
        BigQueryReadOptions queryReadOptions =
                BigQueryReadOptions.builder()
                        .setQueryAndExecutionProject("Select * from table", "project")
                        .setSnapshotTimestampInMillis(System.currentTimeMillis())
                        .setBigQueryConnectOptions(
                                BigQueryConnectOptions.builder()
                                        .setDataset("dataset")
                                        .setProjectId("project")
                                        .setTable("table")
                                        .setCredentialsOptions(null)
                                        .setTestingBigQueryServices(
                                                () ->
                                                        new StorageClientFaker.FakeBigQueryServices(
                                                                new StorageClientFaker
                                                                        .FakeBigQueryServices
                                                                        .FakeBigQueryStorageReadClient(
                                                                        StorageClientFaker
                                                                                .fakeReadSession(
                                                                                        10,
                                                                                        1,
                                                                                        StorageClientFaker
                                                                                                .SIMPLE_AVRO_SCHEMA_STRING),
                                                                        StorageClientFaker
                                                                                ::createRecordList,
                                                                        0.0D)))
                                        .build())
                        .build();
        // initialize the assigner with default options since we are faking the bigquery services
        BigQuerySourceSplitAssigner assigner =
                new BigQuerySourceSplitAssigner(
                        queryReadOptions, BigQuerySourceEnumState.initialState());
        // request the retrieval of the bigquery table info
        assigner.open();

        Optional<BigQuerySourceSplit> maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isPresent();
    }
}
