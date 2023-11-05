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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitAssigner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceEnumeratorTest {

    private TestingSplitEnumeratorContext<BigQuerySourceSplit> enumContext;
    private final Boundedness boundedness = Boundedness.BOUNDED;
    private BigQuerySourceSplitAssigner assigner;

    @Before
    public void beforeTest() throws IOException {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        0, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        this.enumContext = new TestingSplitEnumeratorContext<>(3);
        // Register certain readers
        enumContext.registerReader(1, "reader1");
        enumContext.registerReader(2, "reader2");
        enumContext.registerReader(3, "reader3");
        enumContext.registerReader(4, "reader4");
        this.assigner =
                new BigQuerySourceSplitAssigner(
                        readOptions, BigQuerySourceEnumState.initialState());
    }

    /** Test to check the equality of {@link BigQuerySourceEnumerator} class. */
    @Test
    public void testEquals() {
        BigQuerySourceEnumerator enumerator1 =
                new BigQuerySourceEnumerator(boundedness, enumContext, assigner);

        BigQuerySourceEnumerator enumerator2 =
                new BigQuerySourceEnumerator(boundedness, enumContext, assigner);
        // Check if the enumerators are equal.
        assertThat(enumerator1).isEqualTo(enumerator2);

        SplitEnumeratorContext<BigQuerySourceSplit> newEnumContext =
                new TestingSplitEnumeratorContext<>(3);
        enumerator2 = new BigQuerySourceEnumerator(boundedness, newEnumContext, assigner);

        assertThat(enumerator1).isNotEqualTo(enumerator2);
    }

    /**
     * Test to check invalid/unregistered reader sends a split request.
     *
     * @throws Exception when snapshot state fails.
     */
    @Test
    public void testFailingReaderRequests() throws Exception {
        BigQuerySourceEnumState state;
        try (BigQuerySourceEnumerator testEnumerator =
                new BigQuerySourceEnumerator(boundedness, enumContext, assigner)) {
            // Create the Enumerator
            testEnumerator.start();
            // A non registered or a failed reader sends the request.
            testEnumerator.handleSplitRequest(5, "reader5");
            // See the Enum State
            state = testEnumerator.snapshotState(0);
        }
        // Check if the split is not assigned
        assertThat(state.getAssignedSourceSplits()).isEmpty();
    }

    /**
     * test to check handling of split request and assignment. Check if NoMoreSplits single is
     * issued when splits have been exhausted.
     *
     * @throws Exception when snapshot state fails.
     */
    @Test
    public void testHandleSplitRequest() throws Exception {
        BigQuerySourceEnumState state;
        try (BigQuerySourceEnumerator testEnumerator =
                new BigQuerySourceEnumerator(boundedness, enumContext, assigner)) {
            // Create the Enumerator
            testEnumerator.start();
            // Split 1 is assigned to reader 2
            testEnumerator.handleSplitRequest(2, "reader2");
            // See the Enum State
            state = testEnumerator.snapshotState(0);
            // Check if split request is handled well.
            assertThat(state.getAssignedSourceSplits().size()).isEqualTo(1);
            assertThat(state.getRemaniningTableStreams().size()).isEqualTo(1);

            List<BigQuerySourceSplit> failedSplits =
                    (enumContext.getSplitAssignments().get(2)).getAssignedSplits();
            // Split 2 is assigned to Reader 3.
            testEnumerator.handleSplitRequest(3, "reader3");
            // Since there were only 2 streams, no more splits is left -> noMoreSplits
            state = testEnumerator.snapshotState(1);
            assertThat(state.getRemainingSourceSplits().isEmpty()).isTrue();
            assertThat(state.getRemaniningTableStreams().isEmpty()).isTrue();
            // Now ask for one more split
            testEnumerator.handleSplitRequest(1, "reader1");
            // Since there were 2 streams,Confirm that NoMoreSplits is sent to the Readers.
            assertThat(
                            enumContext.getSplitAssignments().values().stream()
                                    .allMatch(
                                            TestingSplitEnumeratorContext.SplitAssignmentState
                                                    ::hasReceivedNoMoreSplitsSignal))
                    .isTrue();

            // Assume Reader 2 failed.
            testEnumerator.addSplitsBack(failedSplits, 2);
            state = testEnumerator.snapshotState(2);
            // Check if split is available for reassignment
            assertThat(state.getRemainingSourceSplits().size()).isEqualTo(1);
            assertThat(state.getRemaniningTableStreams().isEmpty()).isTrue();
            testEnumerator.handleSplitRequest(4, "reader4");
            // Since there were only 2 streams, no more splits is left -> noMoreSplits
            state = testEnumerator.snapshotState(3);
        }
        assertThat(state.getRemainingSourceSplits().isEmpty()).isTrue();
        assertThat(state.getRemaniningTableStreams().isEmpty()).isTrue();
    }
}
