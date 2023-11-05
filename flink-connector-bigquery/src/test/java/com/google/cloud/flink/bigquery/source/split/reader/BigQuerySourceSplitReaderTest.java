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

package com.google.cloud.flink.bigquery.source.split.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitState;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceSplitReaderTest {

    @Test
    public void testSplitReaderSmall() throws IOException {
        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, 10);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        BigQuerySourceSplit split2 = new BigQuerySourceSplit("stream2", 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split, split2));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // there is one finished split and is named stream1
        assertThat(records.finishedSplits()).hasSize(1);

        String firstSplit = records.nextSplit();
        assertThat(firstSplit).isNotNull();
        assertThat(firstSplit).isEqualTo(splitName);

        int i = 0;
        while (records.nextRecordFromSplit() != null) {
            i++;
        }
        // there were 10 generic records read
        assertThat(i).isEqualTo(10);
        // there are no more splits
        assertThat(records.nextSplit()).isNull();

        // now there should be another split to process
        records = reader.fetch();
        assertThat(records.finishedSplits()).isNotEmpty();

        // after processing no more splits can be retrieved
        records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
    }

    @Test
    public void testSplitReaderMultipleFetch() throws IOException {
        Integer totalRecordCount = 15000;
        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        totalRecordCount, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        // no limits in the read
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // there shouldn't be a finished split
        assertThat(records.finishedSplits()).isEmpty();

        String firstPartialSplit = records.nextSplit();
        assertThat(firstPartialSplit).isNotNull();
        assertThat(firstPartialSplit).isEqualTo(splitName);

        int i = 0;
        while (records.nextRecordFromSplit() != null) {
            i++;
        }
        // there were less than 10000 generic records read, the max per fetch
        assertThat(i).isLessThan(10001);
        // there are no more splits
        assertThat(records.nextSplit()).isNull();

        // now there should be more data in the split and now should be able to finalize it
        records = reader.fetch();
        assertThat(records.finishedSplits()).isNotEmpty();

        // after processing no more splits can be retrieved
        records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
    }

    /**
     * Test to check the error handling for iterator that always fails.
     *
     * @throws IOException IOException inc ase createReadOptions() fails.
     */
    @Test
    public void testFaultyIterator() throws IOException {
        // The reader that always fails
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10,
                        1,
                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING,
                        StorageClientFaker::createRecordList,
                        // Changed the error percentage to 100% so that it always fails.
                        100D);

        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        Mockito.when(readerContext.getConfiguration()).thenReturn(new Configuration());
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "myStream";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);

        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        // Make sure we have an empty record list, there is no split that is covered.
        assertThat(records.nextSplit()).isNull();
    }

    /**
     * Test to check if invalid read options are dealt with and to check if reading with a closed
     * reader is dealt with
     *
     * @throws Exception when close() fails, or createReadOptions fails
     */
    @Test
    public void testErrorHandling() throws Exception {

        // The read options that produce an error.
        BigQueryReadOptions readOptions =
                StorageClientFaker.createInvalidQueryReadOptions(
                        10, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);

        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        Mockito.when(readerContext.getConfiguration()).thenReturn(new Configuration());
        // Just to make sure split time metrics are created.
        Mockito.when(readerContext.metricGroup())
                .thenReturn(Mockito.mock(SourceReaderMetricGroup.class));

        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "myFailingStream";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);

        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        // MAKE SURE WE HAVE AN EMPTY RECORD LIST

        assertThat(records.nextSplit()).isNull();

        // we close the reader
        reader.close();

        // Exception should be thrown as the reader is closed.
        assertThrows(IllegalStateException.class, reader::fetch);
    }

    /**
     * In case reader fails in between, then split can have an offset > 0 Test case to check if this
     * is handled. We provide with a split of an increased offset to check if it is handled.
     *
     * @throws Exception when createReadOptions fails or close() fails
     */
    @Test
    public void testFailedSplitFetch() throws Exception {
        Integer totalRecordCount = 15000;
        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        totalRecordCount, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        // no limits in the read
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "myFailingSplit";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        BigQuerySourceSplitState splitState = new BigQuerySourceSplitState(split);

        records.nextSplit();
        // We update the split state
        int recordsFetched = 0;
        while (records.nextRecordFromSplit() != null) {
            recordsFetched++;
            splitState.updateOffset();
        }

        // Fail the reader.
        reader.close();

        // Create a new reader.
        reader = new BigQuerySourceSplitReader(readOptions, context);

        // Reassign the split
        change = new SplitsAddition<>(Lists.newArrayList(splitState.toBigQuerySourceSplit()));
        reader.handleSplitsChanges(change);

        records = reader.fetch();
        records.nextSplit();
        while (records.nextRecordFromSplit() != null) {
            recordsFetched++;
        }
        assertThat(recordsFetched).isEqualTo(totalRecordCount);
    }

    /**
     * Test to check if Response without Avro Schema are handled. For this we mock an Arrow Schema
     * using createArrowReadOptions() in {@link StorageClientFaker} class.
     *
     * @throws IOException when createArrowReadOptions fails.
     */
    @Test
    public void testInvalidAvro() throws IOException {

        BigQueryReadOptions readOptions =
                StorageClientFaker.createArrowReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        Mockito.when(readerContext.getConfiguration()).thenReturn(new Configuration());
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        // MAKE SURE WE HAVE AN EMPTY RECORD LIST

        assertThat(records.nextSplit()).isNull();
    }
}
