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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumerator;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroDeserializationSchema;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceTest {

    @Test
    public void testReadAvros() throws IOException {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(readOptions);
        TypeInformation<GenericRecord> expected =
                new GenericRecordAvroTypeInfo(StorageClientFaker.SIMPLE_AVRO_SCHEMA);
        assertThat(source.getDeserializationSchema().getProducedType()).isEqualTo(expected);
    }

    @Test
    public void testReadAvrosFromQuery() throws IOException {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvrosFromQuery(readOptions, "SELECT 1", "someProject", -1);

        TypeInformation<GenericRecord> expected =
                new GenericRecordAvroTypeInfo(
                        new Schema.Parser()
                                .parse(StorageClientFaker.SIMPLE_AVRO_SCHEMA_FORQUERY_STRING));
        assertThat(source.getDeserializationSchema().getProducedType()).isEqualTo(expected);
    }

    /**
     * Test that checks the handling of failing query options.
     *
     * @throws IOException when createInvalidQueryReadOptions() fails
     */
    @Test
    public void testFailingReadAvrosFromQuery() throws IOException {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createInvalidQueryReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        assertThrows(
                RuntimeException.class,
                () ->
                        BigQuerySource.readAvrosFromQuery(
                                readOptions,
                                readOptions.getQuery(),
                                readOptions.getQueryExecutionProject(),
                                -1));
    }

    /**
     * Test to check the create and restore enumerator methods.
     *
     * @throws Exception: IOException when createReadOptions fails, Exception when create/restore
     *     enumerator, snapshot state fails.
     */
    @Test
    public void testCreateAndRestoreEnumerator() throws Exception {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        BigQueryConnectOptions connectOptions = readOptions.getBigQueryConnectOptions();

        TableSchema tableSchema =
                BigQueryServicesFactory.instance(connectOptions)
                        .queryClient()
                        .getTableSchema(
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                connectOptions.getTable());

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.<GenericRecord>builder()
                        .setDeserializationSchema(
                                new AvroDeserializationSchema(
                                        SchemaTransform.toGenericAvroSchema(
                                                        String.format(
                                                                "%s.%s.%s",
                                                                connectOptions.getProjectId(),
                                                                connectOptions.getDataset(),
                                                                connectOptions.getTable()),
                                                        tableSchema.getFields())
                                                .toString()))
                        .setReadOptions(readOptions)
                        .build();

        SplitEnumeratorContext<BigQuerySourceSplit> enumContext =
                Mockito.mock(SplitEnumeratorContext.class);

        // Create an enumerator
        BigQuerySourceEnumerator bqSourceEnum =
                (BigQuerySourceEnumerator) bqSource.createEnumerator(enumContext);

        BigQuerySourceEnumState enumState = bqSourceEnum.snapshotState(0);

        // Restore enumerator
        BigQuerySourceEnumerator restoredBqSourceEnum =
                (BigQuerySourceEnumerator) bqSource.restoreEnumerator(enumContext, enumState);

        // Previous and Restored Enumerators are the same.
        assertThat(bqSourceEnum).isEqualTo(restoredBqSourceEnum);
    }
}
