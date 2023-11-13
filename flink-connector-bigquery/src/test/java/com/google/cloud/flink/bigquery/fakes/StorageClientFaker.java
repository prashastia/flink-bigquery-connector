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

package com.google.cloud.flink.bigquery.fakes;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.QueryResultInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.table.restrictions.BigQueryPartition;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility class to generate mocked objects for the BQ storage client. */
public class StorageClientFaker {

    /** Implementation for the BigQuery services for testing purposes. */
    public static class FakeBigQueryServices implements BigQueryServices {

        private final FakeBigQueryStorageReadClient storageReadClient;

        public FakeBigQueryServices(FakeBigQueryStorageReadClient storageReadClient) {
            this.storageReadClient = storageReadClient;
        }

        @Override
        public StorageReadClient getStorageClient(CredentialsOptions readOptions)
                throws IOException {
            return storageReadClient;
        }

        @Override
        public QueryDataClient getQueryDataClient(CredentialsOptions readOptions) {
            return new QueryDataClient() {

                @Override
                public List<String> retrieveTablePartitions(
                        String project, String dataset, String table) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHH");

                    return Lists.newArrayList(
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(5).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(4).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(3).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).format(dtf));
                }

                @Override
                public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                        String project, String dataset, String table) {
                    return Optional.of(
                            new TablePartitionInfo(
                                    "ts",
                                    BigQueryPartition.PartitionType.HOUR,
                                    StandardSQLTypeName.TIMESTAMP));
                }

                @Override
                public TableSchema getTableSchema(String project, String dataset, String table) {
                    return SIMPLE_BQ_TABLE_SCHEMA;
                }

                @Override
                public Optional<QueryResultInfo> runQuery(String projectId, String query) {
                    return Optional.of(QueryResultInfo.succeed("", "", ""));
                }

                @Override
                public Job dryRunQuery(String projectId, String query) {
                    return new Job()
                            .setStatistics(
                                    new JobStatistics()
                                            .setQuery(
                                                    new JobStatistics2()
                                                            .setSchema(SIMPLE_BQ_TABLE_SCHEMA)));
                }
            };
        }

        static class FaultyIterator<T> implements Iterator<T> {

            private final Iterator<T> realIterator;
            private final Double errorPercentage;
            private final Random random = new Random();

            public FaultyIterator(Iterator<T> realIterator, Double errorPercentage) {
                this.realIterator = realIterator;
                Preconditions.checkState(
                        0 <= errorPercentage && errorPercentage <= 100,
                        "The error percentage should be between 0 and 100");
                this.errorPercentage = errorPercentage;
            }

            @Override
            public boolean hasNext() {
                return realIterator.hasNext();
            }

            @Override
            public T next() {
                if (random.nextDouble() * 100 < errorPercentage) {
                    throw new RuntimeException(
                            "Faulty iterator has failed, it will happen with a chance of: "
                                    + errorPercentage);
                }
                return realIterator.next();
            }

            @Override
            public void remove() {
                realIterator.remove();
            }

            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                realIterator.forEachRemaining(action);
            }
        }

        /** Implementation of the server stream for testing purposes. */
        public static class FakeBigQueryServerStream
                implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {

            private final List<ReadRowsResponse> toReturn;
            private final Double errorPercentage;

            public FakeBigQueryServerStream(
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    String schema,
                    String dataPrefix,
                    Long size,
                    Long offset,
                    Double errorPercentage) {
                this.toReturn =
                        createResponse(
                                schema,
                                dataGenerator
                                        .apply(new RecordGenerationParams(schema, size.intValue()))
                                        .stream()
                                        .skip(offset)
                                        .collect(Collectors.toList()),
                                0,
                                size);
                this.errorPercentage = errorPercentage;
            }

            @Override
            @NotNull
            public Iterator<ReadRowsResponse> iterator() {
                return new FaultyIterator<>(toReturn.iterator(), errorPercentage);
            }

            @Override
            public void cancel() {}
        }

        /** Implementation for the storage read client for testing purposes. */
        public static class FakeBigQueryStorageReadClient implements StorageReadClient {

            private final ReadSession session;
            private final SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                    dataGenerator;
            private final Double errorPercentage;

            public FakeBigQueryStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                            dataGenerator) {
                this(session, dataGenerator, 0D);
            }

            public FakeBigQueryStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    Double errorPercentage) {
                this.session = session;
                this.dataGenerator = dataGenerator;
                this.errorPercentage = errorPercentage;
            }

            @Override
            public ReadSession createReadSession(CreateReadSessionRequest request) {
                return session;
            }

            /**
             * Creates Server Stream corresponding to the Request- FakeBigQueryServerStream.
             *
             * @param request The request for the storage API
             * @return Requested ServerStream
             */
            @Override
            public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
                try {
                    // introduce some random delay
                    Thread.sleep(new Random().nextInt(500));
                } catch (InterruptedException ex) {
                }
                // Generate an Valid response
                return new FakeBigQueryServerStream(
                        dataGenerator,
                        session.getAvroSchema().getSchema(),
                        request.getReadStream(),
                        session.getEstimatedRowCount(),
                        request.getOffset(),
                        errorPercentage);
            }

            @Override
            public void close() {}
        }
    }

    /**
     * Implementation of {@link BigQueryServices} interface. This creates Invalid Responses and Non
     * Avro Schemas. dryRunQuery() and runQuery() always throws error. Server Stream provides with
     * invalid server streams.
     */
    public static class FakeBigQueryErrorServices implements BigQueryServices {

        private final FakeBigQueryErrorStorageReadClient storageReadClient;

        public FakeBigQueryErrorServices(FakeBigQueryErrorStorageReadClient storageReadClient) {
            this.storageReadClient = storageReadClient;
        }

        @Override
        public StorageReadClient getStorageClient(CredentialsOptions readOptions)
                throws IOException {
            return storageReadClient;
            //            throw new IOException("Error Retrieving the Storage Read Client");
        }

        @Override
        public QueryDataClient getQueryDataClient(CredentialsOptions readOptions) {
            return new QueryDataClient() {

                @Override
                public List<String> retrieveTablePartitions(
                        String project, String dataset, String table) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHH");

                    return Lists.newArrayList(
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(5).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(4).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).minusHours(3).format(dtf),
                            Instant.now().atOffset(ZoneOffset.UTC).format(dtf));
                }

                @Override
                public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                        String project, String dataset, String table) {
                    return Optional.of(
                            new TablePartitionInfo(
                                    "ts",
                                    BigQueryPartition.PartitionType.HOUR,
                                    StandardSQLTypeName.TIMESTAMP));
                }

                @Override
                public TableSchema getTableSchema(String project, String dataset, String table) {
                    return SIMPLE_BQ_TABLE_SCHEMA;
                }

                @Override
                public Optional<QueryResultInfo> runQuery(String projectId, String query) {
                    List<String> errors = Lists.newArrayList("Invalid Query", "");
                    return Optional.of(QueryResultInfo.failed(errors));
                }

                @Override
                public Job dryRunQuery(String projectId, String query) {
                    throw new RuntimeException(
                            "Problems occurred while trying to dry-run a BigQuery query job.");
                }
            };
        }

        static class FaultyIterator<T> implements Iterator<T> {

            private final Iterator<T> realIterator;
            private final Double errorPercentage;
            private final Random random = new Random();

            public FaultyIterator(Iterator<T> realIterator, Double errorPercentage) {
                this.realIterator = realIterator;
                Preconditions.checkState(
                        0 <= errorPercentage && errorPercentage <= 100,
                        "The error percentage should be between 0 and 100");
                this.errorPercentage = errorPercentage;
            }

            @Override
            public boolean hasNext() {
                return realIterator.hasNext();
            }

            @Override
            public T next() {
                if (random.nextDouble() * 100 < errorPercentage) {
                    throw new RuntimeException(
                            "Faulty iterator has failed, it will happen with a chance of: "
                                    + errorPercentage);
                }
                return realIterator.next();
            }

            @Override
            public void remove() {
                realIterator.remove();
            }

            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                realIterator.forEachRemaining(action);
            }
        }

        /**
         * Class implementing the {@link
         * com.google.cloud.flink.bigquery.services.BigQueryServices.BigQueryServerStream} interface
         * for generating an invalid (throws error) response.
         */
        public static class FakeBigQueryErrorServerStream
                implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {

            private final List<ReadRowsResponse> toReturn;
            private final Double errorPercentage;

            public FakeBigQueryErrorServerStream(
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    String schema,
                    String dataPrefix,
                    Long size,
                    Long offset,
                    Double errorPercentage) {
                this.toReturn =
                        createInvalidResponse(
                                schema,
                                dataGenerator
                                        .apply(new RecordGenerationParams(schema, size.intValue()))
                                        .stream()
                                        .skip(offset)
                                        .collect(Collectors.toList()),
                                0,
                                size);
                this.errorPercentage = errorPercentage;
            }

            @Override
            @NotNull
            public Iterator<ReadRowsResponse> iterator() {
                return new FaultyIterator<>(toReturn.iterator(), errorPercentage);
            }

            @Override
            public void cancel() {}
        }

        /**
         * Class implementing the {@link
         * com.google.cloud.flink.bigquery.services.BigQueryServices.BigQueryServerStream}
         * interface. for generating a response that does not have AvroRows or AvroSchema (done via
         * adding ARROW Schema)
         */
        public static class FakeBigQueryArrowServerStream
                implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {

            private final List<ReadRowsResponse> toReturn;
            private final Double errorPercentage;

            public FakeBigQueryArrowServerStream(
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    String schema,
                    String dataPrefix,
                    Long size,
                    Long offset,
                    Double errorPercentage) {
                this.toReturn =
                        createArrowResponse(
                                schema,
                                dataGenerator
                                        .apply(new RecordGenerationParams(schema, size.intValue()))
                                        .stream()
                                        .skip(offset)
                                        .collect(Collectors.toList()),
                                0,
                                size);
                this.errorPercentage = errorPercentage;
            }

            @Override
            @NotNull
            public Iterator<ReadRowsResponse> iterator() {
                return new FaultyIterator<>(toReturn.iterator(), errorPercentage);
            }

            @Override
            public void cancel() {}
        }

        /** Implementation for the storage read client for testing purposes. */
        public static class FakeBigQueryErrorStorageReadClient implements StorageReadClient {

            private final ReadSession session;
            private final SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                    dataGenerator;
            private final Double errorPercentage;

            public FakeBigQueryErrorStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                            dataGenerator) {
                this(session, dataGenerator, 0D);
            }

            public FakeBigQueryErrorStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    Double errorPercentage) {
                this.session = session;
                this.dataGenerator = dataGenerator;
                this.errorPercentage = errorPercentage;
            }

            @Override
            public ReadSession createReadSession(CreateReadSessionRequest request) {
                return session;
            }

            /**
             * Creates Server Stream corresponding to the Request. 1. In case schema has a marker
             * "invalid_table" in the table field: 1.1 Creates an FakeBigQueryErrorServerStream
             * which always throws an error instead of a response 2. In case request does not have
             * an Avro schema: 2.1 Creates a FakeBigQueryArrowServerStream that creates a stream
             * with Arrow schema. 3. FakeBigQueryServerStream otherwise.
             *
             * @param request The request for the storage API
             * @return Requested ServerStream
             */
            @Override
            public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
                try {
                    // introduce some random delay
                    Thread.sleep(new Random().nextInt(500));
                } catch (InterruptedException ex) {
                }
                // Case where we create an ARROW Schema
                if (session.hasArrowSchema()) {
                    return new FakeBigQueryArrowServerStream(
                            dataGenerator,
                            SIMPLE_AVRO_SCHEMA_STRING,
                            request.getReadStream(),
                            session.getEstimatedRowCount(),
                            request.getOffset(),
                            errorPercentage);
                }
                // Case where we need an Invalid Response
                return new FakeBigQueryErrorServerStream(
                        dataGenerator,
                        session.getAvroSchema().getSchema(),
                        request.getReadStream(),
                        session.getEstimatedRowCount(),
                        request.getOffset(),
                        errorPercentage);
            }

            @Override
            public void close() {}
        }
    }

    public static final String SIMPLE_AVRO_SCHEMA_FIELDS_STRING =
            " \"fields\": [\n"
                    + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                    + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                    + "   {\"name\" : \"ts\", \"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-micros\"}}\n"
                    + " ]\n";
    public static final String SIMPLE_AVRO_SCHEMA_STRING =
            "{\"namespace\": \"project.dataset\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"table\",\n"
                    + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";
    public static final String SIMPLE_AVRO_SCHEMA_FORQUERY_STRING =
            "{\"namespace\": \"project.dataset\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"queryresultschema\",\n"
                    + " \"namespace\": \""
                    + SchemaTransform.DEFAULT_NAMESPACE
                    + "\",\n"
                    + " \"doc\": \"Translated Avro Schema for queryresultschema\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";
    public static final Schema SIMPLE_AVRO_SCHEMA =
            new Schema.Parser().parse(SIMPLE_AVRO_SCHEMA_STRING);

    public static final TableSchema SIMPLE_BQ_TABLE_SCHEMA =
            new TableSchema()
                    .setFields(
                            Lists.newArrayList(
                                    new TableFieldSchema()
                                            .setName("name")
                                            .setType("STRING")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("number")
                                            .setType("INTEGER")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("ts")
                                            .setType("TIMESTAMP")
                                            .setMode("REQUIRED")));

    /** Represents the parameters needed for the Avro data generation. */
    public static class RecordGenerationParams implements Serializable {
        private final String avroSchemaString;
        private final Integer recordCount;

        public RecordGenerationParams(String avroSchemaString, Integer recordCount) {
            this.avroSchemaString = avroSchemaString;
            this.recordCount = recordCount;
        }

        public String getAvroSchemaString() {
            return avroSchemaString;
        }

        public Integer getRecordCount() {
            return recordCount;
        }
    }

    public static ReadSession fakeReadSession(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString) {
        // set up the response for read session request
        List<ReadStream> readStreams =
                IntStream.range(0, expectedReadStreamCount)
                        .mapToObj(i -> ReadStream.newBuilder().setName("stream" + i).build())
                        .collect(Collectors.toList());
        return ReadSession.newBuilder()
                .addAllStreams(readStreams)
                .setEstimatedRowCount(expectedRowCount)
                .setDataFormat(DataFormat.AVRO)
                .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchemaString))
                .build();
    }

    /**
     * Static Method to create readSession that always throw an error in response. This is done by
     * invoking invalidReadOptions() that in turn is responsible for throwing the error.
     *
     * @param expectedRowCount The number of expected rows
     * @param expectedReadStreamCount The number of expected splits/streams
     * @param avroSchemaString The string representing avro schema
     * @return a Read Session
     */
    public static ReadSession fakeInvalidReadSession(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString) {
        // set up the response for read session request
        List<ReadStream> readStreams =
                IntStream.range(0, expectedReadStreamCount)
                        .mapToObj(i -> ReadStream.newBuilder().setName("stream" + i).build())
                        .collect(Collectors.toList());
        return ReadSession.newBuilder()
                .addAllStreams(readStreams)
                .setEstimatedRowCount(expectedRowCount)
                .setDataFormat(DataFormat.AVRO)
                .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchemaString))
                .build();
    }

    /**
     * Static Method to create readSession that does not contain an avro schema. This is done by
     * setting the data format field as ARROW. which when used to create the serverStream invoke
     * ArrowReadOptions() that in turn is responsible for creating the arrow schema.
     *
     * @param expectedRowCount The number of expected rows
     * @param expectedReadStreamCount The number of expected splits/streams
     * @param arrowSchemaString The string representing arrow schema
     * @return a read Session with arrow schema (definitely does not have avro schema)
     */
    public static ReadSession fakeArrowReadSession(
            Integer expectedRowCount, Integer expectedReadStreamCount, String arrowSchemaString) {
        // set up the response for read session request
        List<ReadStream> readStreams =
                IntStream.range(0, expectedReadStreamCount)
                        .mapToObj(i -> ReadStream.newBuilder().setName("stream" + i).build())
                        .collect(Collectors.toList());
        return ReadSession.newBuilder()
                .addAllStreams(readStreams)
                .setEstimatedRowCount(expectedRowCount)
                .setArrowSchema(
                        ArrowSchema.newBuilder()
                                .setSerializedSchema(ByteString.copyFromUtf8(arrowSchemaString))
                                .build())
                // Set as ARROW.
                .setDataFormat(DataFormat.ARROW)
                .build();
    }

    public static List<GenericRecord> createRecordList(RecordGenerationParams params) {
        Schema schema = new Schema.Parser().parse(params.getAvroSchemaString());
        return IntStream.range(0, params.getRecordCount())
                .mapToObj(i -> createRecord(schema))
                .collect(Collectors.toList());
    }

    public static GenericRecord createRecord(Schema schema) {
        return (GenericRecord) new RandomData(schema, 0).iterator().next();
    }

    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

    @SuppressWarnings("deprecation")
    public static List<ReadRowsResponse> createResponse(
            String schemaString,
            List<GenericRecord> genericRecords,
            double progressAtResponseStart,
            double progressAtResponseEnd) {
        // BigQuery delivers the data in 1024 elements chunks, so we partition the generated list
        // into multiple ones with that size max.
        List<List<GenericRecord>> responsesData = Lists.partition(genericRecords, 1024);

        return responsesData.stream()
                // for each data response chunk we generate a read response object
                .map(
                        genRecords -> {
                            try {
                                Schema schema = new Schema.Parser().parse(schemaString);
                                GenericDatumWriter<GenericRecord> writer =
                                        new GenericDatumWriter<>(schema);
                                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                                Encoder binaryEncoder =
                                        ENCODER_FACTORY.binaryEncoder(outputStream, null);
                                for (GenericRecord genericRecord : genRecords) {
                                    writer.write(genericRecord, binaryEncoder);
                                }

                                binaryEncoder.flush();

                                return ReadRowsResponse.newBuilder()
                                        .setAvroRows(
                                                AvroRows.newBuilder()
                                                        .setSerializedBinaryRows(
                                                                ByteString.copyFrom(
                                                                        outputStream.toByteArray()))
                                                        .setRowCount(genRecords.size()))
                                        .setAvroSchema(
                                                AvroSchema.newBuilder()
                                                        .setSchema(schema.toString())
                                                        .build())
                                        .setRowCount(genRecords.size())
                                        .setStats(
                                                StreamStats.newBuilder()
                                                        .setProgress(
                                                                StreamStats.Progress.newBuilder()
                                                                        .setAtResponseStart(
                                                                                progressAtResponseStart)
                                                                        .setAtResponseEnd(
                                                                                progressAtResponseEnd)))
                                        .build();
                            } catch (Exception ex) {
                                throw new RuntimeException(
                                        "Problems generating faked response.", ex);
                            }
                        })
                .collect(Collectors.toList());
    }

    /**
     * Method to create a {@link ReadRowsResponse} that throws an error inplace of valid rows.
     *
     * @param schemaString the Schema String
     * @param genericRecords records
     * @param progressAtResponseStart Start time for progress logging
     * @param progressAtResponseEnd end time for progress logging
     * @return Always throws Runtime Exception.
     */
    @SuppressWarnings("deprecation")
    public static List<ReadRowsResponse> createInvalidResponse(
            String schemaString,
            List<GenericRecord> genericRecords,
            double progressAtResponseStart,
            double progressAtResponseEnd) {
        throw new RuntimeException("Problems generating faked response.");
    }

    /**
     * Method to create a {@link ReadRowsResponse} that do not contain Avro Schema, contain Arrow
     * schema instead.
     *
     * @param schemaString the Schema String
     * @param genericRecords records
     * @param progressAtResponseStart Start time for progress logging
     * @param progressAtResponseEnd end time for progress logging
     * @return List of rows without Avro Schema.
     */
    @SuppressWarnings("deprecation")
    public static List<ReadRowsResponse> createArrowResponse(
            String schemaString,
            List<GenericRecord> genericRecords,
            double progressAtResponseStart,
            double progressAtResponseEnd) {
        // BigQuery delivers the data in 1024 elements chunks, so we partition the generated list
        // into multiple ones with that size max.
        List<List<GenericRecord>> responsesData = Lists.partition(genericRecords, 1024);

        return responsesData.stream()
                // for each data response chunk we generate a read response object
                .map(
                        genRecords -> {
                            try {
                                Schema schema = new Schema.Parser().parse(schemaString);
                                GenericDatumWriter<GenericRecord> writer =
                                        new GenericDatumWriter<>(schema);
                                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                                Encoder binaryEncoder =
                                        ENCODER_FACTORY.binaryEncoder(outputStream, null);
                                for (GenericRecord genericRecord : genRecords) {
                                    writer.write(genericRecord, binaryEncoder);
                                }

                                binaryEncoder.flush();
                                // Removed the Avro Schema from the response.
                                return ReadRowsResponse.newBuilder()
                                        .setArrowRecordBatch(
                                                ArrowRecordBatch.newBuilder()
                                                        .setSerializedRecordBatch(
                                                                ByteString.copyFrom(
                                                                        outputStream.toByteArray()))
                                                        .build())
                                        .setArrowSchema(
                                                ArrowSchema.newBuilder()
                                                        .setSerializedSchema(
                                                                ByteString.copyFromUtf8(
                                                                        schemaString))
                                                        .build())
                                        .setRowCount(genRecords.size())
                                        .setStats(
                                                StreamStats.newBuilder()
                                                        .setProgress(
                                                                StreamStats.Progress.newBuilder()
                                                                        .setAtResponseStart(
                                                                                progressAtResponseStart)
                                                                        .setAtResponseEnd(
                                                                                progressAtResponseEnd)))
                                        .build();
                            } catch (Exception ex) {
                                throw new RuntimeException(
                                        "Problems generating faked response.", ex);
                            }
                        })
                .collect(Collectors.toList());
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString)
            throws IOException {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                StorageClientFaker::createRecordList);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return createReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () ->
                                                new FakeBigQueryServices(
                                                        new FakeBigQueryServices
                                                                .FakeBigQueryStorageReadClient(
                                                                StorageClientFaker.fakeReadSession(
                                                                        expectedRowCount,
                                                                        expectedReadStreamCount,
                                                                        avroSchemaString),
                                                                dataGenerator,
                                                                errorPercentage)))
                                .build())
                .build();
    }

    public static BigQueryReadOptions createInvalidQueryReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString)
            throws IOException {
        return createInvalidQueryReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                StorageClientFaker::createRecordList);
    }

    public static BigQueryReadOptions createInvalidQueryReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return createInvalidQueryReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D);
    }

    public static BigQueryReadOptions createInvalidQueryReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setQuery("Select * from invalid_table;")
                .setQueryExecutionProject("invalid_project")
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("invalid_project")
                                .setTable("invalid_table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () ->
                                                new FakeBigQueryErrorServices(
                                                        new FakeBigQueryErrorServices
                                                                .FakeBigQueryErrorStorageReadClient(
                                                                StorageClientFaker
                                                                        .fakeInvalidReadSession(
                                                                                expectedRowCount,
                                                                                expectedReadStreamCount,
                                                                                avroSchemaString),
                                                                dataGenerator,
                                                                errorPercentage)))
                                .build())
                .build();
    }

    public static BigQueryReadOptions createArrowReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String arrowSchemaString)
            throws IOException {
        return createArrowReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                arrowSchemaString,
                StorageClientFaker::createRecordList);
    }

    public static BigQueryReadOptions createArrowReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String arrowSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return createArrowReadOptions(
                expectedRowCount, expectedReadStreamCount, arrowSchemaString, dataGenerator, 0D);
    }

    public static BigQueryReadOptions createArrowReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String arrowSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () ->
                                                new FakeBigQueryErrorServices(
                                                        new FakeBigQueryErrorServices
                                                                .FakeBigQueryErrorStorageReadClient(
                                                                StorageClientFaker
                                                                        .fakeArrowReadSession(
                                                                                expectedRowCount,
                                                                                expectedReadStreamCount,
                                                                                arrowSchemaString),
                                                                dataGenerator,
                                                                errorPercentage)))
                                .build())
                .build();
    }
}
