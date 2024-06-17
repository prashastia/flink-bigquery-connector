package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test {@link BigQueryTableSchemaProvider}. */
public class BigQueryTableSchemaProviderTest {
    @Test
    public void testTableSchemaFromAvroSchema() {
        // Form the Avro Schema
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();

        // Convert to Data Type
        DataType dataType = BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema);
        DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("number", DataTypes.BIGINT().notNull()),
                                DataTypes.FIELD("price", DataTypes.DOUBLE().notNull()),
                                DataTypes.FIELD("species", DataTypes.STRING().notNull()),
                                DataTypes.FIELD("flighted", DataTypes.BOOLEAN().notNull()),
                                DataTypes.FIELD("sound", DataTypes.BYTES().notNull()),
                                DataTypes.FIELD(
                                        "required_record_field",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "species",
                                                                DataTypes.STRING().notNull()))
                                                .notNull()))
                        .notNull();
        assertEquals(expectedDataType, dataType);
    }
}
