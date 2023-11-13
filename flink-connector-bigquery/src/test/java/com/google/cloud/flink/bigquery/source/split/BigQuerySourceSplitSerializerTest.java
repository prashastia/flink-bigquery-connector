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

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceSplitSerializerTest {

    @Test
    public void testSplitSerializer() throws IOException {
        BigQuerySourceSplit split = new BigQuerySourceSplit("some stream name", 10L);

        byte[] serialized = BigQuerySourceSplitSerializer.INSTANCE.serialize(split);

        BigQuerySourceSplit reformedSplit =
                BigQuerySourceSplitSerializer.INSTANCE.deserialize(
                        BigQuerySourceSplitSerializer.VERSION, serialized);

        assertThat(split).isEqualTo(reformedSplit);
    }

    /**
     * Test to check serialisation and deserialization of {@link BigQuerySourceSplit} object via
     * File Input and Output streams.
     *
     * @throws IOException when serialise/deserialize fails
     */
    @Test
    public void testSerializeDeserializeBigQuerySourceSplit() throws IOException {
        // Create a split
        BigQuerySourceSplit split = new BigQuerySourceSplit("mySplit", 10L);
        String filename = "serialisedBigQuerySourceSplit.txt";
        Path filepath = Paths.get(filename);
        // Create a Output stream
        DataOutputStream dataOut = new DataOutputStream(Files.newOutputStream(filepath));
        // Serialise the object
        BigQuerySourceSplitSerializer.INSTANCE.serializeBigQuerySourceSplit(dataOut, split);
        // Now open the DataInputStream and check if the same object is returned
        DataInputStream dataIn = new DataInputStream(Files.newInputStream(filepath));
        BigQuerySourceSplit reformedSplit =
                BigQuerySourceSplitSerializer.INSTANCE.deserializeBigQuerySourceSplit(0, dataIn);
        assertThat(reformedSplit).isEqualTo(split);
        // Also check incompatible version handling
        IOException ex =
                assertThrows(
                        java.io.IOException.class,
                        () ->
                                BigQuerySourceSplitSerializer.INSTANCE
                                        .deserializeBigQuerySourceSplit(1000, dataIn));
        // The argument that caused the problem is the version.
        assertThat(ex.getMessage()).contains("Unknown version");
        // The file hence created is deleted
        File newCreatedFile = new File(filename);
        newCreatedFile.delete();
    }

    /**
     * test to check handling of invalid Serialisation Option. Also checks the error message.
     *
     * @throws IOException when serialise/deserialize fails
     */
    @Test
    public void testWrongSerializerVersion() throws IOException {
        BigQuerySourceSplit split = new BigQuerySourceSplit("some stream name", 10L);
        byte[] serialized = BigQuerySourceSplitSerializer.INSTANCE.serialize(split);
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> BigQuerySourceSplitSerializer.INSTANCE.deserialize(1000, serialized));
        // The argument that caused the problem is the version.
        assertThat(ex.getMessage()).contains("serializer version");
    }
}
