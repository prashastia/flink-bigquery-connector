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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.cloud.bigquery.StandardSQLTypeName;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** */
public class BigQueryPartitionUtilsTest {
    @Test
    public void testPartitionHour() {
        List<String> partitionIds = Arrays.asList("2023062822", "2023062823");
        // ISO formatted dates as single quote string literals at the beginning of the hour.
        List<String> expectedValues = Arrays.asList("2023-06-28 22:00:00", "2023-06-28 23:00:00");
        List<String> values =
                BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.TIMESTAMP);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionDay() {
        List<String> partitionIds = Arrays.asList("20230628", "20230628");
        // ISO formatted dates as single quote string literals.
        List<String> expectedValues = Arrays.asList("2023-06-28", "2023-06-28");
        List<String> values =
                BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.DATETIME);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionMonth() {
        List<String> partitionIds = Arrays.asList("202306", "202307");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023-06-01", "2023-07-01");
        List<String> values =
                BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.DATE);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionYear() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023-01-01", "2022-01-01");
        List<String> values =
                BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.TIMESTAMP);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionInteger() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023", "2022");
        List<String> values =
                BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.INT64);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTemporalPartition() {
        List<String> partitionIds = Arrays.asList("202308101112");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.TIMESTAMP);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongArrayPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.ARRAY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongStructPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.STRUCT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongJsonPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.JSON);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongGeoPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.GEOGRAPHY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBigNumPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BIGNUMERIC);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBoolPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BOOL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBytesPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongFloatPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.FLOAT64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongStringPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTimePartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.TIME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongIntervalPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.INTERVAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongNumericPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtilsBase.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.NUMERIC);
    }
}
