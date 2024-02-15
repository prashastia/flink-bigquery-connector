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
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfo;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** */
public class BigQueryPartitionUtilsBaseTest {



    @Test
    public void testPartitionValueInteger() {
        Assertions.assertThat(
                        BigQueryPartitionUtilsBase.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.INT64))
                .isEqualTo("2023");
    }

    @Test
    public void testPartitionValueDate() {
        Assertions.assertThat(
                        BigQueryPartitionUtilsBase.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.DATE))
                .isEqualTo("'2023'");
    }

    @Test
    public void testPartitionValueDateTime() {
        Assertions.assertThat(
                        BigQueryPartitionUtilsBase.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.DATETIME))
                .isEqualTo("'2023'");
    }

    @Test
    public void testPartitionValueTimestamp() {
        Assertions.assertThat(
                        BigQueryPartitionUtilsBase.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.TIMESTAMP))
                .isEqualTo("'2023'");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionValueFailsWithOtherType() {
        Assertions.assertThat(
                        BigQueryPartitionUtilsBase.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.NUMERIC))
                .isEqualTo("'2023'");
    }

    @Test
    public void testRetrievePartitionColumnType() {
        StandardSQLTypeName retrieved =
                BigQueryPartitionUtilsBase.retrievePartitionColumnType(
                        StorageClientFaker.SIMPLE_BQ_TABLE_SCHEMA, "ts");

        Assertions.assertThat(retrieved).isEqualTo(StandardSQLTypeName.TIMESTAMP);
    }

    @Test(expected = IllegalStateException.class)
    public void testRetrieveTypeOfNonExistentColumn() {
        BigQueryPartitionUtilsBase.retrievePartitionColumnType(
                StorageClientFaker.SIMPLE_BQ_TABLE_SCHEMA, "address");
    }

    @Test
    public void testCheckPartitionCompletedHour() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023072804",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-07-28 05:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedHour() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023072804",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-07-28 04:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedDay() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "20230728",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.DAY,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-07-29 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedDay() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "20230728",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.DAY,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-07-28 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedMonth() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202307",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-08-01 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedMonth() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202303",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-03-31 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionNotCompletedMonthFebLeapYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202002",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2020-02-29 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2022",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.YEAR,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-01-02 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedLeapYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2020",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtilsBase.PartitionType.YEAR,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2020-12-31 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedIntRange() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023",
                        new TablePartitionInfo(
                                "intvalue",
                                BigQueryPartitionUtilsBase.PartitionType.INT_RANGE,
                                StandardSQLTypeName.INT64,
                                Instant.now()));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtilsBase.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtilsBase.PartitionStatus.COMPLETED);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoEmpty() {
        String expected = "dragon = verde";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.empty(), "dragon", "verde");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoInteger() {
        String expected = "dragon = 5";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.INT_RANGE,
                                        StandardSQLTypeName.INT64,
                                        Instant.now())),
                        "dragon",
                        "5");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateDay() {
        String expected = "dragon BETWEEN '2023-01-02' AND '2023-01-03'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.DAY,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-02");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateMonth() {
        String expected = "dragon BETWEEN '2023-01-01' AND '2023-02-01'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.MONTH,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateYear() {
        String expected = "dragon BETWEEN '2023-01-01' AND '2024-01-01'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.YEAR,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampHour() {
        String expected = "dragon BETWEEN '2023-01-01 03:00:00' AND '2023-01-01 04:00:00'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.HOUR,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampDay() {
        String expected = "dragon BETWEEN '2023-01-01 00:00:00' AND '2023-01-02 00:00:00'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.DAY,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampMonth() {
        String expected = "dragon BETWEEN '2023-01-01 00:00:00' AND '2023-02-01 00:00:00'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.MONTH,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampYear() {
        String expected = "dragon BETWEEN '2023-01-01 00:00:00' AND '2024-01-01 00:00:00'";
        String actual =
                BigQueryPartitionUtilsBase.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtilsBase.PartitionType.YEAR,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    private Instant tsStringToInstant(String ts) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz").parse(ts).toInstant();
        } catch (ParseException e) {
            throw new IllegalStateException(
                    "Invalid date format in test. This should never happen!");
        }
    }
}
