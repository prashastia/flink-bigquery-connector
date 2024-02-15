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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;


import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfo;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utility class to handle the BigQuery partition conversions to Flink types and structures. */
@Internal
public class BigQueryPartitionUtils extends BigQueryPartitionUtilsBase {

    public static List<String> partitionValuesFromIdAndDataType(
            List<String> partitionIds, StandardSQLTypeName dataType) {
        List<String> partitionValues = new ArrayList<>();
        switch (dataType) {
            // integer range partition
            case INT64:
                // we add them as they are
                partitionValues.addAll(partitionIds);
                break;
            // time based partitioning (hour, date, month, year)
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                // lets first check that all the partition ids have the same length
                String firstId = partitionIds.get(0);
                Preconditions.checkState(
                        partitionIds.stream()
                                .allMatch(
                                        pid ->
                                                (pid.length() == firstId.length())
                                                        && StringUtils.isNumeric(pid)),
                        "Some elements in the partition id list have a different length: "
                                + partitionIds.toString());
                switch (firstId.length()) {
                    case 4:
                        // we have yearly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_YEAR_FORMAT, SQL_YEAR_FORMAT));
                        break;
                    case 6:
                        // we have monthly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_MONTH_FORMAT, SQL_MONTH_FORMAT));
                        break;
                    case 8:
                        // we have daily partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_DAY_FORMAT, SQL_DAY_FORMAT));
                        break;
                    case 10:
                        // we have hourly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_HOUR_FORMAT, SQL_HOUR_FORMAT));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "The lenght of the partition id is not one of the expected ones: "
                                        + firstId);
                }
                break;
            // non supported data types for partitions
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The provided SQL type name (%s) is not supported"
                                        + " as a partition column.",
                                dataType.name()));
        }
        return partitionValues;
    }
}

