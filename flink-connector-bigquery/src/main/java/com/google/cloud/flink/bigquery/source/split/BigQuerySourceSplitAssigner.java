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

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;

/** A base split assigner definition. */
@Internal
public abstract class BigQuerySourceSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceSplitAssigner.class);

    protected final BigQueryReadOptions readOptions;
    protected final Set<String> lastSeenPartitions;
    protected final Deque<String> remainingTableStreams;
    protected final List<String> alreadyProcessedTableStreams;
    protected final Deque<BigQuerySourceSplit> remainingSourceSplits;
    protected final Map<String, BigQuerySourceSplit> assignedSourceSplits;
    protected boolean initialized;

    public static BigQuerySourceSplitAssigner createBounded(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        return new BoundedSplitAssigner(readOptions, sourceEnumState);
    }

    public static BigQuerySourceSplitAssigner createUnbounded(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        return null;
    }

    BigQuerySourceSplitAssigner(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        this.readOptions = readOptions;
        this.lastSeenPartitions =
                new ConcurrentSkipListSet<>(sourceEnumState.getLastSeenPartitions());
        this.remainingTableStreams =
                new ConcurrentLinkedDeque<>(sourceEnumState.getRemaniningTableStreams());
        this.alreadyProcessedTableStreams = sourceEnumState.getCompletedTableStreams();
        this.remainingSourceSplits =
                new ConcurrentLinkedDeque<>(sourceEnumState.getRemainingSourceSplits());
        this.assignedSourceSplits =
                new ConcurrentHashMap<>(sourceEnumState.getAssignedSourceSplits());
        this.initialized = sourceEnumState.isInitialized();
    }

    public abstract void discoverSplits();

    public void openAndDiscoverSplits() {
        LOG.info("BigQuery source split assigner is opening.");
        if (!initialized) {
            discoverSplits();
            initialized = true;
        }
    }

    public void addSplitsBack(List<BigQuerySourceSplit> splits) {
        for (BigQuerySourceSplit split : splits) {
            remainingSourceSplits.add((BigQuerySourceSplit) split);
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSourceSplits.remove(split.splitId());
        }
    }

    public BigQuerySourceEnumState snapshotState(long checkpointId) {
        return new BigQuerySourceEnumState(
                Lists.newArrayList(lastSeenPartitions),
                Lists.newArrayList(remainingTableStreams),
                Lists.newArrayList(alreadyProcessedTableStreams),
                Lists.newArrayList(remainingSourceSplits),
                Maps.newHashMap(assignedSourceSplits),
                initialized);
    }

    public void close() {
        // so far not much to be done here
        LOG.info("BigQuery source split assigner is closed.");
    }

    public Optional<BigQuerySourceSplit> getNext() {
        if (!remainingSourceSplits.isEmpty()) {
            // return remaining splits firstly
            BigQuerySourceSplit split = remainingSourceSplits.poll();
            assignedSourceSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextStream = remainingTableStreams.poll();
            if (nextStream != null) {
                BigQuerySourceSplit split = new BigQuerySourceSplit(nextStream);
                remainingSourceSplits.add(split);
                alreadyProcessedTableStreams.add(nextStream);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    public abstract boolean noMoreSplits();
}
