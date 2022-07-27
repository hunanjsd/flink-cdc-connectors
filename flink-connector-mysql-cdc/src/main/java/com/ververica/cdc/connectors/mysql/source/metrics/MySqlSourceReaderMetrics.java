/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    private final MetricGroup metricGroup;

    /**
     * The last record processing time, which is updated after {@link MySqlSourceReader} fetches a
     * batch of data. It's mainly used to report metrics sourceIdleTime for sourceIdleTime =
     * System.currentTimeMillis() - processTime.
     */
    private volatile long processTime = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private volatile long emitDelay = 0L;

    private volatile long sourceSplitTotalSize = 0L;

    private volatile long sourceSplitFinishedSize = 0L;

    private volatile long sourceCurrentBinlogFilePos;

    private volatile long sourceProcessBinlogFilePos;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge("currentFetchEventTimeLag", (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge("currentEmitEventTimeLag", (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge("sourceIdleTime", (Gauge<Long>) this::getIdleTime);
        metricGroup.gauge("sourceSplitSize", (Gauge<Long>) this::getSourceSplitTotalSize);
        metricGroup.gauge("sourceSplitFinishedSize", (Gauge<Long>) this::getSourceSplitFinishedSize);
        metricGroup.gauge("sourceCurrentBinlogFilePos", (Gauge<Long>) this::getSourceCurrentBinlogFilePos);
        metricGroup.gauge("sourceProcessBinlogFilePos", (Gauge<Long>) this::getSourceProcessBinlogFilePos);

    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public long getEmitDelay() {
        return emitDelay;
    }

    public long getIdleTime() {
        // no previous process time at the beginning, return 0 as idle time
        if (processTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - processTime;
    }


    public long getSourceSplitTotalSize() {
        return sourceSplitTotalSize;
    }

    public long getSourceSplitFinishedSize() {
        return sourceSplitFinishedSize;
    }

    public long getSourceCurrentBinlogFilePos() {
        return sourceCurrentBinlogFilePos;
    }

    public long getSourceProcessBinlogFilePos() {
        return sourceProcessBinlogFilePos;
    }

    public void recordProcessTime(long processTime) {
        this.processTime = processTime;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordEmitDelay(long emitDelay) {
        this.emitDelay = emitDelay;
    }

    public void recordSourceSplitTotalSize(long sourceSplitSize) {
        this.sourceSplitTotalSize = sourceSplitSize;
    }

    public void recordSourceSplitFinishedSize(long sourceSplitFinishedSize) {
        this.sourceSplitFinishedSize = sourceSplitFinishedSize;
    }


    public void recordSourceCurrentBinlogFilePos(long sourceCurrentBinlogFilePos) {
        this.sourceCurrentBinlogFilePos = sourceCurrentBinlogFilePos;
    }

    public void recordSourceProcessBinlogFilePos(long sourceProcessBinlogFilePos) {
        this.sourceProcessBinlogFilePos = sourceProcessBinlogFilePos;
    }
}
