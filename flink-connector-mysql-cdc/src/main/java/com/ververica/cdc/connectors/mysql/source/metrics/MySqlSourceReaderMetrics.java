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

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
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

    private volatile long sourceSplitTotalSize = 1L;
    private volatile long sourceSplitFinishedSize = 0L;

    private volatile BinlogOffset sourceCurrentBinlogOffset = BinlogOffset.INITIAL_OFFSET;

    private volatile BinlogOffset taskProcessedBinlogOffset = BinlogOffset.INITIAL_OFFSET;
    private volatile long sourceMaxBinlogSize = 0L;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge("currentFetchEventTimeLag", (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge("currentEmitEventTimeLag", (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge("sourceIdleTime", (Gauge<Long>) this::getIdleTime);

        // source snapshot 阶段监控 metrics
        metricGroup.gauge("sourceSplitTotalSize", (Gauge<Long>) this::getSourceSplitTotalSize);
        metricGroup.gauge(
                "sourceSplitFinishedSize", (Gauge<Long>) this::getSourceSplitFinishedSize);
        metricGroup.gauge(
                "sourceSnapshotCompletionRate",
                (Gauge<Double>) this::getSourceSnapshotCompletionRate);

        // source binlog 同步阶段监控 metrics
        metricGroup.gauge(
                "sourceCurrentBinlogFileSerialNum",
                (Gauge<Long>) this::getSourceCurrentBinlogFileSerialNum);
        metricGroup.gauge(
                "sourceCurrentBinlogFilePos", (Gauge<Long>) this::getSourceCurrentBinlogFilePos);
        metricGroup.gauge(
                "taskProcessedBinlogFileSerialNum",
                (Gauge<Long>) this::getTaskProcessedBinlogFileSerialNum);
        metricGroup.gauge(
                "taskProcessedBinlogFilePos", (Gauge<Long>) this::getTaskProcessedBinlogFilePos);
        metricGroup.gauge("sourceBinlogSyncLag", (Gauge<Long>) this::getSourceBinlogSyncLag);
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
        return this.sourceCurrentBinlogOffset.getPosition();
    }

    public long getSourceCurrentBinlogFileSerialNum() {
        return this.sourceCurrentBinlogOffset.getFilenameSerialNum();
    }

    public long getTaskProcessedBinlogFileSerialNum() {
        return this.taskProcessedBinlogOffset.getFilenameSerialNum();
    }

    public long getTaskProcessedBinlogFilePos() {
        return this.taskProcessedBinlogOffset.getPosition();
    }

    public long getSourceBinlogSyncLag() {
        return (getSourceCurrentBinlogFileSerialNum() - getTaskProcessedBinlogFileSerialNum())
                        * sourceMaxBinlogSize
                + (getSourceCurrentBinlogFilePos() - getTaskProcessedBinlogFilePos());
    }

    public Double getSourceSnapshotCompletionRate() {
        return (this.sourceSplitFinishedSize * 1.0) / (this.sourceSplitTotalSize * 1.0);
    }

    public BinlogOffset getSourceCurrentBinlogOffset() {
        return sourceCurrentBinlogOffset;
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

    public void recordSourceCurrentBinlogOffset(BinlogOffset sourceCurrentBinlogOffset) {
        this.sourceCurrentBinlogOffset = sourceCurrentBinlogOffset;
    }

    public void recordTaskProcessedBinlogOffset(BinlogOffset processedBinlogOffset) {
        this.taskProcessedBinlogOffset = processedBinlogOffset;
    }

    public void recordMaxBinlogSize(long sourceMaxBinlogSize) {
        this.sourceMaxBinlogSize = sourceMaxBinlogSize;
    }

    @Override
    public String toString() {
        return "MySqlSourceReaderMetrics{"
                + "metricGroup="
                + metricGroup
                + ", processTime="
                + processTime
                + ", fetchDelay="
                + fetchDelay
                + ", emitDelay="
                + emitDelay
                + ", sourceSplitTotalSize="
                + sourceSplitTotalSize
                + ", sourceSplitFinishedSize="
                + sourceSplitFinishedSize
                + ", sourceCurrentBinlogFileSerialNum="
                + ", sourceCurrentBinlogFilePos="
                + ", taskProcessedBinlogFileSerialNum="
                + ", taskProcessedBinlogFilePos="
                + ", sourceMaxBinlogSize="
                + sourceMaxBinlogSize
                + ", sourceSnapshotCompletionRate="
                + getSourceSnapshotCompletionRate()
                + ", sourceBinlogSyncLag="
                + getSourceBinlogSyncLag()
                + '}';
    }
}
