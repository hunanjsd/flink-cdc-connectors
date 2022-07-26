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
public class MySqlSourceSyncMonitorMetrics {

    private final MetricGroup metricGroup;

    private volatile long sourceSplitTotalSize = 0L;

    private volatile long sourceSplitFinishedSize = 0L;

    public MySqlSourceSyncMonitorMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge("sourceSplitSize", (Gauge<Long>) this::getSourceSplitTotalSize);
        metricGroup.gauge(
                "sourceSplitFinishedSize", (Gauge<Long>) this::getSourceSplitFinishedSize);
    }

    public void recordSourceSplitTotalSize(long sourceSplitSize) {
        this.sourceSplitTotalSize = sourceSplitSize;
    }

    public void recordSourceSplitFinishedSize(long sourceSplitFinishedSize) {
        this.sourceSplitFinishedSize = sourceSplitFinishedSize;
    }

    public long getSourceSplitTotalSize() {
        return sourceSplitTotalSize;
    }

    public long getSourceSplitFinishedSize() {
        return sourceSplitFinishedSize;
    }
}
