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

package com.ververica.cdc.connectors.mysql.debezium.dispatcher;

import com.github.shyiko.mysql.binlog.event.Event;
import com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/**
 * A dispatcher to dispatch watermark signal events.
 *
 * <p>The watermark signal event is used to describe the start point and end point of a split scan.
 * The Watermark Signal Algorithm is inspired by https://arxiv.org/pdf/2010.12597v1.pdf.
 */
public class SignalEventDispatcher {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "table";
    public static final String WATERMARK_SIGNAL = "_split_watermark_signal_";
    public static final String SPLIT_ID_KEY = "split_id";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String WATERMARK_KIND = "watermark_kind";
    public static final String SIGNAL_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.key";
    public static final String SIGNAL_EVENT_VALUE_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.value";

    private final Schema signalEventKeySchema;
    private final Schema signalEventValueSchema;
    // sourcePartition 在这里的目的是什么?
    private final Map<String, ?> sourcePartition;
    // topic 在这里的目的是什么?
    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public SignalEventDispatcher(
            Map<String, ?> sourcePartition, String topic, ChangeEventQueue<DataChangeEvent> queue) {
        this.sourcePartition = sourcePartition;
        this.topic = topic;
        this.queue = queue;
        // 要构建 watermark 的 SourceRecord, 所以需要定义 key 和 value 的 schema 信息
        this.signalEventKeySchema =
                SchemaBuilder.struct()
                        .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_KEY_SCHEMA_NAME))
                        .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                        .field(WATERMARK_SIGNAL, Schema.BOOLEAN_SCHEMA)
                        .build();
        this.signalEventValueSchema =
                SchemaBuilder.struct()
                        .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_VALUE_SCHEMA_NAME))
                        .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                        .field(WATERMARK_KIND, Schema.STRING_SCHEMA)
                        .build();
    }

    /**
     * 调用情况:
     *
     * <pre>
     * 1. {@link MySqlSnapshotSplitReadTask#doExecute} 在执行 snapshot split 前后会发送 low、high 水位信息
     * 2. {@link SnapshotSplitReader#dispatchBinlogEndEvent(MySqlBinlogSplit)} 在 snapshot 完成后, 订阅步骤一的
     * low -> high 之间的 binlog 消息, 如果 low=high 直接 dispatch 数据
     * 3. {@link MySqlBinlogSplitReadTask#handleEvent(Event)} 在处理 event 数据时判断是否达到最大的 binlog 订阅位置，如果达到, 停止 debezium 继续订阅
     * </pre>
     */
    public void dispatchWatermarkEvent(
            MySqlSplit mySqlSplit, BinlogOffset watermark, WatermarkKind watermarkKind)
            throws InterruptedException {

        SourceRecord sourceRecord =
                new SourceRecord(
                        sourcePartition,
                        watermark.getOffset(),
                        topic,
                        signalEventKeySchema,
                        signalRecordKey(mySqlSplit.splitId()),
                        signalEventValueSchema,
                        signalRecordValue(mySqlSplit.splitId(), watermarkKind));
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    private Struct signalRecordKey(String splitId) {
        Struct result = new Struct(signalEventKeySchema);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_SIGNAL, true);
        return result;
    }

    private Struct signalRecordValue(String splitId, WatermarkKind watermarkKind) {
        Struct result = new Struct(signalEventValueSchema);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_KIND, watermarkKind.toString());
        return result;
    }

    /** The watermark kind. */
    public enum WatermarkKind {
        LOW,
        HIGH,
        BINLOG_END;

        public WatermarkKind fromString(String kindString) {
            if (LOW.name().equalsIgnoreCase(kindString)) {
                return LOW;
            } else if (HIGH.name().equalsIgnoreCase(kindString)) {
                return HIGH;
            } else {
                return BINLOG_END;
            }
        }
    }
}
