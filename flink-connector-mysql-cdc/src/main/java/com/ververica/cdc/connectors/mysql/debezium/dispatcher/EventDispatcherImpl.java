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

import com.ververica.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.BINLOG_FILENAME_OFFSET_KEY;
import static com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.BINLOG_POSITION_OFFSET_KEY;
import static com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.MySqlEventMetadataProvider.SERVER_ID_KEY;

/**
 * A subclass implementation of {@link EventDispatcher}.
 *
 * <pre>
 *  1. This class shares one {@link ChangeEventQueue} between multiple readers.
 *  2. This class override some methods for dispatching {@link HistoryRecord} directly,
 *     this is useful for downstream to deserialize the {@link HistoryRecord} back.
 * </pre>
 *
 * 疑问
 *
 * <pre>
 *  1. dispatch 何时被创建
 *  2. 数据写入到 queue, 如何被下游读取
 *  3. dispatch 和 Reader 什么关系
 * </pre>
 */
public class EventDispatcherImpl<T extends DataCollectionId> extends EventDispatcher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(EventDispatcherImpl.class);

    public static final String HISTORY_RECORD_FIELD = "historyRecord";
    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    /**
     * queue 存储的 DataChangeEvent 是具体的单条变更数据, 变更数据类型
     *
     * <pre>
     *     1. schema change 变更记录, 参考下面变量: dispatchSchemaChangeEvent
     *     2. watermark 数据, 参考下面的 getQueue() 函数
     *     3. snapshot 数据, 参考 {@link MySqlSnapshotSplitReadTask#createDataEventsForTable} 调用的是父类的 dispatchSnapshotEvent 函数
     *     4. binlog 订阅数据, 参考 {@link MySqlStreamingChangeEventSource} handle insert/update event 的时候，调用的是父类的 dispatchDataChangeEvent 函数
     * </pre>
     */
    private final ChangeEventQueue<DataChangeEvent> queue;

    private final HistorizedDatabaseSchema historizedSchema;
    /**
     * 对象使用场景:
     *
     * <pre>
     *     1. {@link EventDispatcher#dispatchDataChangeEvent(DataCollectionId, ChangeRecordEmitter)} 在 dispatch change data 前会被调用, 判断是否为订阅的目标表
     *     2. {@link EventDispatcherImpl#dispatchSchemaChangeEvent(DataCollectionId, SchemaChangeEventEmitter))} 在 dispatch schema change 前会被调用, 判断是否为订阅的目标表
     * </pre>
     */
    // table 过滤器, 包含 include table 的内容
    private final DataCollectionFilters.DataCollectionFilter<T> filter;

    private final CommonConnectorConfig connectorConfig;
    private final TopicSelector<T> topicSelector;
    private final Schema schemaChangeKeySchema;
    private final Schema schemaChangeValueSchema;

    public EventDispatcherImpl(
            CommonConnectorConfig connectorConfig,
            TopicSelector<T> topicSelector,
            DatabaseSchema<T> schema,
            ChangeEventQueue<DataChangeEvent> queue,
            DataCollectionFilters.DataCollectionFilter<T> filter, // include table 的配置项
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            SchemaNameAdjuster schemaNameAdjuster) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                schemaNameAdjuster);
        this.historizedSchema =
                schema instanceof HistorizedDatabaseSchema
                        ? (HistorizedDatabaseSchema<T>) schema
                        : null;
        this.filter = filter;
        this.queue = queue;
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.schemaChangeKeySchema =
                SchemaBuilder.struct()
                        .name(
                                schemaNameAdjuster.adjust(
                                        "io.debezium.connector."
                                                + connectorConfig.getConnectorName()
                                                + ".SchemaChangeKey"))
                        .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                        .build();
        this.schemaChangeValueSchema =
                SchemaBuilder.struct()
                        .name(
                                schemaNameAdjuster.adjust(
                                        "io.debezium.connector."
                                                + connectorConfig.getConnectorName()
                                                + ".SchemaChangeValue"))
                        .field(
                                HistoryRecord.Fields.SOURCE,
                                connectorConfig.getSourceInfoStructMaker().schema())
                        .field(HISTORY_RECORD_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
    }

    /** getQueue 函数目的是将当前 dispatch 中的 queue 在 SignalEventDispatch 进行共享 */
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public void dispatchSchemaChangeEvent(
            T dataCollectionId, SchemaChangeEventEmitter schemaChangeEventEmitter)
            throws InterruptedException {
        // 注意到 filter 的作用就是判断 change event 是否是自己订阅的表, 否则不管
        if (dataCollectionId != null && !filter.isIncluded(dataCollectionId)) {
            if (historizedSchema == null || historizedSchema.storeOnlyMonitoredTables()) {
                LOG.trace("Filtering schema change event for {}", dataCollectionId);
                return;
            }
        }
        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    @Override
    public void dispatchSchemaChangeEvent(
            Collection<T> dataCollectionIds, SchemaChangeEventEmitter schemaChangeEventEmitter)
            throws InterruptedException {
        boolean anyNonfilteredEvent = false;
        if (dataCollectionIds == null || dataCollectionIds.isEmpty()) {
            anyNonfilteredEvent = true;
        } else {
            for (T dataCollectionId : dataCollectionIds) {
                if (filter.isIncluded(dataCollectionId)) {
                    anyNonfilteredEvent = true;
                    break;
                }
            }
        }
        if (!anyNonfilteredEvent) {
            if (historizedSchema == null || historizedSchema.storeOnlyMonitoredTables()) {
                LOG.trace("Filtering schema change event for {}", dataCollectionIds);
                return;
            }
        }

        schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
    }

    /** A {@link SchemaChangeEventEmitter.Receiver} implementation for {@link SchemaChangeEvent}. */
    private final class SchemaChangeEventReceiver implements SchemaChangeEventEmitter.Receiver {

        private Struct schemaChangeRecordKey(SchemaChangeEvent event) {
            Struct result = new Struct(schemaChangeKeySchema);
            result.put(HistoryRecord.Fields.DATABASE_NAME, event.getDatabase());
            return result;
        }

        private Struct schemaChangeRecordValue(SchemaChangeEvent event) throws IOException {
            Struct sourceInfo = event.getSource();
            Map<String, Object> source = new HashMap<>();
            String fileName = sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY);
            Long pos = sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY);
            Long serverId = sourceInfo.getInt64(SERVER_ID_KEY);
            source.put(SERVER_ID_KEY, serverId);
            source.put(BINLOG_FILENAME_OFFSET_KEY, fileName);
            source.put(BINLOG_POSITION_OFFSET_KEY, pos);
            HistoryRecord historyRecord =
                    new HistoryRecord(
                            source,
                            event.getOffset(),
                            event.getDatabase(),
                            null,
                            event.getDdl(),
                            event.getTableChanges());
            String historyStr = DOCUMENT_WRITER.write(historyRecord.document());

            Struct value = new Struct(schemaChangeValueSchema);
            value.put(HistoryRecord.Fields.SOURCE, event.getSource());
            value.put(HISTORY_RECORD_FIELD, historyStr);
            return value;
        }

        @Override
        public void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException {
            historizedSchema.applySchemaChange(event);
            if (connectorConfig.isSchemaChangesHistoryEnabled()) {
                try {
                    final String topicName = topicSelector.getPrimaryTopic();
                    final Integer partition = 0;
                    final Struct key = schemaChangeRecordKey(event);
                    final Struct value = schemaChangeRecordValue(event);
                    final SourceRecord record =
                            new SourceRecord(
                                    event.getPartition(),
                                    event.getOffset(),
                                    topicName,
                                    partition,
                                    schemaChangeKeySchema,
                                    key,
                                    schemaChangeValueSchema,
                                    value);
                    queue.enqueue(new DataChangeEvent(record));
                } catch (IOException e) {
                    throw new IllegalStateException(
                            String.format("dispatch schema change event %s error ", event), e);
                }
            }
        }
    }
}
