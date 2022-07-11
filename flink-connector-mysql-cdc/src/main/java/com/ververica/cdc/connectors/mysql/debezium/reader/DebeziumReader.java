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

package com.ververica.cdc.connectors.mysql.debezium.reader;

import javax.annotation.Nullable;

import java.util.Iterator;

/**
 * 作用: 存活在 tm 的 MySqlSplitReader 中, 这应该是一个很底层的 debezium reader 为何放在 mysql-cdc 模块 Reader to read
 * split of table, the split is either snapshot split or binlog split.
 */
public interface DebeziumReader<T, Split> {

    /** Return the current split of the reader is finished or not. */
    boolean isFinished();

    /**
     * Add to split to read, this should call only the when reader is idle. 空闲的判断标志是上面 isFinished
     * 函数返回 true, 提交具体的 task, 例如 snapshot split、binlog read task 等等
     *
     * @param splitToRead
     */
    void submitSplit(Split splitToRead);

    /** Close the reader and releases all resources. */
    void close();

    /**
     * 都是从 EventDispatcherImpl 里面的 queue 拉取数据 Reads records from MySQL. The method should return
     * null when reaching the end of the split, the empty {@link Iterator} will be returned if the
     * data of split is on pulling.
     */
    @Nullable
    Iterator<T> pollSplitRecords() throws InterruptedException;
}
