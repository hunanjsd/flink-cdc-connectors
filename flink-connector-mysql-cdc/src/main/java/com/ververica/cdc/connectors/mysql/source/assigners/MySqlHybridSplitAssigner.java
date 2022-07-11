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

package com.ververica.cdc.connectors.mysql.source.assigners;

import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isInitialAssigningFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isNewlyAddedAssigningFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isSuspended;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size and also continue with a binlog split. 分配策略:
 *
 * <pre>
 *   1. 分配 snapshot table split
 *   2. 分配 binlog split(只分配一次)
 * </pre>
 */
public class MySqlHybridSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlHybridSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final int splitMetaGroupSize;

    // 标记 binlog 是否已经分配
    private boolean isBinlogSplitAssigned;

    // 内部包含的一个 snapshot assigner 对象
    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive) {
        this(
                new MySqlSnapshotSplitAssigner(
                        sourceConfig, currentParallelism, remainingTables, isTableIdCaseSensitive),
                false,
                sourceConfig.getSplitMetaGroupSize());
    }

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint) {
        this(
                new MySqlSnapshotSplitAssigner(
                        sourceConfig, currentParallelism, checkpoint.getSnapshotPendingSplits()),
                checkpoint.isBinlogSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize());
    }

    private MySqlHybridSplitAssigner(
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            boolean isBinlogSplitAssigned,
            int splitMetaGroupSize) {
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
    }

    @Override
    public void open() {
        // 先初始化 snapshot 数据
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (isSuspended(getAssignerStatus())) {
            // do not assign split until the assigner received SuspendBinlogReaderAckEvent
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            // snapshot table 和 table split 都执行完了, 说明 snapshot 初始化结束了
            // binlog split assigning
            if (isBinlogSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (isInitialAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isBinlogSplitAssigned = true;
                return Optional.of(createBinlogSplit());
            } else if (isNewlyAddedAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create binlog, but send event to wake up the binlog reader
                isBinlogSplitAssigned = true;
                return Optional.empty();
            } else {
                // binlog split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create binlog split later
                isBinlogSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public void suspend() {
        snapshotSplitAssigner.suspend();
    }

    @Override
    public void wakeup() {
        snapshotSplitAssigner.wakeup();
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    /** snapshot 初始化完成后, 创建 binlog split 对象 */
    private MySqlBinlogSplit createBinlogSplit() {
        // 根据 split 进行排序
        final List<MySqlSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());

        // 获取所有已经完成且上报 enumerator 且 ack 的 binlog offset
        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        BinlogOffset minBinlogOffset = null;
        for (MySqlSnapshotSplit split : assignedSnapshotSplit) {
            // find the min binlog offset
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them

        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        // 从最小的 binlog 开始拉数据
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? BinlogOffset.INITIAL_OFFSET : minBinlogOffset,
                BinlogOffset.NO_STOPPING_OFFSET,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
