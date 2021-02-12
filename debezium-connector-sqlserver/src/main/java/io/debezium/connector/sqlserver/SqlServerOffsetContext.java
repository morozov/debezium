/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

public class SqlServerOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean snapshotCompleted;
    private final TransactionContext transactionContext;

    /**
     * The index of the current event within the current transaction.
     */
    private long eventSerialNo;

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, String database, TxLogPosition position,
                                  boolean snapshot, boolean snapshotCompleted, long eventSerialNo,
                                  TransactionContext transactionContext) {
        partition = new HashMap<>();
        partition.put(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        partition.put(DATABASE_PARTITION_KEY, database);

        sourceInfo = new SourceInfo(connectorConfig);

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.eventSerialNo = eventSerialNo;
        this.transactionContext = transactionContext;
    }

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, String database, TxLogPosition position, boolean snapshot, boolean snapshotCompleted) {
        this(connectorConfig, database, position, snapshot, snapshotCompleted, 1, new TransactionContext());
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(
                    SourceInfo.SNAPSHOT_KEY, true,
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        else {
            return transactionContext.store(Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(),
                    SourceInfo.CHANGE_LSN_KEY,
                    sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(),
                    SourceInfo.EVENT_SERIAL_NO_KEY, eventSerialNo));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn());
    }

    public long getEventSerialNo() {
        return eventSerialNo;
    }

    public void setChangePosition(TxLogPosition position, int eventCount) {
        if (getChangePosition().equals(position)) {
            eventSerialNo += eventCount;
        }
        else {
            eventSerialNo = eventCount;
        }
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfo.setEventSerialNo(eventSerialNo);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public static class Loader implements OffsetContext.Loader {

        private final SqlServerConnectorConfig connectorConfig;
        private final String[] databases;

        public Loader(SqlServerConnectorConfig connectorConfig, String[] databases) {
            this.connectorConfig = connectorConfig;
            this.databases = databases;
        }

        @Override
        public Collection<Map<String, ?>> getPartitions() {
            Map<String, String> prototype = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
            Map<String, String> partition;
            Collection<Map<String, ?>> partitions = new HashSet<>();

            for (String database : databases) {
                partition = new HashMap<>(prototype);
                partition.put(DATABASE_PARTITION_KEY, database);
                partitions.add(partition);
            }

            return partitions;
        }

        @Override
        public Map<Map<String, ?>, OffsetContext> load(Map<Map<String, ?>, Map<String, ?>> offsets) {
            return offsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                Map<String, ?> offset = entry.getKey();
                final Lsn changeLsn = Lsn.valueOf((String) offset.get(SourceInfo.CHANGE_LSN_KEY));
                final Lsn commitLsn = Lsn.valueOf((String) offset.get(SourceInfo.COMMIT_LSN_KEY));
                final String database = (String) offset.get(SourceInfo.DATABASE_NAME_KEY);
                boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
                boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

                // only introduced in 0.10.Beta1, so it might be not present when upgrading from earlier versions
                Long eventSerialNo = ((Long) offset.get(SourceInfo.EVENT_SERIAL_NO_KEY));
                if (eventSerialNo == null) {
                    eventSerialNo = Long.valueOf(0);
                }

                return new SqlServerOffsetContext(connectorConfig, database, TxLogPosition.valueOf(commitLsn, changeLsn), snapshot, snapshotCompleted, eventSerialNo,
                        TransactionContext.load(offset));
            }));
        }
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", partition=" + partition +
                ", snapshotCompleted=" + snapshotCompleted +
                ", eventSerialNo=" + eventSerialNo +
                "]";
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
