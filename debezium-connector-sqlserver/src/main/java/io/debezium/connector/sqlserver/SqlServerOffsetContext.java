/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
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
    private final Map<String, ?> partition;
    private boolean snapshotCompleted;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * The index of the current event within the current transaction.
     */
    private long eventSerialNo;

    Queue<SqlServerChangeTable> schemaChangeCheckpoints;

    private AtomicReference<SqlServerChangeTable[]> tablesSlot;
    private TxLogPosition lastProcessedPositionOnStart;
    private long lastProcessedEventSerialNoOnStart;
    private TxLogPosition lastProcessedPosition;
    private AtomicBoolean changesStoppedBeingMonotonic;
    boolean shouldIncreaseFromLsn = false;

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, Map<String, ?> partition, TxLogPosition position,
                                  boolean snapshot,
                                  boolean snapshotCompleted, long eventSerialNo, TransactionContext transactionContext,
                                  IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfoSchema = sourceInfo.schema();

        this.partition = partition;

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.eventSerialNo = eventSerialNo;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        this.schemaChangeCheckpoints = null;
        this.lastProcessedPositionOnStart = null;
        this.lastProcessedEventSerialNoOnStart = 0;
        this.lastProcessedPosition = null;
        this.changesStoppedBeingMonotonic = null;
        this.shouldIncreaseFromLsn = false;
    }

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, Map<String, ?> partition, TxLogPosition position,
                                  boolean snapshot, boolean snapshotCompleted) {
        this(connectorConfig, partition, position, snapshot, snapshotCompleted, 1, new TransactionContext(), new IncrementalSnapshotContext<>());
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
            return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(),
                    SourceInfo.CHANGE_LSN_KEY,
                    sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(),
                    SourceInfo.EVENT_SERIAL_NO_KEY, eventSerialNo)));
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

    public static class Loader implements OffsetContext.Loader<SqlServerOffsetContext> {

        private final SqlServerConnectorConfig connectorConfig;

        public Loader(SqlServerConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public SqlServerOffsetContext load(Map<String, ?> partition, Map<String, ?> offset) {
            final Lsn changeLsn = Lsn.valueOf((String) offset.get(SourceInfo.CHANGE_LSN_KEY));
            final Lsn commitLsn = Lsn.valueOf((String) offset.get(SourceInfo.COMMIT_LSN_KEY));
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            // only introduced in 0.10.Beta1, so it might be not present when upgrading from earlier versions
            Long eventSerialNo = ((Long) offset.get(SourceInfo.EVENT_SERIAL_NO_KEY));
            if (eventSerialNo == null) {
                eventSerialNo = Long.valueOf(0);
            }

            return new SqlServerOffsetContext(connectorConfig, partition, TxLogPosition.valueOf(commitLsn, changeLsn), snapshot, snapshotCompleted, eventSerialNo,
                    TransactionContext.load(offset), IncrementalSnapshotContext.load(offset, TableId.class));
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

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public void saveStreamingExecutionContext(Queue<SqlServerChangeTable> schemaChangeCheckpoints,
                                              AtomicReference<SqlServerChangeTable[]> tablesSlot,
                                              TxLogPosition lastProcessedPositionOnStart,
                                              long lastProcessedEventSerialNoOnStart,
                                              TxLogPosition lastProcessedPosition,
                                              AtomicBoolean changesStoppedBeingMonotonic,
                                              boolean shouldIncreaseFromLsn) {
        this.schemaChangeCheckpoints = schemaChangeCheckpoints;
        this.tablesSlot = tablesSlot;
        this.lastProcessedPositionOnStart = lastProcessedPositionOnStart;
        this.lastProcessedPosition = lastProcessedPosition;
        this.changesStoppedBeingMonotonic = changesStoppedBeingMonotonic;
        this.shouldIncreaseFromLsn = shouldIncreaseFromLsn;
    }

    public Queue<SqlServerChangeTable> getSchemaChangeCheckpoints() {
        return schemaChangeCheckpoints;
    }

    public AtomicReference<SqlServerChangeTable[]> getTablesSlot() {
        return tablesSlot;
    }

    public TxLogPosition getLastProcessedPositionOnStart() {
        return lastProcessedPositionOnStart;
    }

    public long getLastProcessedEventSerialNoOnStart() {
        return lastProcessedEventSerialNoOnStart;
    }

    public TxLogPosition getLastProcessedPosition() {
        return lastProcessedPosition;
    }

    public AtomicBoolean getChangesStoppedBeingMonotonic() {
        return changesStoppedBeingMonotonic;
    }

    public boolean getShouldIncreaseFromLsn() {
        return shouldIncreaseFromLsn;
    }

}
