/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;

import io.debezium.connector.common.Partition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * A Contract t
 * 
 * @author Jiri Pechanec
 *
 * @param <T> data collection id class
 */
public interface IncrementalSnapshotChangeEventSource<P extends Partition, O extends OffsetContext, T extends DataCollectionId> {

    void closeWindow(String id, EventDispatcher<P, O, T> dispatcher, P partition, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext);

    void init(P partition, OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(P partition, List<String> dataCollectionIds, OffsetContext offsetContext)
            throws InterruptedException;
}