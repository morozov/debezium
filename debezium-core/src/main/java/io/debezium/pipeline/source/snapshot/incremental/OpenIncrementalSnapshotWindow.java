/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.Partition;
import io.debezium.pipeline.signal.Signal;
import io.debezium.pipeline.signal.Signal.Payload;

public class OpenIncrementalSnapshotWindow implements Signal.Action {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenIncrementalSnapshotWindow.class);

    public static final String NAME = "snapshot-window-open";

    public OpenIncrementalSnapshotWindow() {
    }

    @Override
    public boolean arrived(Partition partition, Payload signalPayload) {
        signalPayload.offsetContext.getIncrementalSnapshotContext().openWindow(signalPayload.id);
        return true;
    }

}
