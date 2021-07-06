/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.Partition;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.pipeline.spi.StreamingResult;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
@ThreadSafe
public class ChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceCoordinator.class);

    /**
     * Waiting period for the polling loop to finish. Will be applied twice, once gracefully, once forcefully.
     */
    public static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(90);

    private final Map<P, O> previousOffsets;
    private final ErrorHandler errorHandler;
    private final ChangeEventSourceFactory<P, O> changeEventSourceFactory;
    private final ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory;
    private final ExecutorService executor;
    private final EventDispatcher<P, O, ?> eventDispatcher;
    private final DatabaseSchema<?> schema;

    private volatile boolean running;
    private volatile StreamingChangeEventSource<P, O> streamingSource;
    private final ReentrantLock commitOffsetLock = new ReentrantLock();

    private SnapshotChangeEventSourceMetrics snapshotMetrics;
    private StreamingChangeEventSourceMetrics<P> streamingMetrics;
    private final Clock clock;
    private final Duration pollInterval;

    public ChangeEventSourceCoordinator(Map<P, O> previousOffsets, ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
                                        CommonConnectorConfig connectorConfig,
                                        ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                        ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory, EventDispatcher<P, O, ?> eventDispatcher,
                                        DatabaseSchema<?> schema, Clock clock) {
        this.previousOffsets = previousOffsets;
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.changeEventSourceMetricsFactory = changeEventSourceMetricsFactory;
        this.executor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "change-event-source-coordinator");
        this.eventDispatcher = eventDispatcher;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        this.clock = clock;
    }

    public synchronized void start(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider) {
        AtomicReference<LoggingContext.PreviousContext> previousLogContext = new AtomicReference<>();
        try {
            Collection<P> partitions = this.previousOffsets.keySet();

            this.snapshotMetrics = changeEventSourceMetricsFactory.getSnapshotMetrics(taskContext, changeEventQueueMetrics, metadataProvider, partitions);
            this.streamingMetrics = changeEventSourceMetricsFactory.getStreamingMetrics(taskContext, changeEventQueueMetrics, metadataProvider, partitions);
            running = true;

            // run the snapshot source on a separate thread so start() won't block
            executor.submit(() -> {
                try {
                    previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                    snapshotMetrics.register(LOGGER);
                    streamingMetrics.register(LOGGER);
                    LOGGER.info("Metrics registered");

                    ChangeEventSourceContext context = new ChangeEventSourceContextImpl();
                    LOGGER.info("Context created");

                    Map<P, SnapshotResult<O>> partitionState = new HashMap<>();

                    SnapshotChangeEventSource<P, O> snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource(snapshotMetrics);
                    for (Map.Entry<P, O> entry : previousOffsets.entrySet()) {
                        P partition = entry.getKey();
                        O previousOffset = entry.getValue();
                        CatchUpStreamingResult catchUpStreamingResult = executeCatchUpStreaming(context, snapshotSource, partition, previousOffset);
                        if (catchUpStreamingResult.performedCatchUpStreaming) {
                            streamingConnected(false);
                            commitOffsetLock.lock();
                            streamingSource = null;
                            commitOffsetLock.unlock();
                        }
                        eventDispatcher.setEventListener(snapshotMetrics);
                        SnapshotResult<O> snapshotResult = snapshotSource.execute(context, partition, previousOffset);
                        LOGGER.info("Snapshot ended with {}", snapshotResult);

                        if (snapshotResult.getStatus() == SnapshotResultStatus.COMPLETED || schema.tableInformationComplete()) {
                            schema.assureNonEmptySchema();
                        }
                        partitionState.put(partition, snapshotResult);
                    }

                    final Metronome metronome = Metronome.sleeper(pollInterval, clock);
                    Map<P, StreamingResult<O>> partitionStreamingResults = new HashMap<>();
                    while (running) {
                        for (Map.Entry<P, O> entry : previousOffsets.entrySet()) {
                            P partition = entry.getKey();
                            SnapshotResult<O> snapshotResult = partitionState.get(partition);
                            if (running && snapshotResult.isCompletedOrSkipped()) {
                                StreamingResult<O> previousStreamingResult = null;

                                if (!partitionStreamingResults.containsKey(entry.getKey())) {
                                    previousStreamingResult = partitionStreamingResults.get(partition);
                                }

                                StreamingResult<O> streamingResult = streamEvents(context, partition, snapshotResult.getOffset(), previousStreamingResult);
                                partitionStreamingResults.put(entry.getKey(), streamingResult);
                            }
                        }

                        boolean streamedEvents = false;
                        for (StreamingResult streamingResult : partitionStreamingResults.values()) {
                            if (streamingResult.eventsStreamed()) {
                                streamedEvents = true;
                                break;
                            }
                        }

                        if (!streamedEvents) {
                            metronome.pause();
                        }
                    }

                    LOGGER.info("Finished streaming");
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Change event source executor was interrupted", e);
                }
                catch (Throwable e) {
                    errorHandler.setProducerThrowable(e);
                }
                finally {
                    streamingConnected(false);
                }
            });
        }
        finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }

    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<P, O> snapshotSource,
                                                             P partition, O previousOffset)
            throws InterruptedException {
        return new CatchUpStreamingResult(false);
    }

    protected StreamingResult<O> streamEvents(ChangeEventSourceContext context, P partition, O offsetContext, StreamingResult<O> oStreamingResult)
            throws InterruptedException {
        if (streamingSource == null) {
            streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
            final Optional<IncrementalSnapshotChangeEventSource<P, O, ? extends DataCollectionId>> incrementalSnapshotChangeEventSource = changeEventSourceFactory
                    .getIncrementalSnapshotChangeEventSource(offsetContext, snapshotMetrics, snapshotMetrics);
            eventDispatcher.setIncrementalSnapshotChangeEventSource(incrementalSnapshotChangeEventSource);
            eventDispatcher.setEventListener(streamingMetrics);
            streamingConnected(true);
            LOGGER.info("Starting streaming");
            incrementalSnapshotChangeEventSource.ifPresent(x -> x.init(partition, offsetContext));
        }
        StreamingResult<O> streamingResult = streamingSource.execute(context, partition, offsetContext);
        return streamingResult;
    }

    public void commitOffset(Map<String, ?> offset) {
        if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
            streamingSource.commitOffset(offset);
        }
    }

    /**
     * Stops this coordinator.
     */
    public synchronized void stop() throws InterruptedException {
        running = false;

        try {
            // Clear interrupt flag so the graceful termination is always attempted
            Thread.interrupted();
            executor.shutdown();
            boolean isShutdown = executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (!isShutdown) {
                LOGGER.warn("Coordinator didn't stop in the expected time, shutting down executor now");

                // Clear interrupt flag so the forced termination is always attempted
                Thread.interrupted();
                executor.shutdownNow();
                executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        finally {
            snapshotMetrics.unregister(LOGGER);
            streamingMetrics.unregister(LOGGER);
        }
    }

    private class ChangeEventSourceContextImpl implements ChangeEventSourceContext {

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    private void streamingConnected(boolean status) {
        if (changeEventSourceMetricsFactory.connectionMetricHandledByCoordinator()) {
            streamingMetrics.connected(status);
        }
    }

    protected class CatchUpStreamingResult {

        public boolean performedCatchUpStreaming;

        public CatchUpStreamingResult(boolean performedCatchUpStreaming) {
            this.performedCatchUpStreaming = performedCatchUpStreaming;
        }

    }
}
