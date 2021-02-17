/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SqlServerPartitionContext {
    private String[] databaseNames;
    private String serverName;

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    public SqlServerPartitionContext(SqlServerConnectorConfig connectorConfig, Configuration taskConfig) {
        serverName = connectorConfig.getLogicalName();

        // TODO: move "databases" to a constant, throw if the array is empty
        databaseNames = taskConfig.getString("databases", "").split(",");
    }

    public String[] getDatabaseNames() {
        return databaseNames;
    }

    public Collection<Map<String, String>> getPartitions() {
    }

    public Map<String, String> getPartitionForDatabase(String databaseName) {
        Map<String, String> partition = new HashMap<>();
        partition.put(SERVER_PARTITION_KEY, serverName);
        partition.put(DATABASE_PARTITION_KEY, databaseName);

        return partition;
    }

    public OffsetContext getOffsetForDatabase(String databaseName) {
    }
}
