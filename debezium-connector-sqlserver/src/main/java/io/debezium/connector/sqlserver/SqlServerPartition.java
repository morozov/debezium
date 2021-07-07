/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.TASK_DATABASE_NAMES;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.Partition;

public class SqlServerPartition implements Partition {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerPartition.class);
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String serverName;
    private final String databaseName;

    public SqlServerPartition(String serverName, String databaseName) {
        this.serverName = serverName;
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        // partition components must be stored in a linked map because they are used to build JMX bean names
        Map<String, String> partition = new LinkedHashMap<>();
        partition.put(SERVER_PARTITION_KEY, serverName);
        partition.put(DATABASE_PARTITION_KEY, databaseName);

        return partition;
    }

    @Override
    public String toString() {
        return "{" +
                "server=" + serverName +
                ", database=" + databaseName +
                '}';
    }

    /**
     * Returns the SQL Server database name corresponding to the partition.
     */
    String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SqlServerPartition other = (SqlServerPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName, databaseName);
    }

    static class Provider implements Partition.Provider<SqlServerPartition> {
        private final SqlServerConnectorConfig connectorConfig;
        private final Configuration taskConfig;
        private final SqlServerConnection connection;

        Provider(SqlServerConnectorConfig connectorConfig, Configuration taskConfig, SqlServerConnection connection) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
            this.connection = connection;
        }

        @Override
        public Set<SqlServerPartition> getPartitions() {
            String serverName = connectorConfig.getLogicalName();

            String[] databaseNames = taskConfig.getString(TASK_DATABASE_NAMES.name()).split(",");

            return Arrays.stream(databaseNames)
                    .map(databaseName -> {
                        try {
                            return connection.retrieveRealDatabaseName(databaseName);
                        }
                        catch (RuntimeException e) {
                            LOGGER.warn("Couldn't obtain real name for database {}", databaseName);
                            return "";
                        }
                    })
                    .filter(realDatabaseName -> !realDatabaseName.isEmpty())
                    .map(realDatabaseName -> new SqlServerPartition(serverName, realDatabaseName))
                    .collect(Collectors.toSet());
        }
    }
}
