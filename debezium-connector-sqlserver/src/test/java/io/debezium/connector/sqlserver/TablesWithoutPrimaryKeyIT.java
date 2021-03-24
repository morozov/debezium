/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of tables that do not have primary key
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class TablesWithoutPrimaryKeyIT extends AbstractConnectorTest {

    private static final String DDL_STATEMENTS = "CREATE TABLE t1 (pk INT UNIQUE, val INT);" +
            "CREATE TABLE t2 (pk INT UNIQUE, val INT UNIQUE);" +
            "CREATE TABLE t3 (pk INT, val INT);";

    private static final String DML_STATEMENTS = "INSERT INTO t1 VALUES (1,10);" +
            "INSERT INTO t2 VALUES (2,20);" +
            "INSERT INTO t3 VALUES (3,30);";

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        initializeConnectorTestFramework();

        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldProcessFromSnapshot() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(DDL_STATEMENTS + DML_STATEMENTS);

            TestHelper.enableTableCdc(connection, databaseName, "t1");
            TestHelper.enableTableCdc(connection, databaseName, "t2");
            TestHelper.enableTableCdc(connection, databaseName, "t3");
        });

        start(SqlServerConnector.class, TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.t[123]")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1 + 1 + 1;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordsCount * TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t1")).get(0).keySchema().field("pk")).isNotNull();
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t1")).get(0).keySchema().fields()).hasSize(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t2")).get(0).keySchema().field("pk")).isNotNull();
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t2")).get(0).keySchema().fields()).hasSize(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t3")).get(0).keySchema()).isNull();
        });
    }

    @Test
    public void shouldProcessFromStreaming() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE init (pk INT PRIMARY KEY);",
                    "INSERT INTO init VALUES (1);");
            TestHelper.enableTableCdc(connection, databaseName, "init");

            waitForDisabledCdc(connection, databaseName, "t1");
            waitForDisabledCdc(connection, databaseName, "t2");
            waitForDisabledCdc(connection, databaseName, "t3");
        });

        start(SqlServerConnector.class, TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build());
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());

        TestHelper.waitForStreamingStarted();
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            TestHelper.waitForMaxLsnAvailable(connection, databaseName);

            connection.execute(DDL_STATEMENTS);

            Testing.Print.enable();
            TestHelper.enableTableCdc(connection, databaseName, "t1");
            TestHelper.enableTableCdc(connection, databaseName, "t2");
            TestHelper.enableTableCdc(connection, databaseName, "t3");

            waitForEnabledCdc(connection, databaseName, "t1");
            waitForEnabledCdc(connection, databaseName, "t2");
            waitForEnabledCdc(connection, databaseName, "t3");

            connection.execute("INSERT INTO t1 VALUES (1,10);");
            connection.execute("INSERT INTO t2 VALUES (2,20);");
            connection.execute("INSERT INTO t3 VALUES (3,30);");

            TestHelper.waitForCdcRecord(connection, databaseName, "t1", rs -> rs.getInt("pk") == 1);
            TestHelper.waitForCdcRecord(connection, databaseName, "t2", rs -> rs.getInt("pk") == 2);
            TestHelper.waitForCdcRecord(connection, databaseName, "t3", rs -> rs.getInt("pk") == 3);

            final int expectedRecordsCount = 1 + 1 + 1;

            final SourceRecords records = consumeRecordsByTopic(expectedRecordsCount, 24);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t1")).get(0).keySchema().field("pk")).isNotNull();
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t1")).get(0).keySchema().fields()).hasSize(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t2")).get(0).keySchema().field("pk")).isNotNull();
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t2")).get(0).keySchema().fields()).hasSize(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "t3")).get(0).keySchema()).isNull();
        });
    }

    private void waitForEnabledCdc(SqlServerConnection connection, String databaseName, String tableName) throws SQLException, InterruptedException {
        Awaitility
                .await("CDC " + tableName)
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> TestHelper.isCdcEnabled(connection, databaseName, tableName));
    }

    private void waitForDisabledCdc(SqlServerConnection connection, String databaseName, String tableName) throws SQLException, InterruptedException {
        Awaitility
                .await("CDC " + tableName)
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> !TestHelper.isCdcEnabled(connection, databaseName, tableName));
    }
}
