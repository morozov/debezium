/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of database with and without case sensitive names.
 *
 * @author Jiri Pechanec
 */
public class CaseSensitivenessIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        connection = TestHelper.testConnection();

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
    @FixFor("DBZ-1051")
    public void caseInsensitiveDatabase() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE MyTableOne (Id int primary key, ColA varchar(30))",
                    "INSERT INTO MyTableOne VALUES(1, 'a')");
            TestHelper.enableTableCdc(connection, databaseName, "MyTableOne");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> testDatabase(databaseName, snapshotRecords));
    }

    @Test
    @FixFor("DBZ-1051")
    public void caseSensitiveDatabase() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    String.format("ALTER DATABASE %s COLLATE Latin1_General_BIN", databaseName),
                    "CREATE TABLE MyTableOne (Id int primary key, ColA varchar(30))",
                    "INSERT INTO MyTableOne VALUES(1, 'a')");
            TestHelper.enableTableCdc(connection, databaseName, "MyTableOne");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> testDatabase(databaseName, snapshotRecords));
    }

    private void testDatabase(String databaseName, SourceRecords snapshotRecords) throws Exception {
        connection.execute("USE " + databaseName);

        Assertions.assertThat(snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableOne"))).hasSize(1);
        SourceRecord snapshotRecord = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableOne")).get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) snapshotRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name(TestHelper.schemaName(databaseName, "MyTableOne", "Value"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColA", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) snapshotRecord.key(),
                SchemaBuilder.struct()
                        .name(TestHelper.schemaName(databaseName, "MyTableOne", "Key"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        Assertions.assertThat(((Struct) ((Struct) snapshotRecord.value()).get("after")).getInt32("Id")).isEqualTo(1);

        connection.execute("INSERT INTO MyTableOne VALUES(2, 'b')");
        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        Assertions.assertThat(streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableOne"))).hasSize(1);
        SourceRecord streamingRecord = streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableOne")).get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) streamingRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name(TestHelper.schemaName(databaseName, "MyTableOne", "Value"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColA", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) streamingRecord.key(),
                SchemaBuilder.struct()
                        .name(TestHelper.schemaName(databaseName, "MyTableOne", "Key"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        Assertions.assertThat(((Struct) ((Struct) streamingRecord.value()).get("after")).getInt32("Id")).isEqualTo(2);

        connection.execute(
                "CREATE TABLE MyTableTwo (Id int primary key, ColB varchar(30))");
        TestHelper.enableTableCdc(connection, databaseName, "MyTableTwo");
        connection.execute("INSERT INTO MyTableTwo VALUES(3, 'b')");
        streamingRecords = consumeRecordsByTopic(1);
        Assertions.assertThat(streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableTwo"))).hasSize(1);
        streamingRecord = streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "MyTableTwo")).get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) streamingRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name(TestHelper.schemaName(databaseName, "MyTableTwo", "Value"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColB", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) streamingRecord.key(),
                SchemaBuilder.struct()
                        .name(TestHelper.schemaName(databaseName, "MyTableTwo", "Key"))
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        Assertions.assertThat(((Struct) ((Struct) streamingRecord.value()).get("after")).getInt32("Id")).isEqualTo(3);
    }
}
