/*
 *
 *  Copyright (c) 2022 [The original author]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package com.flipkart.varidhi.relayer;

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;

import javax.sql.DataSource;
import javax.validation.Validation;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by ashudeep.sharma on 10/10/16.
 */
public class InitializeRelayer {
    private static final String showDatabaseQuery = "SHOW DATABASES";
    private static final String showTablesQuery = "SHOW TABLES";
    private static final String relayerIndentifier = "@@RELAYER@@";
    private static final String createDatabaseQuery = "CREATE DATABASE ";
    private static final String dropTableQuery = "DROP TABLE ";
    private static String messagesTableName = "@@RELAYER@@_messages";
    private static String messagesMetadataTableName = "@@RELAYER@@_message_meta_data";
    private static String skippedIdsTableName = "@@RELAYER@@_skipped_ids";
    private static String sidelinedMessagesTableName = "@@RELAYER@@_sidelined_messages";
    private static ArrayList<String> tableNames;
    private static ArrayList<String> tableSql;

    private static String messagesTableSQL =
            "CREATE TABLE if not exists `@@RELAYER@@_messages` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "  `message_id` varchar(100) DEFAULT NULL,\n" + "  `message` mediumtext,\n"
                    + "  `created_at` timestamp NOT NULL DEFAULT '1970-01-01 12:00:00',\n"
                    + "  `exchange_name` varchar(100) DEFAULT NULL,\n"
                    + "  `exchange_type` varchar(20) DEFAULT 'queue',\n"
                    + "  `app_id` varchar(100) DEFAULT NULL,\n"
                    + "  `group_id` varchar(100) DEFAULT NULL,\n"
                    + "  `http_method` varchar(10) DEFAULT NULL,\n"
                    + "  `http_uri` varchar(4096) DEFAULT NULL,\n"
                    + "  `parent_txn_id` varchar(100) DEFAULT NULL,\n"
                    + "  `reply_to` varchar(100) DEFAULT NULL,\n"
                    + "  `reply_to_http_method` varchar(10) DEFAULT NULL,\n"
                    + "  `reply_to_http_uri` varchar(4096) DEFAULT NULL,\n" + "  `context` text,\n"
                    + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "  `custom_headers` text,\n" + "  `transaction_id` varchar(100) DEFAULT NULL,\n"
                    + "  `correlation_id` varchar(100) DEFAULT NULL,\n"
                    + "  `destination_response_status` int(11) DEFAULT NULL,\n" + "  PRIMARY KEY (`id`),\n"
                    + "  KEY `message_id` (`message_id`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1039 DEFAULT CHARSET=utf8mb4\n"
                    + "/*!50100 PARTITION BY RANGE (id)\n"
                    + "(PARTITION p1 VALUES LESS THAN (1000) ENGINE = InnoDB,\n"
                    + " PARTITION p2 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                    + " PARTITION p3 VALUES LESS THAN (3000) ENGINE = InnoDB,\n"
                    + " PARTITION p4 VALUES LESS THAN (4000) ENGINE = InnoDB,\n"
                    + " PARTITION p5 VALUES LESS THAN (5000) ENGINE = InnoDB,\n"
                    + " PARTITION p6 VALUES LESS THAN (6000) ENGINE = InnoDB,\n"
                    + " PARTITION p7 VALUES LESS THAN (7000) ENGINE = InnoDB,\n"
                    + " PARTITION p8 VALUES LESS THAN (8000) ENGINE = InnoDB,\n"
                    + " PARTITION p9 VALUES LESS THAN (9000) ENGINE = InnoDB,\n"
                    + " PARTITION p10 VALUES LESS THAN (10000) ENGINE = InnoDB,\n"
                    + " PARTITION p11 VALUES LESS THAN (11000) ENGINE = InnoDB,\n"
                    + " PARTITION p12 VALUES LESS THAN (12000) ENGINE = InnoDB,\n"
                    + " PARTITION p13 VALUES LESS THAN (13000) ENGINE = InnoDB,\n"
                    + " PARTITION p14 VALUES LESS THAN (14000) ENGINE = InnoDB,\n"
                    + " PARTITION p15 VALUES LESS THAN (15000) ENGINE = InnoDB,\n"
                    + " PARTITION p16 VALUES LESS THAN (16000) ENGINE = InnoDB,\n"
                    + " PARTITION p17 VALUES LESS THAN (17000) ENGINE = InnoDB,\n"
                    + " PARTITION p18 VALUES LESS THAN (18000) ENGINE = InnoDB,\n"
                    + " PARTITION p19 VALUES LESS THAN (19000) ENGINE = InnoDB,\n"
                    + " PARTITION p20 VALUES LESS THAN (20000) ENGINE = InnoDB,\n"
                    + " PARTITION p21 VALUES LESS THAN (21000) ENGINE = InnoDB,\n"
                    + " PARTITION p22 VALUES LESS THAN (22000) ENGINE = InnoDB,\n"
                    + " PARTITION p23 VALUES LESS THAN (23000) ENGINE = InnoDB,\n"
                    + " PARTITION p24 VALUES LESS THAN (24000) ENGINE = InnoDB,\n"
                    + " PARTITION p25 VALUES LESS THAN (25000) ENGINE = InnoDB,\n"
                    + " PARTITION p26 VALUES LESS THAN (26000) ENGINE = InnoDB,\n"
                    + " PARTITION p27 VALUES LESS THAN (27000) ENGINE = InnoDB,\n"
                    + " PARTITION p28 VALUES LESS THAN (28000) ENGINE = InnoDB,\n"
                    + " PARTITION p29 VALUES LESS THAN (29000) ENGINE = InnoDB,\n"
                    + " PARTITION p30 VALUES LESS THAN (30000) ENGINE = InnoDB) */";
    private static String messagesMetadataTableSQL =
            "CREATE TABLE if not exists `@@RELAYER@@_message_meta_data` (\n"
                    + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "  `message_id` varchar(100) DEFAULT NULL,\n"
                    + "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "  PRIMARY KEY (`id`),\n" + "  KEY `message_id` (`message_id`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1200 DEFAULT CHARSET=latin1\n"
                    + "/*!50100 PARTITION BY RANGE (id)\n"
                    + "(PARTITION p1 VALUES LESS THAN (1000) ENGINE = InnoDB,\n"
                    + " PARTITION p2 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                    + " PARTITION p3 VALUES LESS THAN (3000) ENGINE = InnoDB,\n"
                    + " PARTITION p4 VALUES LESS THAN (4000) ENGINE = InnoDB,\n"
                    + " PARTITION p5 VALUES LESS THAN (5000) ENGINE = InnoDB,\n"
                    + " PARTITION p6 VALUES LESS THAN (6000) ENGINE = InnoDB,\n"
                    + " PARTITION p7 VALUES LESS THAN (7000) ENGINE = InnoDB,\n"
                    + " PARTITION p8 VALUES LESS THAN (8000) ENGINE = InnoDB,\n"
                    + " PARTITION p9 VALUES LESS THAN (9000) ENGINE = InnoDB,\n"
                    + " PARTITION p10 VALUES LESS THAN (10000) ENGINE = InnoDB,\n"
                    + " PARTITION p11 VALUES LESS THAN (11000) ENGINE = InnoDB,\n"
                    + " PARTITION p12 VALUES LESS THAN (12000) ENGINE = InnoDB,\n"
                    + " PARTITION p13 VALUES LESS THAN (13000) ENGINE = InnoDB,\n"
                    + " PARTITION p14 VALUES LESS THAN (14000) ENGINE = InnoDB,\n"
                    + " PARTITION p15 VALUES LESS THAN (15000) ENGINE = InnoDB,\n"
                    + " PARTITION p16 VALUES LESS THAN (16000) ENGINE = InnoDB,\n"
                    + " PARTITION p17 VALUES LESS THAN (17000) ENGINE = InnoDB,\n"
                    + " PARTITION p18 VALUES LESS THAN (18000) ENGINE = InnoDB,\n"
                    + " PARTITION p19 VALUES LESS THAN (19000) ENGINE = InnoDB,\n"
                    + " PARTITION p20 VALUES LESS THAN (20000) ENGINE = InnoDB,\n"
                    + " PARTITION p21 VALUES LESS THAN (21000) ENGINE = InnoDB,\n"
                    + " PARTITION p22 VALUES LESS THAN (22000) ENGINE = InnoDB,\n"
                    + " PARTITION p23 VALUES LESS THAN (23000) ENGINE = InnoDB,\n"
                    + " PARTITION p24 VALUES LESS THAN (24000) ENGINE = InnoDB,\n"
                    + " PARTITION p25 VALUES LESS THAN (25000) ENGINE = InnoDB,\n"
                    + " PARTITION p26 VALUES LESS THAN (26000) ENGINE = InnoDB,\n"
                    + " PARTITION p27 VALUES LESS THAN (27000) ENGINE = InnoDB,\n"
                    + " PARTITION p28 VALUES LESS THAN (28000) ENGINE = InnoDB,\n"
                    + " PARTITION p29 VALUES LESS THAN (29000) ENGINE = InnoDB,\n"
                    + " PARTITION p30 VALUES LESS THAN (30000) ENGINE = InnoDB) */";
    private static String skippedIdsTableSQL =
            "CREATE TABLE if not exists `@@RELAYER@@_skipped_ids` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                    + "  `message_seq_id` int(11) NOT NULL DEFAULT '0',\n"
                    + "  `status` varchar(20) DEFAULT NULL,\n" + "  `retry_count` int(3) DEFAULT '0',\n"
                    + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "  `created_at` timestamp NOT NULL DEFAULT '1970-01-01 12:00:00',\n"
                    + "  PRIMARY KEY (`id`,`message_seq_id`),\n" + "  KEY `created_at` (`created_at`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=161697029 DEFAULT CHARSET=utf8mb4\n"
                    + "/*!50100 PARTITION BY RANGE (message_seq_id)\n"
                    + "(PARTITION p1 VALUES LESS THAN (1000) ENGINE = InnoDB,\n"
                    + " PARTITION p2 VALUES LESS THAN (2000) ENGINE = InnoDB,\n"
                    + " PARTITION p3 VALUES LESS THAN (3000) ENGINE = InnoDB,\n"
                    + " PARTITION p4 VALUES LESS THAN (4000) ENGINE = InnoDB,\n"
                    + " PARTITION p5 VALUES LESS THAN (5000) ENGINE = InnoDB,\n"
                    + " PARTITION p6 VALUES LESS THAN (6000) ENGINE = InnoDB,\n"
                    + " PARTITION p7 VALUES LESS THAN (7000) ENGINE = InnoDB,\n"
                    + " PARTITION p8 VALUES LESS THAN (8000) ENGINE = InnoDB,\n"
                    + " PARTITION p9 VALUES LESS THAN (9000) ENGINE = InnoDB,\n"
                    + " PARTITION p10 VALUES LESS THAN (10000) ENGINE = InnoDB,\n"
                    + " PARTITION p11 VALUES LESS THAN (11000) ENGINE = InnoDB,\n"
                    + " PARTITION p12 VALUES LESS THAN (12000) ENGINE = InnoDB,\n"
                    + " PARTITION p13 VALUES LESS THAN (13000) ENGINE = InnoDB,\n"
                    + " PARTITION p14 VALUES LESS THAN (14000) ENGINE = InnoDB,\n"
                    + " PARTITION p15 VALUES LESS THAN (15000) ENGINE = InnoDB,\n"
                    + " PARTITION p16 VALUES LESS THAN (16000) ENGINE = InnoDB,\n"
                    + " PARTITION p17 VALUES LESS THAN (17000) ENGINE = InnoDB,\n"
                    + " PARTITION p18 VALUES LESS THAN (18000) ENGINE = InnoDB,\n"
                    + " PARTITION p19 VALUES LESS THAN (19000) ENGINE = InnoDB,\n"
                    + " PARTITION p20 VALUES LESS THAN (20000) ENGINE = InnoDB,\n"
                    + " PARTITION p21 VALUES LESS THAN (21000) ENGINE = InnoDB,\n"
                    + " PARTITION p22 VALUES LESS THAN (22000) ENGINE = InnoDB,\n"
                    + " PARTITION p23 VALUES LESS THAN (23000) ENGINE = InnoDB,\n"
                    + " PARTITION p24 VALUES LESS THAN (24000) ENGINE = InnoDB,\n"
                    + " PARTITION p25 VALUES LESS THAN (25000) ENGINE = InnoDB,\n"
                    + " PARTITION p26 VALUES LESS THAN (26000) ENGINE = InnoDB,\n"
                    + " PARTITION p27 VALUES LESS THAN (27000) ENGINE = InnoDB,\n"
                    + " PARTITION p28 VALUES LESS THAN (28000) ENGINE = InnoDB,\n"
                    + " PARTITION p29 VALUES LESS THAN (29000) ENGINE = InnoDB,\n"
                    + " PARTITION p30 VALUES LESS THAN (30000) ENGINE = InnoDB) */";

    private static String sidelinedMessagesTableSQL =
            "CREATE TABLE if not exists `@@RELAYER@@_sidelined_messages` (\n"
                    + "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "  `group_id` varchar(100) DEFAULT NULL,\n"
                    + "  `message_id` varchar(100) DEFAULT NULL,\n"
                    + "  `status` varchar(20) DEFAULT NULL,\n"
                    + "  `created_at` timestamp NOT NULL DEFAULT '1970-01-01 12:00:00',\n"
                    + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "  `http_status_code` int(11) DEFAULT NULL,\n"
                    + "  `sideline_reason_code` varchar(255) DEFAULT NULL,\n"
                    + "  `retries` int(11) DEFAULT '0',\n" + "  `details` text,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=12785 DEFAULT CHARSET=utf8mb4";
    private static String relayerName = "";
    private static String relayerDatabase = "";
    private static String connectionURL = "";

    public static void setRelayerMetadata(String relayerTable, String relayerDB, String dbConnURL) {
        relayerName = relayerTable;
        relayerDatabase = relayerDB;
        connectionURL = dbConnURL;
    }

    public static DataSource dataSource() {
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setURL(connectionURL);
        return mysqlDataSource;

    }

    private static void populateTables() {
        tableNames = new ArrayList<>();
        tableSql = new ArrayList<>();

        messagesTableName = messagesTableName.replace(relayerIndentifier, relayerName);
        messagesTableSQL = messagesTableSQL.replace(relayerIndentifier, relayerName);
        messagesMetadataTableName =
                messagesMetadataTableName.replace(relayerIndentifier, relayerName);
        messagesMetadataTableSQL =
                messagesMetadataTableSQL.replace(relayerIndentifier, relayerName);
        skippedIdsTableName = skippedIdsTableName.replace(relayerIndentifier, relayerName);
        skippedIdsTableSQL = skippedIdsTableSQL.replace(relayerIndentifier, relayerName);
        sidelinedMessagesTableName =
                sidelinedMessagesTableName.replace(relayerIndentifier, relayerName);
        sidelinedMessagesTableSQL =
                sidelinedMessagesTableSQL.replace(relayerIndentifier, relayerName);

        tableNames.add(messagesTableName);
        tableNames.add(messagesMetadataTableName);
        tableNames.add(skippedIdsTableName);
        tableNames.add(sidelinedMessagesTableName);

        tableSql.add(messagesTableSQL);
        tableSql.add(messagesMetadataTableSQL);
        tableSql.add(skippedIdsTableSQL);
        tableSql.add(sidelinedMessagesTableSQL);
    }

    public static void setupRelayer() throws SQLException {

        Connection connection;
        ResultSet resultSet;
        //Replacing TableNames and Sql with the corresponding Relayer Identifier

        try {
            connection = dataSource().getConnection();
            connection.setAutoCommit(false);
            resultSet = connection.createStatement().executeQuery(showDatabaseQuery);
            ArrayList<String> databases = new ArrayList<>();
            while (resultSet.next()) {
                databases.add(resultSet.getString("Database"));
            }

            if (!databases.contains(relayerDatabase)) {
                connection.createStatement().execute(createDatabaseQuery + relayerDatabase);
            }
            connection.createStatement().execute("USE " + relayerDatabase);
            resultSet = connection.createStatement().executeQuery(showTablesQuery);

            ArrayList<String> tables = new ArrayList<>();

            while (resultSet.next()) {
                tables.add(resultSet.getString(1));
            }
            populateTables();
            for (int i = 0; i < tableNames.size(); i++) {
                initializeTables(connection, tableNames.get(i), tableSql.get(i), tables);
            }
            connection.commit();
        } catch (SQLException sqlException) {
            throw new SQLException("Error in Relayer Setup : " + sqlException.toString());
        }
    }

    private static void initializeTables(Connection connection, String tableName, String tableSQL,
                                         ArrayList<String> existingTables) throws SQLException {
        if (!existingTables.contains(tableName)) {
            connection.createStatement().execute(tableSQL);
        }
    }

    public static ApplicationConfiguration prepareConfiguration(String fileName) {
        ConfigurationFactory<ApplicationConfiguration> factory =
                new YamlConfigurationFactory<>(ApplicationConfiguration.class,
                        Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
                        "");
        try {
            System.out.println(new File(".").getAbsoluteFile());
            return factory.build(new File(fileName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeQueries(ArrayList<String> queryList) throws SQLException {

        Connection connection = null;
        try {
            connection = dataSource().getConnection();
            connection.setAutoCommit(false);
            connection.createStatement().execute("USE " + relayerDatabase);
            for (String query : queryList) {
                connection.createStatement().execute(query);
            }
            connection.commit();
        } catch (SQLException sqlException) {
            if (null != connection) {
                connection.rollback();
                connection.close();
            }
            throw new SQLException("Error While Executing Queries : " + sqlException.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void cleanup() throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource().getConnection();
            connection.createStatement().execute("USE " + relayerDatabase);
            for (String tableName : tableNames) {
                System.out.println("Dropping :" + tableName);
                connection.createStatement().execute(dropTableQuery + tableName);
            }
        } catch (SQLException sqlException) {
            if (null != connection) {
                connection.close();
            }
            throw new SQLException("Error during Cleanup : " + sqlException.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static ApplicationConfiguration getApplicationConfigurationFromResourceFile(String fileName) {
        ClassLoader classLoader = InitializeRelayer.class.getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return prepareConfiguration(file.getAbsolutePath());
    }
}
