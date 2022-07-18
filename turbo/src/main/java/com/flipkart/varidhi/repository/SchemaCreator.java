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

package com.flipkart.varidhi.repository;

import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider.TableType;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SchemaCreator {

    private static final Logger logger = LoggerFactory.getLogger(SchemaCreator.class);
    private final Supplier<Session> sessionSupplier;
    private final ExchangeTableNameProvider exchangeTableNameProvider;
    private final Long partitionSize;
    private final Integer noOfPartitions;
    private final PartitionMode partitionMode;

    public SchemaCreator(Supplier<Session> sessionSupplier,
                         ExchangeTableNameProvider exchangeTableNameProvider, long partitionSize, int noOfPartitions, PartitionMode partitionMode) {
        this.sessionSupplier = sessionSupplier;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.partitionSize = partitionSize;
        this.noOfPartitions = noOfPartitions;
        this.partitionMode = partitionMode;
    }

    public SchemaCreator(Supplier<Session> sessionSupplier,
                         ExchangeTableNameProvider exchangeTableNameProvider) {
        this.sessionSupplier = sessionSupplier;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.partitionSize = null;
        this.noOfPartitions = null;
        this.partitionMode = null;
    }

    public void ensureAppSchema() {
        Session session = null;
        Transaction transaction = null;
        try {
            session = sessionSupplier.get();
            transaction = session.beginTransaction();

            Query messageMetaDataQuery = session.createSQLQuery(generateMessageMetaDataTableScript("id", partitionSize, noOfPartitions));
            messageMetaDataQuery.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.warn("Unable to create app side table " + e.getMessage(), e.getCause()!= null ? e.getCause().getMessage() : e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    public void ensureOutboundSchema() {
        Session session = null;
        Transaction transaction = null;
        try {
            session = sessionSupplier.get();
            transaction = session.beginTransaction();

            Query maxProcessedMessageSeqQuery = session.createSQLQuery(
                    "CREATE TABLE if not exists " + exchangeTableNameProvider
                            .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE) + "(" +
                            "  `id` int(11) NOT NULL AUTO_INCREMENT," +
                            "  `process_id` varchar(100) NOT NULL," +
                            "  `message_id` varchar(100) DEFAULT NULL," +
                            "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                            +
                            "  `active` tinyint(1) DEFAULT '1'," +
                            "  PRIMARY KEY (`id`)" +
                            ")");// MAX_PROCESSED_MESSAGE_SEQUENCE

            Query controlTasksQuery = session.createSQLQuery(
                    "CREATE TABLE if not exists " + exchangeTableNameProvider
                            .getTableName(TableType.CONTROL_TASKS) + " (" +
                            "  `id` int(11) NOT NULL AUTO_INCREMENT," +
                            "  `task_type` varchar(100) NOT NULL," +
                            "  `group_id` varchar(100) DEFAULT NULL," +
                            "  `message_id` varchar(100) DEFAULT NULL," +
                            "  `status` varchar(20) DEFAULT NULL," +
                            "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `from_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `to_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  PRIMARY KEY (`id`), " +
                            "KEY `status` (`status`) " +
                            ")");

            Query sidelinedGroupsQuery = session.createSQLQuery(
                    "CREATE TABLE if not exists " + exchangeTableNameProvider
                            .getTableName(TableType.SIDELINED_GROUPS) + "(" +
                            "  `id` int(11) NOT NULL AUTO_INCREMENT," +
                            "  `group_id` varchar(100) DEFAULT NULL," +
                            "  `status` varchar(20) DEFAULT NULL," +
                            "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                            "  PRIMARY KEY (`id`), " +
                            "KEY `group_id` (`group_id`) " +
                            ")");

            Query sidelinedMessagesQuery = session.createSQLQuery(
                    "CREATE TABLE if not exists " + exchangeTableNameProvider
                            .getTableName(TableType.SIDELINED_MESSAGES) + " (" +
                            "  `id` int(11) NOT NULL AUTO_INCREMENT," +
                            "  `group_id` varchar(100) DEFAULT NULL," +
                            "  `message_id` varchar(100) DEFAULT NULL," +
                            "  `status` varchar(20) DEFAULT NULL," +
                            "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                            "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                            "  `http_status_code` int(11) DEFAULT NULL," +
                            "  `sideline_reason_code` varchar(255) DEFAULT NULL," +
                            "  `retries` int(11) DEFAULT '0'," +
                            "  `details` text," +
                            "  PRIMARY KEY (`id`), " +
                            "KEY `group_id` (`group_id`)," +
                            "KEY `message_id` (`message_id`)," +
                            "KEY `status` (`status`)" +
                            ")");

            Query leaderElectionQuery = session.createSQLQuery(
                    "CREATE TABLE if not exists " + exchangeTableNameProvider
                            .getTableName(TableType.LEADER_ELECTION) + " (" +
                            "  anchor tinyint(3) unsigned NOT NULL,\n" +
                            "  relayer_uid varchar(255) NOT NULL,\n" +
                            "  lease_expiry_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                            "  PRIMARY KEY (anchor)\n" +
                            ") ");


            Query skippedIdsQuery = session.createSQLQuery(
                    generateSkippedIdsTableScript("id", partitionSize, noOfPartitions));

            Query messagesQuery = session.createSQLQuery(generateMessageTableScript("id", partitionSize, noOfPartitions));

            maxProcessedMessageSeqQuery.executeUpdate();
            controlTasksQuery.executeUpdate();
            sidelinedGroupsQuery.executeUpdate();
            sidelinedMessagesQuery.executeUpdate();
            skippedIdsQuery.executeUpdate();
            messagesQuery.executeUpdate();
            leaderElectionQuery.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in ensurePersistenceStoreStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            throw e;
        } finally {
            closeSession(session);
        }
    }

    private String generateSkippedIdsTableScript(String partitionColumn, Long partitionSize, Integer noOfPartitions) {
        //here we have to change the id and message_seq_id to bigint because they both are taking the same values in outboundRepositoryImpl
        return "CREATE TABLE if not exists " + exchangeTableNameProvider
                .getTableName(TableType.SKIPPED_IDS) + " (" +
                "  `id` bigint(20)  NOT NULL AUTO_INCREMENT," +
                "  `message_seq_id` bigint(20)  DEFAULT NULL," +
                "  `status` varchar(20) DEFAULT NULL," +
                "  `retry_count` int(3) DEFAULT '0'," +
                "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "  PRIMARY KEY (`id`), " +
                "KEY `message_seq_id` (`message_seq_id`)," +
                "KEY `created_at_status_index` (`created_at`,`status`)" +
                ")  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
                + getPartitionsSQL(partitionColumn, partitionSize, noOfPartitions);
    }

    private String getPartitionsSQL(String partitionColumn, Long partitionSize, Integer noOfPartitions) {
        if(partitionSize == null || noOfPartitions == null) {
            return "";
        }
        return "/*!50100 PARTITION BY RANGE (" + partitionColumn + ") (" + IntStream.range(0, noOfPartitions)
                .mapToObj(i -> String.format("PARTITION p%d VALUES LESS THAN (%d)", partitionSize * (i + 1), partitionSize * (i + 1)))
                .collect(Collectors.joining(",\n")) + ")*/";
    }

    private String generateMessageMetaDataTableScript(String partitionColumn, Long partitionSize, Integer noOfPartitions) {
        String createTableQuery =
                "CREATE TABLE IF NOT EXISTS " + exchangeTableNameProvider
                .getTableName(TableType.MESSAGE_META_DATA) + " (\n" +
                "  `id` bigint(20)  NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) NOT NULL,\n" +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `message_id` (`message_id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n";

        if(partitionMode!=PartitionMode.OUTBOUND)
                createTableQuery += getPartitionsSQL(partitionColumn, partitionSize, noOfPartitions);

        return  createTableQuery;
    }

    private String generateMessageTableScript(String partitionColumn, Long partitionSize, Integer noOfPartitions) {
        return "CREATE TABLE IF NOT EXISTS " + exchangeTableNameProvider
                .getTableName(TableType.MESSAGE) + " (\n" +
                "  `id` bigint(20)  NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) NOT NULL,\n" +
                "  `message` mediumtext,\n" +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  `exchange_name` varchar(100) DEFAULT NULL,\n" +
                "  `exchange_type` varchar(20) DEFAULT 'queue',\n" +
                "  `app_id` varchar(100) DEFAULT NULL,\n" +
                "  `group_id` varchar(100) DEFAULT NULL,\n" +
                "  `http_method` varchar(10) DEFAULT NULL,\n" +
                "  `http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `parent_txn_id` varchar(100) DEFAULT NULL,\n" +
                "  `reply_to` varchar(100) DEFAULT NULL,\n" +
                "  `reply_to_http_method` varchar(10) DEFAULT NULL,\n" +
                "  `reply_to_http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `context` text,\n" +
                "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  `custom_headers` text,\n" +
                "  `transaction_id` varchar(100) DEFAULT NULL,\n" +
                "  `correlation_id` varchar(100) DEFAULT NULL,\n" +
                "  `destination_response_status` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `message_id` (`message_id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n" +
                getPartitionsSQL(partitionColumn, partitionSize, noOfPartitions);
    }

    private void closeSession(Session session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Error in Closing Session:" + e.getMessage(), e);
        }
    }
}
