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

package com.flipkart.restbus.hibernate.utils;

/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */

import com.flipkart.restbus.client.entity.OutboundMessage;
import com.flipkart.restbus.client.entity.TDSOutboundMessage;
import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import com.flipkart.restbus.hibernate.models.TDSTurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import com.mysql.jdbc.NonRegisteringDriver;
import org.apache.commons.lang.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.internal.SessionFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class TurboOutboundMessageUtils {
    private static final Logger logger = LoggerFactory.getLogger(TurboOutboundMessageUtils.class);


    enum OutboundMessageColumn {

        id, message_id, relayed, relayed_at, exchange_name, message, created_at, updated_at,
        inbound_message_id, exchange_type, app_id, correlation_id, group_id, http_method,
        http_uri, reply_to, reply_to_http_method, reply_to_http_uri, txn_id, routing_key, context,
        destination_response_status, relay_error, retries, custom_headers
    }

    public static final String DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME = "outbound_messages";
    public static final String DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME = "messages";
    public static final String DEFAULT_TURBO_META_DATA_TABLE_NAME = "message_meta_data";


    private static final String CREATE_SQL_FORMAT = "insert into %s " +
            "(message_id, relayed, relayed_at, exchange_name, message, created_at, updated_at, inbound_message_id, " +
            "exchange_type, app_id, correlation_id, group_id, http_method,  " +
            "http_uri, reply_to, reply_to_http_method, reply_to_http_uri, txn_id, routing_key, context, " +
            "destination_response_status, relay_error, retries, custom_headers) values " +
            "(? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_TURBO_MESSAGE_SQL_FORMAT = "insert into %s " +
            "(message_id, message, created_at, exchange_name, exchange_type, app_id, group_id, http_method, " +
            "http_uri, reply_to, reply_to_http_method, reply_to_http_uri,  " +
            "context, custom_headers, transaction_id, correlation_id, destination_response_status) values " +
            "(? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?, ?, ?, ?, ?)";

    private static final String INSERT_TURBO_META_DATA_FORMAT = "insert into %s "+
            "(id, message_id) values( ?, ? )";

    private static final String INSERT_TDS_TURBO_META_DATA_FORMAT = "insert into %s "+
        "(id, message_id, shard_key) values( ?, ?, ? )";

    public static void ensureShard(Session session, Shard shard) {
        if (!exists(session, shard))
            createShard(session, shard);
    }

    private static void createShard(Session session, Shard shard) {
        String tableName = shard.getShardName();

        String createCmd = "CREATE TABLE " + tableName + " (\n" +
                "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) NOT NULL,\n" +
                "  `relayed` tinyint(4) DEFAULT NULL,\n" +
                "  `relayed_at` datetime DEFAULT NULL,\n" +
                "  `exchange_name` varchar(100) NOT NULL,\n" +
                "  `message` longtext,\n" +
                "  `created_at` datetime DEFAULT NULL,\n" +
                "  `updated_at` datetime DEFAULT NULL,\n" +
                "  `inbound_message_id` bigint(20) DEFAULT NULL,\n" +
                "  `exchange_type` varchar(255) DEFAULT NULL,\n" +
                "  `app_id` varchar(50) DEFAULT NULL,\n" +
                "  `correlation_id` varchar(100) DEFAULT NULL,\n" +
                "  `group_id` varchar(100) NOT NULL,\n" +
                "  `http_method` varchar(10) DEFAULT NULL,\n" +
                "  `http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `reply_to` varchar(50) DEFAULT NULL,\n" +
                "  `reply_to_http_method` varchar(10) DEFAULT NULL,\n" +
                "  `reply_to_http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `txn_id` varchar(100) DEFAULT NULL,\n" +
                "  `routing_key` varchar(100) DEFAULT NULL,\n" +
                "  `context` longtext,\n" +
                "  `destination_response_status` int(11) DEFAULT NULL,\n" +
                "  `relay_error` varchar(255) DEFAULT NULL,\n" +
                "  `retries` int(11) DEFAULT '0',\n" +
                "  `custom_headers` longtext,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `message_id_idx` (`message_id`),\n" +
                "  KEY `exchange_name_idx` (`exchange_name`),\n" +
                "  KEY `group_id_idx` (`group_id`),\n" +
                "  KEY `relayed_idx` (`relayed`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        session.createSQLQuery(createCmd).executeUpdate();
    }

    public static void createNewPartition(long partitionSize, Session session, String tableName) {
        long lastPartitionEndId = partitionMaxId(session, tableName) - 1;
        long endId = lastPartitionEndId + partitionSize;
        String partitionName = "p" + (endId + 1)/ partitionSize;
            Query query = session.createSQLQuery(
                    "ALTER TABLE " + tableName + " ADD PARTITION (PARTITION " + partitionName + " VALUES LESS THAN (" + (endId + 1) + "))");
            query.executeUpdate();
    }

    public static long partitionMaxId(Session session, String tableName) {
        Properties sessionProperties = ((SessionFactoryImpl)session.getSessionFactory()).getProperties();
        String connectionUrl = sessionProperties.getProperty("hibernate.connection.url");
        Properties urlProperties;
        try {
            urlProperties = new NonRegisteringDriver().parseURL(connectionUrl, new Properties());
        } catch (SQLException e) {
            logger.error("Error while parsing DB connection URL: ", e);
            throw new RuntimeException(e);
        }
        String dbName = urlProperties.getProperty("DBNAME");

        Query query = session.createSQLQuery(
                "SELECT MAX(CAST(partition_description AS UNSIGNED)) from INFORMATION_SCHEMA.PARTITIONS WHERE " +
                        "TABLE_SCHEMA = \"" + dbName + "\" AND " +
                        "TABLE_NAME = \"" + tableName + "\"");
        return ((BigInteger) query.uniqueResult()).longValue();
    }

    private static boolean exists(Session session, Shard shard) {
        String tableName = shard.getShardName();
        String existsCommand = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " +
                "and TABLE_NAME LIKE '" + tableName + "'";
        return !(session.createSQLQuery(existsCommand).list().isEmpty());
    }

    public static void insertInto(Session session, Shard shard, OutboundMessageEntity entity) {

        String tableName = shard.getShardName();

        String insertCmd = "INSERT INTO " + tableName +
                " (message_id, relayed, relayed_at, exchange_name, message, created_at, updated_at" +
                ", inbound_message_id, exchange_type, app_id, correlation_id, group_id, http_method, http_uri" +
                ", reply_to, reply_to_http_method, reply_to_http_uri, txn_id, routing_key, context" +
                ", destination_response_status, relay_error, retries, custom_headers) " +
                "VALUES (:message_id, :relayed, :relayed_at, :exchange_name, :message, :created_at, :updated_at, " +
                ":inbound_message_id, :exchange_type, :app_id, :correlation_id, :group_id, :http_method, :http_uri, " +
                ":reply_to, :reply_to_http_method, :reply_to_http_uri, :txn_id, :routing_key, :context, " +
                ":destination_response_status, :relay_error, :retries, :custom_headers)";

        Query insertQuery = session.createSQLQuery(insertCmd)
                .setParameter("message_id", entity.getMessageId())
                .setParameter("relayed", entity.getRelayedByte())
                .setParameter("relayed_at", entity.getRelayedAt())
                .setParameter("exchange_name", entity.getExchangeName())
                .setParameter("message", entity.getMessage())
                .setParameter("created_at", entity.getCreatedAt())
                .setParameter("updated_at", entity.getUpdatedAt())
                .setParameter("inbound_message_id", entity.getInboundMessageId())
                .setParameter("exchange_type", entity.getExchangeType())
                .setParameter("app_id", entity.getAppId())
                .setParameter("correlation_id", entity.getCorrelationId())
                .setParameter("group_id", entity.getGroupId())
                .setParameter("http_method", entity.getHttpMethod())
                .setParameter("http_uri", entity.getHttpUri())
                .setParameter("reply_to", entity.getReplyTo())
                .setParameter("reply_to_http_method", entity.getReplyToHttpMethod())
                .setParameter("reply_to_http_uri", entity.getReplyToHttpUri())
                .setParameter("txn_id", entity.getTxnId())
                .setParameter("routing_key", entity.getRoutingKey())
                .setParameter("context", entity.getContext())
                .setParameter("destination_response_status", entity.getDestinationResponseStatus())
                .setParameter("relay_error", entity.getRelayError())
                .setParameter("retries", entity.getRetries())
                .setParameter("custom_headers", entity.getCustomHeaders());

        insertQuery.executeUpdate();
    }

//    public static OutboundMessageEntity findOutboundMessageByMessageId(Session session,
//                                                                       String messageId,
//                                                                       String tableName) {
//        String projections = StringUtils.join(OutboundMessageColumn.values(), ", ");
//        String selectCmd = "SELECT " + projections + " FROM " + tableName + " " +
//                "WHERE message_id = '" + messageId + "'";
//
//        List list = session.createSQLQuery(selectCmd).list();
//        if (list.size() == 0)
//            return null;
//
//        Object[] result = (Object[]) list.get(0);
//        return constructOutboundMessageFromSQLResponse(result);
//    }

    private static OutboundMessageEntity constructOutboundMessageFromSQLResponse(Object[] sqlResponse) {
        OutboundMessageEntity entity = new OutboundMessageEntity();

        entity.setId(((BigInteger)sqlResponse[OutboundMessageColumn.id.ordinal()]).longValue());
        entity.setMessageId((String)sqlResponse[OutboundMessageColumn.message_id.ordinal()]);
        Object relayed = sqlResponse[OutboundMessageColumn.relayed.ordinal()];
        Boolean isRelayed = relayed != null ? (Byte) relayed > 0 : null;
        entity.setRelayed(isRelayed);
        entity.setRelayedAt((Timestamp)sqlResponse[OutboundMessageColumn.relayed_at.ordinal()]);
        entity.setExchangeName((String)sqlResponse[OutboundMessageColumn.exchange_name.ordinal()]);
        entity.setMessage((String)sqlResponse[OutboundMessageColumn.message.ordinal()]);
        entity.setCreatedAt((Timestamp)sqlResponse[OutboundMessageColumn.created_at.ordinal()]);
        entity.setUpdatedAt((Timestamp)sqlResponse[OutboundMessageColumn.updated_at.ordinal()]);
        entity.setInboundMessageId((Long)sqlResponse[OutboundMessageColumn.inbound_message_id.ordinal()]);
        entity.setExchangeType((String)sqlResponse[OutboundMessageColumn.exchange_type.ordinal()]);
        entity.setAppId((String)sqlResponse[OutboundMessageColumn.app_id.ordinal()]);
        entity.setCorrelationId((String)sqlResponse[OutboundMessageColumn.correlation_id.ordinal()]);
        entity.setGroupId((String)sqlResponse[OutboundMessageColumn.group_id.ordinal()]);
        entity.setHttpMethod((String)sqlResponse[OutboundMessageColumn.http_method.ordinal()]);
        entity.setHttpUri((String)sqlResponse[OutboundMessageColumn.http_uri.ordinal()]);
        entity.setReplyTo((String)sqlResponse[OutboundMessageColumn.reply_to.ordinal()]);
        entity.setReplyToHttpMethod((String)sqlResponse[OutboundMessageColumn.reply_to_http_method.ordinal()]);
        entity.setReplyToHttpUri((String)sqlResponse[OutboundMessageColumn.reply_to_http_uri.ordinal()]);
        entity.setTxnId((String)sqlResponse[OutboundMessageColumn.txn_id.ordinal()]);
        entity.setRoutingKey((String)sqlResponse[OutboundMessageColumn.routing_key.ordinal()]);
        entity.setContext((String)sqlResponse[OutboundMessageColumn.context.ordinal()]);
        entity.setDestinationResponseStatus((Integer)sqlResponse[OutboundMessageColumn.destination_response_status.ordinal()]);
        entity.setRelayError((String)sqlResponse[OutboundMessageColumn.relay_error.ordinal()]);
        entity.setRetries((Long)sqlResponse[OutboundMessageColumn.retries.ordinal()]);
        entity.setCustomHeaders((String)sqlResponse[OutboundMessageColumn.custom_headers.ordinal()]);

        return entity;
    }


    public static void setPreparedStatement(OutboundMessageEntity outboundMessageEntity, PreparedStatement preparedStatement)
            throws SQLException {

        int index = 1;
        preparedStatement.setString(index++, outboundMessageEntity.getMessageId());
        setByteOrNull(preparedStatement, index++, outboundMessageEntity.getRelayedByte());
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessageEntity.getRelayedAt(), null));
        preparedStatement.setString(index++, outboundMessageEntity.getExchangeName());
        preparedStatement.setString(index++, outboundMessageEntity.getMessage());
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessageEntity.getCreatedAt()));
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessageEntity.getUpdatedAt()));
        setLongOrNull(preparedStatement, index++, outboundMessageEntity.getInboundMessageId());
        preparedStatement.setString(index++, outboundMessageEntity.getExchangeType());
        preparedStatement.setString(index++, outboundMessageEntity.getAppId());
        preparedStatement.setString(index++, outboundMessageEntity.getCorrelationId());
        preparedStatement.setString(index++, outboundMessageEntity.getGroupId());
        preparedStatement.setString(index++, outboundMessageEntity.getHttpMethod());
        preparedStatement.setString(index++, outboundMessageEntity.getHttpUri());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyTo());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyToHttpMethod());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyToHttpUri());
        preparedStatement.setString(index++, outboundMessageEntity.getTxnId());
        preparedStatement.setString(index++, outboundMessageEntity.getRoutingKey());
        preparedStatement.setString(index++, outboundMessageEntity.getContext());
        setIntOrNull(preparedStatement, index++, outboundMessageEntity.getDestinationResponseStatus());
        preparedStatement.setString(index++, outboundMessageEntity.getRelayError());
        setLongOrNull(preparedStatement, index++, outboundMessageEntity.getRetries());
        preparedStatement.setString(index++, outboundMessageEntity.getCustomHeaders());
    }


    private static void setBooleanOrNull(PreparedStatement preparedStatement, int index, Boolean value)
            throws SQLException {

        if (value == null)
            preparedStatement.setNull(index, Types.BOOLEAN);
        else
            preparedStatement.setBoolean(index, value);
    }

    private static void setIntOrNull(PreparedStatement preparedStatement, int index, Integer value)
            throws SQLException {

        if (value == null)
            preparedStatement.setNull(index, Types.INTEGER);
        else
            preparedStatement.setInt(index, value);
    }

    private static void setLongOrNull(PreparedStatement preparedStatement, int index, Long value)
            throws SQLException {

        if (value == null)
            preparedStatement.setNull(index, Types.BIGINT);
        else
            preparedStatement.setLong(index, value);
    }


    private static void setByteOrNull(PreparedStatement preparedStatement, int index, Byte value)
            throws SQLException {

        if (value == null)
            preparedStatement.setNull(index, Types.TINYINT);
        else
            preparedStatement.setByte(index, value);
    }


    public static Timestamp convertToSqlTimestamp(Date date) {
        return convertToSqlTimestamp(date, new Timestamp(new Date().getTime()));
    }

    public static Timestamp convertToSqlTimestamp(Date date, Timestamp defaultValue) {
        return date != null ? new Timestamp(date.getTime()) : defaultValue;
    }

    public static String getCreateSqlQueryForOutboundMessages() {
        return getCreateSqlQueryForOutboundMessages(DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME);
    }

    public static String getCreateSqlQueryForOutboundMessages(String tableName) {
        String sqlQuery = String.format(CREATE_SQL_FORMAT, tableName != null ? tableName : "");
        return sqlQuery;
    }

    ///////////////

    public static void ensureTurboMessageShard(Session session, Shard shard) {
        if (!existsTurboMessageShard(session, shard))
            createTurboMessageShard(session, shard);
    }

    private static void createTurboMessageShard(Session session, Shard shard) {
        String tableName = shard.getShardName();

        String createCmd = "CREATE TABLE "+tableName+" (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) DEFAULT NULL,\n" +
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
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n";


        session.createSQLQuery(createCmd).executeUpdate();
    }

    private static boolean existsTurboMessageShard(Session session, Shard shard) {
        String tableName = shard.getShardName();
        String existsCommand = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " +
                "and TABLE_NAME LIKE '" + tableName + "'";
        return !(session.createSQLQuery(existsCommand).list().isEmpty());
    }


    public static void ensureTurboMetaDataShard(Session session, Shard shard) {
        if (!existsTurboMetaDataShard(session, shard)) {
            if(Constants.APP_DB_TYPE_TDS.equalsIgnoreCase(TurboConfigProvider.getConfig().getAppDbType())) {
                createTDSTurboMetaDataShard(session, shard);
            } else {
                createTurboMetaDataShard(session, shard);
            }
        }
    }

    private static void createTurboMetaDataShard(Session session, Shard shard) {
        String tableName = shard.getShardName();

        String createCmd = "CREATE TABLE "+tableName+"(\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) DEFAULT NULL,\n" +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `message_id` (`message_id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n";


        session.createSQLQuery(createCmd).executeUpdate();
    }

    private static void createTDSTurboMetaDataShard(Session session, Shard shard) {
        String tableName = shard.getShardName();

        StringBuilder createCmd = new StringBuilder();
        createCmd.append("CREATE TABLE ").append(tableName)
            .append(" (`id` int(11) NOT NULL AUTO_INCREMENT")
            .append(", `message_id` varchar(100) DEFAULT NULL")
            .append(", `shard_key`  varchar(100) NOT NULL")
            .append(", `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
            .append(", PRIMARY KEY (`id`)").append(",  KEY `message_id` (`message_id`)")
            .append(") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n");

        session.createSQLQuery(createCmd.toString()).executeUpdate();
    }

    private static boolean existsTurboMetaDataShard(Session session, Shard shard) {
        String tableName = shard.getShardName();
        String existsCommand = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " +
                "and TABLE_NAME LIKE '" + tableName + "'";
        return !(session.createSQLQuery(existsCommand).list().isEmpty());
    }

    public static List<TurboAppMessageEntity> prepareTurboAppEntities(Session session, List<String> messageIds, String tableName) {
        String messageIdsSubClause = "'" + StringUtils.join(messageIds.iterator(), "','") + "'";
        Query query = session.createSQLQuery("SELECT id, message_id FROM " +  tableName + " WHERE message_id in ( " + messageIdsSubClause + " ) ORDER BY id ");
        List<Object[]> messages = query.list();
        List<TurboAppMessageEntity> appMessages = new ArrayList<TurboAppMessageEntity>();
        for(Object[] message: messages) {
            appMessages.add(new TurboAppMessageEntity(Long.parseLong(message[0].toString()), (String) message[1]));
        }
        return appMessages;
    }

    public static List<TDSTurboAppMessageEntity> prepareTDSTurboAppEntities(Session session, List<String> messageIds, String tableName, Map<String, String> mIdShardKeyMapping) {
        String messageIdsSubClause = "'" + StringUtils.join(messageIds.iterator(), "','") + "'";
        Query query = session.createSQLQuery("SELECT id, message_id FROM " +  tableName + " WHERE message_id in ( " + messageIdsSubClause + " ) ORDER BY id ");
        List<Object[]> messages = query.list();
        List<TDSTurboAppMessageEntity> appMessages = new ArrayList<TDSTurboAppMessageEntity>();
        for(Object[] message: messages) {
            appMessages.add(new TDSTurboAppMessageEntity(Long.valueOf(message[0].toString()), (String) message[1], mIdShardKeyMapping.get(message[1].toString())));
        }
        return appMessages;
    }

    public static List<TurboOutboundMessageEntity> getMessagesByMessageId(Session session, List<String> messageIds, String tableName) {
        String messageIdsSubClause = "'" + StringUtils.join(messageIds.iterator(), "','") + "'";
        Query query = session.createSQLQuery("SELECT * FROM " +  tableName + " WHERE message_id in ( " + messageIdsSubClause + " )");
        List<TurboOutboundMessageEntity> messages = query.list();
        return messages;
    }

    public static String getCreateSqlQueryForTurboMetaData() {
        return getCreateSqlQueryForTurboMetaData(DEFAULT_TURBO_META_DATA_TABLE_NAME);
    }

    public static String getCreateSqlQueryForTDSTurboMetaData() {
        return getCreateSqlQueryForTDSTurboMetaData(DEFAULT_TURBO_META_DATA_TABLE_NAME);
    }

    public static String getCreateSqlQueryForTurboMetaData(String tableName) {
        String sqlQuery = String.format(INSERT_TURBO_META_DATA_FORMAT, tableName != null ? tableName : "");
        return sqlQuery;
    }

    public static String getCreateSqlQueryForTDSTurboMetaData(String tableName) {
        String sqlQuery = String.format(INSERT_TDS_TURBO_META_DATA_FORMAT, tableName != null ? tableName : "");
        return sqlQuery;
    }

    public static String getCreateSqlQueryForTurboOutboundMessages() {
        return getCreateSqlQueryForTurboOutboundMessages(DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME);
    }

    public static String getCreateSqlQueryForTurboOutboundMessages(String tableName) {
        String sqlQuery = String.format(INSERT_TURBO_MESSAGE_SQL_FORMAT, tableName != null ? tableName : "");
        return sqlQuery;
    }

    public static void bulkInsertTurboMessages(PreparedStatement preparedStatement, List<TurboOutboundMessageEntity> outboundMessageEntities)
            throws SQLException {

        for(TurboOutboundMessageEntity outboundMessageEntity: outboundMessageEntities) {
            setTurboPreparedStatement(outboundMessageEntity, preparedStatement);

            preparedStatement.addBatch();
            preparedStatement.clearParameters();
        }
        preparedStatement.executeBatch();
    }

    public static void setTurboPreparedStatement(TurboOutboundMessageEntity outboundMessageEntity, PreparedStatement preparedStatement)
            throws SQLException {

        int index = 1;
        preparedStatement.setString(index++, outboundMessageEntity.getMessageId());
        preparedStatement.setString(index++, outboundMessageEntity.getMessage());
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessageEntity.getCreatedAt()));
        preparedStatement.setString(index++, outboundMessageEntity.getExchangeName());
        preparedStatement.setString(index++, outboundMessageEntity.getExchangeType());
        preparedStatement.setString(index++, outboundMessageEntity.getAppId());
        preparedStatement.setString(index++, outboundMessageEntity.getGroupId());
        preparedStatement.setString(index++, outboundMessageEntity.getHttpMethod());
        preparedStatement.setString(index++, outboundMessageEntity.getHttpUri());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyTo());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyToHttpMethod());
        preparedStatement.setString(index++, outboundMessageEntity.getReplyToHttpUri());
        preparedStatement.setString(index++, outboundMessageEntity.getContext());
        preparedStatement.setString(index++, outboundMessageEntity.getCustomHeaders());
        preparedStatement.setString(index++, outboundMessageEntity.getTransactionId());
        preparedStatement.setString(index++, outboundMessageEntity.getCorrelationId());
        setIntOrNull(preparedStatement, index++, outboundMessageEntity.getDestinationResponseStatus());
    }


    public static void bulkInsertTurboMetaData(PreparedStatement preparedStatement, List<TurboAppMessageEntity> turboAppMessageEntities)
            throws SQLException {

        for(TurboAppMessageEntity turboAppMessageEntity: turboAppMessageEntities) {
            setTurboMetaDataPreparedStatement(turboAppMessageEntity, preparedStatement);

            preparedStatement.addBatch();
            preparedStatement.clearParameters();
        }
        preparedStatement.executeBatch();
    }

    public static void setTurboMetaDataPreparedStatement(TurboAppMessageEntity turboAppMessageEntity, PreparedStatement preparedStatement)
            throws SQLException {

        int index = 1;
        preparedStatement.setLong(index++, turboAppMessageEntity.getId());
        preparedStatement.setString(index++, turboAppMessageEntity.getMessageId());

    }

    public static void bulkInsertTDSTurboMetaData(PreparedStatement preparedStatement, List<TDSTurboAppMessageEntity> tdsTurboAppMessageEntities)
        throws SQLException {

        for(TDSTurboAppMessageEntity tdsTurboAppMessageEntity: tdsTurboAppMessageEntities) {
            setTDSTurboMetaDataPreparedStatement(tdsTurboAppMessageEntity, preparedStatement);

            preparedStatement.addBatch();
            preparedStatement.clearParameters();
        }
        preparedStatement.executeBatch();
    }

    public static void setTDSTurboMetaDataPreparedStatement(TDSTurboAppMessageEntity tdsTurboAppMessageEntity, PreparedStatement preparedStatement)
        throws SQLException {

        int index = 1;
        preparedStatement.setLong(index++, tdsTurboAppMessageEntity.getId());
        preparedStatement.setString(index++, tdsTurboAppMessageEntity.getMessageId());
        preparedStatement.setString(index++, tdsTurboAppMessageEntity.getShardKey());

    }

    //////////////////
    public static void bulkInsertMessages(PreparedStatement preparedStatement, List<OutboundMessageEntity> outboundMessageEntities)
            throws SQLException {

        for(OutboundMessageEntity outboundMessageEntity: outboundMessageEntities) {
            setPreparedStatement(outboundMessageEntity, preparedStatement);

            preparedStatement.addBatch();
            preparedStatement.clearParameters();
        }

        preparedStatement.executeBatch();
    }

    public static OutboundMessage constructOutboundMessage(String exchangeName, String exchangeType, String httpMethod,
                                                           String httpUri, String replyToExchangeName,
                                                           String replyToHttpMethod, String replyToHttpUri,
                                                           String payload, String groupId,
                                                           Map<String, String> options, String appId) {

        OutboundMessage outboundMessage = new OutboundMessage();
        return MessageUtilsCommon.constructOutboundMessage(
                outboundMessage,
                exchangeName,exchangeType,httpMethod,httpUri,replyToExchangeName,replyToHttpMethod,replyToHttpUri,
                payload,groupId,options,appId,true);
    }

    public static OutboundMessage constructOutboundMessage(String exchangeName, String exchangeType, String httpMethod,
                                                           String httpUri, String replyToExchangeName,
                                                           String replyToHttpMethod, String replyToHttpUri,
                                                           String payload, String groupId,
                                                           Map<String, String> options, String appId, boolean modifyHeader) {

        OutboundMessage outboundMessage = new OutboundMessage();
        return MessageUtilsCommon.constructOutboundMessage(
                outboundMessage,
                exchangeName,exchangeType,httpMethod,httpUri,replyToExchangeName,replyToHttpMethod,replyToHttpUri,
                payload,groupId,options,appId,modifyHeader);
    }

    public static OutboundMessage constructOutboundMessage(String exchangeName, String exchangeType, String httpMethod,
                                                           String httpUri, String replyToExchangeName,
                                                           String replyToHttpMethod, String replyToHttpUri,
                                                           String payload, String groupId,
                                                           Map<String, String> options, Map<String, String> headers, String appId) {

        OutboundMessage outboundMessage = new OutboundMessage();
        return MessageUtilsCommon.constructOutboundMessage(
                outboundMessage,
                exchangeName,exchangeType,httpMethod,httpUri,replyToExchangeName,replyToHttpMethod,replyToHttpUri,
                payload,groupId,options,headers,appId);
    }

    public static TDSOutboundMessage constructTDSOutboundMessage(String exchangeName, String exchangeType, String httpMethod,
                                                            String httpUri, String replyToExchangeName,
                                                            String replyToHttpMethod, String replyToHttpUri,
                                                            String payload, String groupId,
                                                            Map<String, String> options, String appId, String shardKey) {

        TDSOutboundMessage tdsOutboundMessage = new TDSOutboundMessage(shardKey);
        return (TDSOutboundMessage)MessageUtilsCommon.constructOutboundMessage(
                tdsOutboundMessage,
                exchangeName,exchangeType,httpMethod,httpUri,replyToExchangeName,replyToHttpMethod,replyToHttpUri,
                payload,groupId,options,appId, true);
    }

}
