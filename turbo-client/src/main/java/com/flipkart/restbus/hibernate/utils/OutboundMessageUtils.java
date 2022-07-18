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

import com.flipkart.restbus.client.entity.OutboundMessage;
import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import org.apache.commons.lang.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class OutboundMessageUtils {

    enum OutboundMessageColumn {

        id, message_id, relayed, relayed_at, exchange_name, message, created_at, updated_at,
        inbound_message_id, exchange_type, app_id, correlation_id, group_id, http_method,
        http_uri, reply_to, reply_to_http_method, reply_to_http_uri, txn_id, routing_key, context,
        destination_response_status, relay_error, retries, custom_headers
    }

    public static final String DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME = "outbound_messages";

    private static final String CREATE_SQL_FORMAT = "insert into %s " +
            "(message_id, relayed, relayed_at, exchange_name, message, created_at, updated_at, inbound_message_id, " +
            "exchange_type, app_id, correlation_id, group_id, http_method,  " +
            "http_uri, reply_to, reply_to_http_method, reply_to_http_uri, txn_id, routing_key, context, " +
            "destination_response_status, relay_error, retries, custom_headers) values " +
            "(? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

    private static boolean exists(Session session, Shard shard) {
        String tableName = shard.getShardName();
        String existsCommand = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() " +
                "and TABLE_NAME LIKE '" + tableName + "'";
        return !(session.createSQLQuery(existsCommand).list().isEmpty());
    }

    public static void insertInto(Session session, Shard shard, OutboundMessage outboundMessage) {

        OutboundMessageEntity entity = new OutboundMessageEntity(outboundMessage);
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
                .setParameter("relayed", entity.isRelayed())
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

    public static OutboundMessageEntity findOutboundMessageByMessageId(Session session,
                                                                       String messageId,
                                                                       String tableName) {
        String projections = StringUtils.join(OutboundMessageColumn.values(), ", ");
        String selectCmd = "SELECT " + projections + " FROM " + tableName + " " +
                "WHERE message_id = '" + messageId + "'";

        List list = session.createSQLQuery(selectCmd).list();
        if (list.size() == 0)
            return null;

        Object[] result = (Object[]) list.get(0);
        return constructOutboundMessageFromSQLResponse(result);
    }

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

    public static void setPreparedStatement(OutboundMessage outboundMessage, PreparedStatement preparedStatement)
            throws SQLException
    {

        int index = 1;
        preparedStatement.setString(index++, outboundMessage.getMessageId());
        setBooleanOrNull(preparedStatement, index++, outboundMessage.isRelayed());
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessage.getRelayedAt(), null));
        preparedStatement.setString(index++, outboundMessage.getExchangeName());
        preparedStatement.setString(index++, outboundMessage.getMessage());
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessage.getCreatedAt()));
        preparedStatement.setTimestamp(index++, convertToSqlTimestamp(outboundMessage.getUpdatedAt()));
        setLongOrNull(preparedStatement, index++, outboundMessage.getInboundMessageId());
        preparedStatement.setString(index++, outboundMessage.getExchangeType());
        preparedStatement.setString(index++, outboundMessage.getAppId());
        preparedStatement.setString(index++, outboundMessage.getCorrelationId());
        preparedStatement.setString(index++, outboundMessage.getGroupId());
        preparedStatement.setString(index++, outboundMessage.getHttpMethod());
        preparedStatement.setString(index++, outboundMessage.getHttpUri());
        preparedStatement.setString(index++, outboundMessage.getReplyTo());
        preparedStatement.setString(index++, outboundMessage.getReplyToHttpMethod());
        preparedStatement.setString(index++, outboundMessage.getReplyToHttpUri());
        preparedStatement.setString(index++, outboundMessage.getTxnId());
        preparedStatement.setString(index++, outboundMessage.getRoutingKey());
        preparedStatement.setString(index++, outboundMessage.getContext());
        setIntOrNull(preparedStatement, index++, outboundMessage.getDestinationResponseStatus());
        preparedStatement.setString(index++, outboundMessage.getRelayError());
        setLongOrNull(preparedStatement, index++, outboundMessage.getRetries());
        preparedStatement.setString(index++, outboundMessage.getCustomHeaders());
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

    public static void bulkInsertMessages(PreparedStatement preparedStatement, List<OutboundMessage> outboundMessages)
            throws SQLException {

        for(OutboundMessage outboundMessage: outboundMessages) {
            OutboundMessageUtils.setPreparedStatement(outboundMessage, preparedStatement);

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
                payload,groupId,options,appId, true);
    }
}
