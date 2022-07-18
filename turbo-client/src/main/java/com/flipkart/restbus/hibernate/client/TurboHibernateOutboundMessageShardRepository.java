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

package com.flipkart.restbus.hibernate.client;

import com.flipkart.restbus.client.entity.OutboundMessage;
import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.client.shards.ShardStrategy;
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import com.flipkart.restbus.turbo.shard.DynamicShardStrategy;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import javax.naming.ConfigurationException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */
public class TurboHibernateOutboundMessageShardRepository implements OutboundMessageRepository {
    private final Session session;
    private Boolean isMultiDbWriteEnabled;
    private Boolean isSingleDbWriteEnabled;
    private boolean turboOutboundWithoutTrxEnabled;
    private final ShardStrategy<OutboundMessage> omsShardStrategy;
    private final DynamicShardStrategy<OutboundMessage> turboShardStrategy;

    public TurboHibernateOutboundMessageShardRepository(Session session,
        ShardStrategy<OutboundMessage> omsShardStrategy,
        DynamicShardStrategy<OutboundMessage> turboShardStrategy, Boolean isMultiDbWriteEnabled,
        Boolean isSingleDbWriteEnabled,boolean turboOutboundWithoutTrxEnabled) {
        this.session = session;
        this.omsShardStrategy = omsShardStrategy;
        this.turboShardStrategy = turboShardStrategy;
        this.isMultiDbWriteEnabled = isMultiDbWriteEnabled;
        this.isSingleDbWriteEnabled = isSingleDbWriteEnabled;
        this.turboOutboundWithoutTrxEnabled = turboOutboundWithoutTrxEnabled;
    }

    @Override public void persist(OutboundMessage message) {

        OutboundMessageEntity outboundMessageEntity = new OutboundMessageEntity(message);
        if (isSingleDbWriteEnabled) {
            message.setCreatedAt(
                TurboOutboundMessageUtils.convertToSqlTimestamp(message.getCreatedAt()));
            Shard shard = omsShardStrategy.resolve(message);
            TurboOutboundMessageUtils.ensureShard(session, shard);
            TurboOutboundMessageUtils.insertInto(session, shard, outboundMessageEntity);

        }
        if (isMultiDbWriteEnabled) {
            final Shard outboundShard = turboShardStrategy.resolve("messages", message);
            final Shard appShard = turboShardStrategy.resolve("message_meta_data", message);
            final Session outboundSession = TurboSessionProvider.getSession();
            try {
                final List<TurboOutboundMessageEntity> outboundMessageEntities =
                    new ArrayList<TurboOutboundMessageEntity>();
                outboundMessageEntities.add(new TurboOutboundMessageEntity(message));
                List<String> messageIds = new ArrayList<String>();
                messageIds.add(message.getMessageId());


                outboundSession.doWork(new Work() {
                    @Override public void execute(Connection connection) throws SQLException {
                        final PreparedStatement preparedStatement = connection.prepareStatement(
                            TurboOutboundMessageUtils.getCreateSqlQueryForTurboOutboundMessages(
                                outboundShard.getShardName()));
                        try {
                            TurboOutboundMessageUtils.bulkInsertTurboMessages(preparedStatement,
                                outboundMessageEntities);
                        } catch (BatchUpdateException e) {
                            throw e;
                        } finally {
                            if (null != preparedStatement) {
                                preparedStatement.close();
                            }
                        }
                    }
                });

                if(!turboOutboundWithoutTrxEnabled) {
                    final List<TurboAppMessageEntity> turboAppMessageEntities =
                            TurboOutboundMessageUtils.
                                    prepareTurboAppEntities(outboundSession, messageIds,
                                            turboShardStrategy.resolve("messages", message).getShardName());

                    session.doWork(new Work() {
                        @Override
                        public void execute(Connection connection) throws SQLException {
                            final PreparedStatement preparedStatement = connection.prepareStatement(
                                    TurboOutboundMessageUtils
                                            .getCreateSqlQueryForTurboMetaData(appShard.getShardName()));
                            try {
                                TurboOutboundMessageUtils.bulkInsertTurboMetaData(preparedStatement,
                                        turboAppMessageEntities);
                            } catch (BatchUpdateException e) {
                                throw e;
                            } finally {
                                if (null != preparedStatement) {
                                    preparedStatement.close();
                                }
                            }
                        }
                    });
                }

            } finally {
                TurboSessionProvider.closeSession(outboundSession);
            }
        }
    }


    @Override public void persist(List<OutboundMessage> messages) {
        if (isSingleDbWriteEnabled) {
            fkscFormatWrite(messages);
        }
        if (isMultiDbWriteEnabled) {
            turboFormatWrite(TurboSessionProvider.getSession(), messages);
        }
    }

    @Override
    public void persist(String dbShard, List<OutboundMessage> messages) throws ConfigurationException {
        if (isSingleDbWriteEnabled) {
            fkscFormatWrite(messages);
        }
        if (isMultiDbWriteEnabled) {
            turboFormatWrite(TurboSessionProvider.getSession(dbShard), messages);
        }
    }

    private List<String> getMessageIds(List<OutboundMessage> messages) {
        List<String> messageIds = new ArrayList<String>();
        for (OutboundMessage message : messages) {
            messageIds.add(message.getMessageId());
        }
        return messageIds;
    }

    private Map<String, List<OutboundMessageEntity>> groupOutboundMessagesByShardName(
        List<OutboundMessage> messages) {
        Map<String, List<OutboundMessageEntity>> map =
            new HashMap<String, List<OutboundMessageEntity>>();
        for (OutboundMessage message : messages) {
            message.setCreatedAt(
                TurboOutboundMessageUtils.convertToSqlTimestamp(message.getCreatedAt()));
            OutboundMessageEntity outboundMessageEntity = new OutboundMessageEntity(message);

            Shard shard = omsShardStrategy.resolve(message);
            String shardName = shard.getShardName();
            if (!map.containsKey(shardName))
                map.put(shardName, new ArrayList<OutboundMessageEntity>());
            map.get(shardName).add(outboundMessageEntity);
        }
        return map;
    }

    private Map<String, List<TurboOutboundMessageEntity>> groupTurboOutboundMessagesByShardName(
        List<OutboundMessage> messages) {
        Map<String, List<TurboOutboundMessageEntity>> map =
            new HashMap<String, List<TurboOutboundMessageEntity>>();
        for (OutboundMessage message : messages) {
            message.setCreatedAt(
                TurboOutboundMessageUtils.convertToSqlTimestamp(message.getCreatedAt()));
            TurboOutboundMessageEntity outboundMessageEntity =
                new TurboOutboundMessageEntity(message);

            Shard shard = turboShardStrategy.resolve("messages", message);
            String shardName = shard.getShardName();
            if (!map.containsKey(shardName))
                map.put(shardName, new ArrayList<TurboOutboundMessageEntity>());
            map.get(shardName).add(outboundMessageEntity);
        }
        return map;
    }

    private Map<String, List<OutboundMessage>> groupOutboundMessageByAppShardName(
        List<OutboundMessage> messages) {
        Map<String, List<OutboundMessage>> map = new HashMap<String, List<OutboundMessage>>();
        for (OutboundMessage message : messages) {

            Shard shard = turboShardStrategy.resolve("message_meta_data", message);
            String shardName = shard.getShardName();
            if (!map.containsKey(shardName))
                map.put(shardName, new ArrayList<OutboundMessage>());
            map.get(shardName).add(message);
        }
        return map;
    }

    @Override public OutboundMessage findMessageById(String messageId, String tableName) {
        return OutboundMessageUtils.findOutboundMessageByMessageId(session, messageId, tableName);
    }


    private void fkscFormatWrite(List<OutboundMessage> messages){
        final Map<String, List<OutboundMessageEntity>> shardToOutboundMessages =
                groupOutboundMessagesByShardName(messages);

        session.doWork(new Work() {
            @Override public void execute(Connection connection) throws SQLException {
                for (Map.Entry<String, List<OutboundMessageEntity>> entry : shardToOutboundMessages
                        .entrySet()) {
                    Shard shard = omsShardStrategy.resolve(entry.getValue().get(0));
                    TurboOutboundMessageUtils.ensureShard(session, shard);

                    String shardName = entry.getKey();
                    List<OutboundMessageEntity> outboundMessages = entry.getValue();
                    final PreparedStatement preparedStatement = connection.prepareStatement(
                            OutboundMessageUtils.getCreateSqlQueryForOutboundMessages(shardName));
                    try {
                        TurboOutboundMessageUtils.bulkInsertMessages(preparedStatement, outboundMessages);
                    }finally {
                        if (null != preparedStatement) {
                            preparedStatement.close();
                        }
                    }
                }
            }
        });
    }

    private void turboFormatWrite(final Session outboundSession,List<OutboundMessage> messages) {

        try {
            final Map<String, List<TurboOutboundMessageEntity>> shardToTurboOutboundMessages =
                    groupTurboOutboundMessagesByShardName(messages);
            final Map<String, List<OutboundMessage>> appShardToMessages =
                    groupOutboundMessageByAppShardName(messages);


            for (Map.Entry<String, List<TurboOutboundMessageEntity>> entry : shardToTurboOutboundMessages
                    .entrySet()) {
                final String shardName = entry.getKey();
                final List<TurboOutboundMessageEntity> turboOutboundMessageEntities =
                        entry.getValue();

                outboundSession.doWork(new Work() {
                    @Override public void execute(Connection connection) throws SQLException {
                        final PreparedStatement preparedStatement = connection.prepareStatement(
                                TurboOutboundMessageUtils
                                        .getCreateSqlQueryForTurboOutboundMessages(shardName));
                        try {
                            TurboOutboundMessageUtils.bulkInsertTurboMessages(preparedStatement,
                                    turboOutboundMessageEntities);
                        } catch (BatchUpdateException e) {
                            throw e;
                        } finally {
                            if (null != preparedStatement) {
                                preparedStatement.close();
                            }
                        }
                    }
                });
            }

            if(!turboOutboundWithoutTrxEnabled) {
                for (Map.Entry<String, List<OutboundMessage>> entry : appShardToMessages
                        .entrySet()) {
                    final String shardName = entry.getKey();
                    String outboundShardName =
                            turboShardStrategy.resolve("messages", entry.getValue().get(0))
                                    .getShardName();
                    final List<TurboAppMessageEntity> turboAppMessageEntities =
                            TurboOutboundMessageUtils.prepareTurboAppEntities(outboundSession,
                                    getMessageIds(entry.getValue()), outboundShardName);

                    session.doWork(new Work() {
                        @Override
                        public void execute(Connection connection) throws SQLException {
                            final PreparedStatement preparedStatement = connection.prepareStatement(
                                    TurboOutboundMessageUtils
                                            .getCreateSqlQueryForTurboMetaData(shardName));
                            try {
                                TurboOutboundMessageUtils.bulkInsertTurboMetaData(preparedStatement,
                                        turboAppMessageEntities);
                            } catch (BatchUpdateException e) {
                                throw e;
                            } finally {
                                if (null != preparedStatement) {
                                    preparedStatement.close();
                                }
                            }
                        }
                    });
                }
            }

        } finally {
            TurboSessionProvider.closeSession(outboundSession);
        }

    }

}
