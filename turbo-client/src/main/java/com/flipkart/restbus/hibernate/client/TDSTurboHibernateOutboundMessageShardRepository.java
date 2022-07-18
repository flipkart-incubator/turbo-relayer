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
import com.flipkart.restbus.client.entity.TDSOutboundMessage;
import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.client.shards.ShardStrategy;
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import com.flipkart.restbus.hibernate.models.TDSMetaDataEntity;
import com.flipkart.restbus.hibernate.models.TDSTurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import com.flipkart.restbus.turbo.shard.DynamicShardStrategy;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harshit.gangal on 16/12/16.
 */
public class TDSTurboHibernateOutboundMessageShardRepository implements TDSOutboundMessageRepository {

    private final Session session;
    private Boolean isMultiDbWriteEnabled;
    private Boolean isSingleDbWriteEnabled;
    private boolean turboOutboundWithoutTrxEnabled;
    private final ShardStrategy<OutboundMessage> omsShardStrategy;
    private final DynamicShardStrategy<OutboundMessage> turboShardStrategy;

    public TDSTurboHibernateOutboundMessageShardRepository(Session session,
        Boolean isMultiDbWriteEnabled, Boolean isSingleDbWriteEnabled, boolean turboOutboundWithoutTrxEnabled,
        ShardStrategy<OutboundMessage> omsShardStrategy,
        DynamicShardStrategy<OutboundMessage> turboShardStrategy) {
        this.session = session;
        this.isMultiDbWriteEnabled = isMultiDbWriteEnabled;
        this.isSingleDbWriteEnabled = isSingleDbWriteEnabled;
        this.turboOutboundWithoutTrxEnabled = turboOutboundWithoutTrxEnabled;
        this.omsShardStrategy = omsShardStrategy;
        this.turboShardStrategy = turboShardStrategy;
    }

    @Override public TDSMetaDataEntity persist(OutboundMessage message) {
        List<OutboundMessage> messages = new ArrayList<OutboundMessage>();
        messages.add(message);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = persist(messages);
        return (tdsMetaDataEntityList.size() == 0) ? null : tdsMetaDataEntityList.get(0);
    }

    @Override public List<TDSMetaDataEntity>  persist(List<? extends OutboundMessage> messages) {

        List<TDSMetaDataEntity> tdsMetaDataEntityList = new ArrayList<TDSMetaDataEntity>();
        if (isSingleDbWriteEnabled) {
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
                            TurboOutboundMessageUtils
                                .bulkInsertMessages(preparedStatement, outboundMessages);
                        }finally {
                            if (null != preparedStatement) {
                                preparedStatement.close();
                            }
                        }
                    }
                }
            });
        }

        if (isMultiDbWriteEnabled) {

            final Session outboundSession = TurboSessionProvider.getSession();
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

                Map<String,List<TDSTurboAppMessageEntity>> shardToMetaDataEntity =  new HashMap<String, List<TDSTurboAppMessageEntity>>();
                for (Map.Entry<String, List<OutboundMessage>> entry : appShardToMessages
                        .entrySet()) {
                    final String appShardName = entry.getKey();
                    String outboundShardName =
                            turboShardStrategy.resolve("messages", entry.getValue().get(0))
                                    .getShardName();
                    final List<TDSTurboAppMessageEntity> tdsTurboAppMessageEntities =
                            TurboOutboundMessageUtils.prepareTDSTurboAppEntities(outboundSession,
                                    getMessageIds(entry.getValue()), outboundShardName,
                                    getMessageIdShardMapping(entry.getValue()));

                    List<TDSTurboAppMessageEntity> messageList = shardToMetaDataEntity.get(appShardName);
                    messageList = messageList == null ? new ArrayList<TDSTurboAppMessageEntity>() : messageList;
                    messageList.addAll(tdsTurboAppMessageEntities);
                    shardToMetaDataEntity.put(appShardName,messageList);

                    if(!turboOutboundWithoutTrxEnabled) {
                        session.doWork(new Work() {
                            @Override
                            public void execute(Connection connection) throws SQLException {
                                final PreparedStatement preparedStatement = connection.prepareStatement(
                                        TurboOutboundMessageUtils
                                                .getCreateSqlQueryForTDSTurboMetaData(appShardName));
                                try {
                                    TurboOutboundMessageUtils
                                            .bulkInsertTDSTurboMetaData(preparedStatement,
                                                    tdsTurboAppMessageEntities);
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

                for(Map.Entry<String,List<TDSTurboAppMessageEntity>> entry:shardToMetaDataEntity.entrySet()){
                    TDSMetaDataEntity tdsMetaDataEntity = new TDSMetaDataEntity();
                    tdsMetaDataEntity.setTableName(entry.getKey());
                    tdsMetaDataEntity.setTdsTurboAppMessageEntityList(entry.getValue());
                    tdsMetaDataEntityList.add(tdsMetaDataEntity);
                }

            } finally {
                TurboSessionProvider.closeSession(outboundSession);
            }
        }
        return tdsMetaDataEntityList;
    }

    @Override
    public List<TDSMetaDataEntity> persist(String dbShard, List<? extends OutboundMessage> messages) {
        throw new UnsupportedOperationException("method not implemented");
    }

    private List<String> getMessageIds(List<OutboundMessage> messages) {
        List<String> messageIds = new ArrayList<String>();
        for (OutboundMessage message : messages) {
            messageIds.add(message.getMessageId());
        }
        return messageIds;
    }

    private Map<String, String> getMessageIdShardMapping(List<OutboundMessage> messages) {
        Map<String, String> mIdShardKeyMapping = new HashMap<String, String>();
        for (OutboundMessage message : messages) {
            mIdShardKeyMapping
                .put(message.getMessageId(), ((TDSOutboundMessage) message).getShardKey());
        }
        return mIdShardKeyMapping;
    }

    private Map<String, List<OutboundMessageEntity>> groupOutboundMessagesByShardName(
        List<? extends OutboundMessage> messages) {
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
        List<? extends OutboundMessage> messages) {
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
        List<? extends OutboundMessage> messages) {
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
}
