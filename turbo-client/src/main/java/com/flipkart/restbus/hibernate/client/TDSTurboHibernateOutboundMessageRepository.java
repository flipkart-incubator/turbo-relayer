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
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import com.flipkart.restbus.hibernate.models.TDSMetaDataEntity;
import com.flipkart.restbus.hibernate.models.TDSTurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
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
public class TDSTurboHibernateOutboundMessageRepository implements TDSOutboundMessageRepository {

    private final Session session;
    private Boolean isMultiDbWriteEnabled;
    private Boolean isSingleDbWriteEnabled;
    private boolean turboOutboundWithoutTrxEnabled;

    public TDSTurboHibernateOutboundMessageRepository(Session session,
        Boolean isMultiDbWriteEnabled, Boolean isSingleDbWriteEnabled,boolean turboOutboundWithoutTrxEnabled) {
        this.session = session;
        this.isMultiDbWriteEnabled = isMultiDbWriteEnabled;
        this.isSingleDbWriteEnabled = isSingleDbWriteEnabled;
        this.turboOutboundWithoutTrxEnabled = turboOutboundWithoutTrxEnabled;

    }

    @Override public TDSMetaDataEntity persist(OutboundMessage message) {
        List<OutboundMessage> messages = new ArrayList<OutboundMessage>();
        messages.add(message);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = persist(messages);
        return (tdsMetaDataEntityList.size() == 0) ? null : tdsMetaDataEntityList.get(0);
    }

    @Override public List<TDSMetaDataEntity> persist(List<? extends OutboundMessage> messages) {

        List<TDSMetaDataEntity> tdsMetaDataEntityList = new ArrayList<TDSMetaDataEntity>();
        if (isSingleDbWriteEnabled) {
            final List<OutboundMessageEntity> outboundMessageEntities =
                new ArrayList<OutboundMessageEntity>();
            for (OutboundMessage outboundMessage : messages) {
                OutboundMessageEntity outboundMessageEntity =
                    new OutboundMessageEntity(outboundMessage);
                outboundMessageEntities.add(outboundMessageEntity);
            }

            session.doWork(new Work() {
                @Override public void execute(Connection connection) throws SQLException {
                    final PreparedStatement preparedStatement = connection.prepareStatement(
                        TurboOutboundMessageUtils.getCreateSqlQueryForOutboundMessages());
                    try {
                        TurboOutboundMessageUtils
                            .bulkInsertMessages(preparedStatement, outboundMessageEntities);
                    } finally {
                        if (null != preparedStatement) {
                            preparedStatement.close();
                        }
                    }
                }
            });
        }

        if (isMultiDbWriteEnabled) {
            final List<TurboOutboundMessageEntity> turboOutboundMessageEntities =
                new ArrayList<TurboOutboundMessageEntity>();// = messages;
            List<String> messageIds = new ArrayList<String>();
            Map<String, String> mIdShardKeyMapping = new HashMap<String, String>();
            for (OutboundMessage outboundMessage : messages) {
                messageIds.add(outboundMessage.getMessageId());
                mIdShardKeyMapping.put(outboundMessage.getMessageId(),
                    ((TDSOutboundMessage) outboundMessage).getShardKey());
                TurboOutboundMessageEntity turboOutboundMessageEntity =
                    new TurboOutboundMessageEntity(outboundMessage);
                turboOutboundMessageEntities.add(turboOutboundMessageEntity);
            }

            final Session outboundSession = TurboSessionProvider.getSession();
            try {
                outboundSession.doWork(new Work() {
                    @Override public void execute(Connection connection) throws SQLException {
                        final PreparedStatement preparedStatement = connection.prepareStatement(
                            TurboOutboundMessageUtils.getCreateSqlQueryForTurboOutboundMessages());
                        try {
                            TurboOutboundMessageUtils.bulkInsertTurboMessages(preparedStatement,
                                turboOutboundMessageEntities);
                        } catch (BatchUpdateException e) {
                            //TODO: Need to create partition.
                            throw e;
                        } finally {
                            if (null != preparedStatement) {
                                preparedStatement.close();
                            }
                        }
                    }
                });

                final List<TDSTurboAppMessageEntity> tdsTurboAppMessageEntities =
                        TurboOutboundMessageUtils.
                                prepareTDSTurboAppEntities(outboundSession, messageIds,
                                        TurboOutboundMessageUtils.DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME,
                                        mIdShardKeyMapping);
                TDSMetaDataEntity tdsMetaDataEntity =  new TDSMetaDataEntity();
                tdsMetaDataEntity.setTableName(TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME);
                tdsMetaDataEntity.setTdsTurboAppMessageEntityList(tdsTurboAppMessageEntities);
                tdsMetaDataEntityList.add(tdsMetaDataEntity);

                if(!turboOutboundWithoutTrxEnabled) {
                    session.doWork(new Work() {
                        @Override
                        public void execute(Connection connection) throws SQLException {
                            final PreparedStatement preparedStatement = connection.prepareStatement(
                                    TurboOutboundMessageUtils.getCreateSqlQueryForTDSTurboMetaData());
                            try {
                                TurboOutboundMessageUtils.bulkInsertTDSTurboMetaData(preparedStatement,
                                        tdsTurboAppMessageEntities);
                            } catch (BatchUpdateException e) {
                                //TODO: Need to create partition.
                                throw e;
                            } finally {
                                preparedStatement.close();
                            }
                        }
                    });
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

    @Override public OutboundMessage findMessageById(String messageId, String tableName) {
        OutboundMessageEntity outboundMessageEntity =
            (OutboundMessageEntity) session.createCriteria(TurboOutboundMessageEntity.class)
                .add(Restrictions.eq("messageId", messageId)).uniqueResult();
        return outboundMessageEntity;
    }
}
