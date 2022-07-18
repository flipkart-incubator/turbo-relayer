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
import com.flipkart.restbus.hibernate.models.OutboundMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboAppMessageEntity;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.jdbc.Work;

import javax.naming.ConfigurationException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */
public class TurboHibernateOutboundMessageRepository implements OutboundMessageRepository {
    private final Session session;
    private Boolean isMultiDbWriteEnabled;
    private Boolean isSingleDbWriteEnabled;
    private boolean turboOutboundWithoutTrxEnabled;


    public TurboHibernateOutboundMessageRepository(Session session, Boolean isMultiDbWriteEnabled,
        Boolean isSingleDbWriteEnabled, boolean turboOutboundWithoutTrxEnabled) {
        this.session = session;
        this.isMultiDbWriteEnabled = isMultiDbWriteEnabled;
        this.isSingleDbWriteEnabled = isSingleDbWriteEnabled;
        this.turboOutboundWithoutTrxEnabled = turboOutboundWithoutTrxEnabled;
    }



    @Override public void persist(OutboundMessage message) {

        if (isSingleDbWriteEnabled) {
            OutboundMessageEntity outboundMessageEntity = new OutboundMessageEntity(message);
            session.saveOrUpdate(outboundMessageEntity);
        }

        if (isMultiDbWriteEnabled) {
            TurboOutboundMessageEntity turboOutboundMessageEntity =
                new TurboOutboundMessageEntity(message);
            Session outboundSession = TurboSessionProvider.getSession();

            try {
                Long outboundId;
                try {
                    outboundId = (Long) outboundSession.save(turboOutboundMessageEntity);
                } catch (GenericJDBCException e) {
                    throw e;
                }

                if(!turboOutboundWithoutTrxEnabled) {
                    TurboAppMessageEntity turboAppMessageEntity = new TurboAppMessageEntity(outboundId,
                            turboOutboundMessageEntity.getMessageId());
                    try {
                        session.persist(turboAppMessageEntity);
                    } catch (GenericJDBCException e) {
                        throw e;
                    }
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
            turboFormatWrite(TurboSessionProvider.getSession(),messages);
        }
    }

    @Override public void persist(String dbShard, List<OutboundMessage> messages) throws ConfigurationException {

        if (isSingleDbWriteEnabled) {
            fkscFormatWrite(messages);
        }
        if (isMultiDbWriteEnabled) {
            turboFormatWrite(TurboSessionProvider.getSession(dbShard),messages);
        }
    }

    @Override public OutboundMessage findMessageById(String messageId, String tableName) {
        OutboundMessageEntity outboundMessageEntity =
            (OutboundMessageEntity) session.createCriteria(OutboundMessageEntity.class)
                .add(Restrictions.eq("messageId", messageId)).uniqueResult();
        return outboundMessageEntity;
    }

    private void fkscFormatWrite(List<OutboundMessage> messages){
        final List<OutboundMessageEntity> outboundMessageEntities =
                new ArrayList<OutboundMessageEntity>();// = messages;
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

    private void turboFormatWrite(final Session outboundSession,List<OutboundMessage> messages) {

        final List<TurboOutboundMessageEntity> turboOutboundMessageEntities =
                new ArrayList<TurboOutboundMessageEntity>();// = messages;
        List<String> messageIds = new ArrayList<String>();
        for (OutboundMessage outboundMessage : messages) {
            messageIds.add(outboundMessage.getMessageId());
            TurboOutboundMessageEntity turboOutboundMessageEntity =
                    new TurboOutboundMessageEntity(outboundMessage);
            turboOutboundMessageEntities.add(turboOutboundMessageEntity);
        }

        try {
            outboundSession.doWork(new Work() {
                @Override public void execute(Connection connection) throws SQLException {
                    final PreparedStatement preparedStatement = connection.prepareStatement(
                            TurboOutboundMessageUtils.getCreateSqlQueryForTurboOutboundMessages());
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

            if(!turboOutboundWithoutTrxEnabled) {
                final List<TurboAppMessageEntity> turboAppMessageEntities =
                        TurboOutboundMessageUtils.
                                prepareTurboAppEntities(outboundSession, messageIds,
                                        TurboOutboundMessageUtils.DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME);

                session.doWork(new Work() {
                    @Override
                    public void execute(Connection connection) throws SQLException {
                        final PreparedStatement preparedStatement = connection.prepareStatement(
                                TurboOutboundMessageUtils.getCreateSqlQueryForTurboMetaData());
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
