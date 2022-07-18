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
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.jdbc.Work;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class HibernateOutboundMessageRepository implements OutboundMessageRepository {
    private final Session session;

    public HibernateOutboundMessageRepository(Session session) {
        this.session = session;
    }

    @Override
    public void persist(OutboundMessage message) {
        session.saveOrUpdate(new OutboundMessageEntity(message));
    }

    @Override
    public void persist(List<OutboundMessage> messages) {
        final List<OutboundMessage> outboundMessages = messages;
        session.doWork(new Work() {
            @Override
            public void execute(Connection connection) throws SQLException {
                final PreparedStatement preparedStatement = connection.prepareStatement(OutboundMessageUtils.getCreateSqlQueryForOutboundMessages());
                try {
                    OutboundMessageUtils.bulkInsertMessages(preparedStatement, outboundMessages);
                } finally {
                    if(null != preparedStatement) {
                        preparedStatement.close();
                    }
                }
            }
        });
    }

    @Override
    public void persist(String dbShard, List<OutboundMessage> messages) {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public OutboundMessage findMessageById(String messageId, String tableName) {
        OutboundMessageEntity outboundMessageEntity =
                (OutboundMessageEntity) session.createCriteria(OutboundMessageEntity.class)
                        .add(Restrictions.eq("messageId", messageId))
                        .uniqueResult();
        return outboundMessageEntity;
    }
}
