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
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HibernateOutboundMessageShardRepository implements OutboundMessageRepository {

    private final ShardStrategy<OutboundMessage> shardStrategy;
    private final Session session;

    public HibernateOutboundMessageShardRepository(Session session, ShardStrategy<OutboundMessage> shardStrategy) {
        this.session = session;
        this.shardStrategy = shardStrategy;
    }

    @Override
    public void persist(OutboundMessage message) {
        message.setCreatedAt(OutboundMessageUtils.convertToSqlTimestamp(message.getCreatedAt()));
        Shard shard = shardStrategy.resolve(message);
        OutboundMessageUtils.ensureShard(session, shard);
        OutboundMessageUtils.insertInto(session, shard, message);
    }

    @Override
    public void persist(List<OutboundMessage> messages) {

        final Map<String, List<OutboundMessage>> shardToOutboundMessages = groupOutboundMessagesByShardName(messages);

        session.doWork(new Work() {
            @Override
            public void execute(Connection connection) throws SQLException {
                for (Map.Entry<String, List<OutboundMessage>> entry: shardToOutboundMessages.entrySet()) {
                    String shardName = entry.getKey();
                    List<OutboundMessage> outboundMessages = entry.getValue();
                    final PreparedStatement preparedStatement =
                            connection.prepareStatement(OutboundMessageUtils.getCreateSqlQueryForOutboundMessages(shardName));
                    try {
                        OutboundMessageUtils.bulkInsertMessages(preparedStatement, outboundMessages);
                    } finally {
                        if(null != preparedStatement) {
                            preparedStatement.close();
                        }
                    }
                }
            }
        });
    }

    @Override
    public void persist(String dbShard, List<OutboundMessage> messages) {
        throw new UnsupportedOperationException("method not implemented");
    }


    private Map<String, List<OutboundMessage>> groupOutboundMessagesByShardName(List<OutboundMessage> messages) {
        Map<String, List<OutboundMessage>> map = new HashMap<String, List<OutboundMessage>>();
        for (OutboundMessage message: messages) {
            message.setCreatedAt(OutboundMessageUtils.convertToSqlTimestamp(message.getCreatedAt()));
            Shard shard = shardStrategy.resolve(message);
            String shardName = shard.getShardName();
            if (!map.containsKey(shardName))
                map.put(shardName, new ArrayList<OutboundMessage>());
            map.get(shardName).add(message);
        }
        return map;
    }

    @Override
    public OutboundMessage findMessageById(String messageId, String tableName) {
        return OutboundMessageUtils.findOutboundMessageByMessageId(session, messageId, tableName);
    }

}
