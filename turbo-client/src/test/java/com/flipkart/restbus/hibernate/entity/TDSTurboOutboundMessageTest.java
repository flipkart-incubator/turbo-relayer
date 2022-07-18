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

package com.flipkart.restbus.hibernate.entity;

import com.flipkart.restbus.client.entity.OutboundMessage;
import com.flipkart.restbus.client.entity.TDSOutboundMessage;
import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.hibernate.DBBaseTest;
import com.flipkart.restbus.hibernate.client.TDSOutboundMessageRepository;
import com.flipkart.restbus.hibernate.client.TDSTurboHibernateOutboundMessageRepository;
import com.flipkart.restbus.hibernate.client.TDSTurboHibernateOutboundMessageShardRepository;
import com.flipkart.restbus.hibernate.models.TDSMetaDataEntity;
import com.flipkart.restbus.hibernate.models.TDSTurboAppMessageEntity;
import com.flipkart.restbus.hibernate.utils.Constants;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboConfig;
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import com.flipkart.restbus.turbo.shard.*;
import org.hibernate.Query;
import org.hibernate.Session;
import org.testng.annotations.Test;

import javax.naming.ConfigurationException;
import java.sql.Timestamp;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class TDSTurboOutboundMessageTest extends DBBaseTest {

    private TDSOutboundMessage getNewTDSOutboundMessage(String shardKey, String exchangeName) {
        exchangeName = exchangeName == null ? "exchangeName": exchangeName;
        String exchangeType = "queue";
        String methodType = "POST";
        String httpUri = "http://flipkart.com";
        String replyToExchangeName = null;
        String replyToHttpMethod = "GET";
        String replyToHttpUri = "http://flipkart.com/__reply__to";
        String headerEventName = "event_name";
        String payload = "{ \"test\": 123, event:  \""+headerEventName+"\"  }";
        String groupId = "T1";
        Map<String, String> options = new HashMap<String, String>();
        String appId = "hibernate-restbus-client";
        return TurboOutboundMessageUtils.constructTDSOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId,shardKey);
    }

    /**
     * TDSTurboHibernateOutboundMessageRepository test cases
     */
    @Test
    public void testMultiDBInsertion() {

        TDSOutboundMessageRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageRepository(session,true,false, true);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        TDSMetaDataEntity tdsMetaDataEntity = outboundMessageRepository.persist(outboundMessage);
        assertEquals(TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME,tdsMetaDataEntity.getTableName());
        assertNotNull(tdsMetaDataEntity.getTdsTurboAppMessageEntityList());
        assertEquals(1,tdsMetaDataEntity.getTdsTurboAppMessageEntityList().size());
        TDSTurboAppMessageEntity entity = tdsMetaDataEntity.getTdsTurboAppMessageEntityList().get(0);
        assertEquals(outboundMessage.getMessageId(),entity.getMessageId());


        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId(),null,false);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testMultiDBListInsertion() {
        TDSOutboundMessageRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageRepository(session,true,false, true);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = outboundMessageRepository.persist(tdsOutboundMessages);


        assertNotNull(tdsMetaDataEntityList);
        assertEquals(1,tdsMetaDataEntityList.size());
        TDSMetaDataEntity metaDataEntity = tdsMetaDataEntityList.get(0);
        assertEquals(TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME,metaDataEntity.getTableName());
        List<TDSTurboAppMessageEntity> tdsTurboAppMessageEntityList = metaDataEntity.getTdsTurboAppMessageEntityList();
        assertNotNull(tdsTurboAppMessageEntityList);
        assertEquals(1,tdsTurboAppMessageEntityList.size());
        TDSTurboAppMessageEntity entity = tdsTurboAppMessageEntityList.get(0);
        assertEquals(outboundMessage.getMessageId(),entity.getMessageId());


        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId(),null,false);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testSingleDBInsertion() {
        TDSOutboundMessageRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageRepository(session,false,true, false);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = outboundMessageRepository.persist(tdsOutboundMessages);

        assertNotNull(tdsMetaDataEntityList);
        assertEquals(0,tdsMetaDataEntityList.size());
        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                session,outboundMessage.getMessageId(),
                TurboOutboundMessageUtils.DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME,true);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testMessageListInsertionWithEx() {
        TDSOutboundMessageRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageRepository(session,true,false, true);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        try {
            outboundMessageRepository.persist(null,tdsOutboundMessages);
            fail("UnsupportedOperationException not thrown");
        } catch (UnsupportedOperationException e){
        } catch (ConfigurationException e){
            fail("ConfigurationException "+e.getMessage());
        }
    }


    /**
     * TDSTurboHibernateOutboundMessageRepository test cases
     */
    @Test
    public void testMultiDBShardInsertion() {

        DummyOmsShardStrategy<OutboundMessage> shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
        DummyTurboShardStrategy<OutboundMessage> dynamicShardStrategy = new DummyTurboShardStrategy<OutboundMessage>();
        TDSTurboHibernateOutboundMessageShardRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageShardRepository(
                session,true,false, true,
                shardStrategy,dynamicShardStrategy);
        shardStrategy.resolve(null);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        TDSMetaDataEntity tdsMetaDataEntity = outboundMessageRepository.persist(outboundMessage);
        assertEquals(TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME,tdsMetaDataEntity.getTableName());
        assertNotNull(tdsMetaDataEntity.getTdsTurboAppMessageEntityList());
        assertEquals(1,tdsMetaDataEntity.getTdsTurboAppMessageEntityList().size());
        TDSTurboAppMessageEntity entity = tdsMetaDataEntity.getTdsTurboAppMessageEntityList().get(0);
        assertEquals(outboundMessage.getMessageId(),entity.getMessageId());


        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId(),null,false);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testMultiDBListShardInsertion() {
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("someShardKey", "test_topic_pattern");
        DummyOmsShardStrategy<OutboundMessage> shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
        DynamicShardStrategy<OutboundMessage> dynamicShardStrategy = TurboShardStrategyProvider.getStrategy();
        Shard outboundShard = dynamicShardStrategy.resolve("messages", outboundMessage);
        Shard appShard = dynamicShardStrategy.resolve("message_meta_data", outboundMessage);
        TurboOutboundMessageUtils.ensureTurboMessageShard(TurboSessionProvider.getSession(), outboundShard);

        TurboConfig turboConfig = TurboConfigProvider.getConfig();
        String originalAppDBType = turboConfig.getAppDbType();
        turboConfig.setAppDbType(Constants.APP_DB_TYPE_TDS);
        TurboOutboundMessageUtils.ensureTurboMetaDataShard(TurboSessionProvider.getSession(), appShard);
        turboConfig.setAppDbType(originalAppDBType);

        TDSTurboHibernateOutboundMessageShardRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageShardRepository(
                session,true,false, false,
                shardStrategy,dynamicShardStrategy);

        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = outboundMessageRepository.persist(tdsOutboundMessages);



        assertNotNull(tdsMetaDataEntityList);
        assertEquals(1,tdsMetaDataEntityList.size());
        TDSMetaDataEntity metaDataEntity = tdsMetaDataEntityList.get(0);
        assertEquals(appShard.getShardName(),metaDataEntity.getTableName());
        List<TDSTurboAppMessageEntity> tdsTurboAppMessageEntityList = metaDataEntity.getTdsTurboAppMessageEntityList();
        assertNotNull(tdsTurboAppMessageEntityList);
        assertEquals(1,tdsTurboAppMessageEntityList.size());
        TDSTurboAppMessageEntity entity = tdsTurboAppMessageEntityList.get(0);
        assertEquals(outboundMessage.getMessageId(),entity.getMessageId());


        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId(),outboundShard.getShardName(),false);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());

        Query query = session.createSQLQuery(String.format("SELECT message_id FROM %s WHERE message_id in ( :messageIds) ",appShard.getShardName()));
        query.setParameterList("messageIds",Arrays.asList(outboundMessage.getMessageId()));
        assertEquals(outboundMessage.getMessageId(),query.uniqueResult());
    }

    @Test
    public void testSingleDBShardInsertion() {
        DummyOmsShardStrategy<OutboundMessage> shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
        DummyTurboShardStrategy<OutboundMessage> dynamicShardStrategy = new DummyTurboShardStrategy<OutboundMessage>();
        TDSTurboHibernateOutboundMessageShardRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageShardRepository(
                session,false,true, true,
                shardStrategy,dynamicShardStrategy);
        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        List<TDSMetaDataEntity> tdsMetaDataEntityList = outboundMessageRepository.persist(tdsOutboundMessages);

        assertNotNull(tdsMetaDataEntityList);
        assertEquals(0,tdsMetaDataEntityList.size());
        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                session,outboundMessage.getMessageId(),
                TurboOutboundMessageUtils.DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME,true);
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testMessageListShardInsertionWithEx() {
        DummyOmsShardStrategy<OutboundMessage> shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
        DummyTurboShardStrategy<OutboundMessage> dynamicShardStrategy = new DummyTurboShardStrategy<OutboundMessage>();
        TDSOutboundMessageRepository outboundMessageRepository = new TDSTurboHibernateOutboundMessageShardRepository(
                session,true,false, false,
                shardStrategy,dynamicShardStrategy);

        TDSOutboundMessage outboundMessage = getNewTDSOutboundMessage("shardKey", null);
        List<TDSOutboundMessage> tdsOutboundMessages = new ArrayList<TDSOutboundMessage>();
        tdsOutboundMessages.add(outboundMessage);
        try {
            outboundMessageRepository.persist(null,tdsOutboundMessages);
            fail("UnsupportedOperationException not thrown");
        } catch (UnsupportedOperationException e){
        } catch (ConfigurationException e){
            fail("ConfigurationException "+e.getMessage());
        }
    }


    private OutboundMessage findOutboundMessageById(Session session,String messageId,String tableName,boolean fkscFormat) {
        String transactionColumn = fkscFormat ? "txn_id" : "transaction_id";

        tableName = (tableName == null) ? "messages" : tableName;
        Query query = session.createSQLQuery(
                "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                        +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, "+transactionColumn+", "
                        +  "correlation_id, destination_response_status, created_at from " + tableName
                        + " where message_id in ( :messageId )");
        query.setParameter("messageId", messageId);
        Object[] object = (Object[])query.uniqueResult();
        OutboundMessage message = new OutboundMessage();

        message.setGroupId(object[1] != null ? object[1].toString() : null);
        message.setMessageId(object[2] != null ? object[2].toString() : null);
        message.setMessage(object[3] != null ? object[3].toString() : null);
        message.setExchangeName(object[4] != null ? object[4].toString() : null);
        message.setExchangeType(object[5] != null ? object[5].toString() : null);
        message.setAppId(object[6] != null ? object[6].toString() : null);
        message.setHttpMethod(object[7] != null ? object[7].toString() : null);
        message.setHttpUri(object[8] != null ? object[8].toString() : null);
        message.setCustomHeaders(object[9] != null ? object[9].toString() : null);
        message.setReplyTo(object[10] != null ? object[10].toString() : null);
        message.setReplyToHttpMethod(object[11] != null ? object[11].toString() : null);
        message.setReplyToHttpUri(object[12] != null ? object[12].toString() : null);
        message.setTxnId(object[13] != null ? object[13].toString() : null);
        message.setCorrelationId(object[14] != null ? object[14].toString() : null);
        message.setDestinationResponseStatus(object[15] != null ? Integer.parseInt(object[15].toString()) : null);
        message.setCreatedAt(object[16] != null ? (Timestamp) object[16] : null);
        return message;
    }


    @Test
    public void testConstructOutboundMessageInOutboundMessageUtils() {
        String exchangeName = "exchangeName";
        String exchangeType = "queue";
        String methodType = "POST";
        String httpUri = "http://flipkart.com";
        String replyToExchangeName = null;
        String replyToHttpMethod = "GET";
        String replyToHttpUri = "http://flipkart.com/__reply__to";
        String headerEventName = "event_name";
        String payload = "{ \"test\": 123, event:  \""+headerEventName+"\"  }";
        String groupId = "T1";
        String shardKey = "shardKey";
        Map<String, String> options = new HashMap<String, String>();
        String appId = "hibernate-restbus-client";

        OutboundMessage outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId);

        assertEquals(outboundMessage.getExchangeName(), exchangeName);
        assertEquals(outboundMessage.getExchangeType(), exchangeType);
        assertEquals(outboundMessage.getHttpMethod(), methodType);
        assertEquals(outboundMessage.getHttpUri(), httpUri);
        assertNull(outboundMessage.getReplyTo());
        assertEquals(outboundMessage.getReplyToHttpMethod(), replyToHttpMethod);
        assertEquals(outboundMessage.getReplyToHttpUri(), replyToHttpUri);
        assertEquals(outboundMessage.getMessage(), payload);
        assertEquals(outboundMessage.getGroupId(), groupId);
        assertEquals(outboundMessage.getAppId(), appId);
        assertNotNull(outboundMessage.getMessageId());

        replyToExchangeName = "replyToExchangeName";

        TDSOutboundMessage tdsOutboundMessage = TurboOutboundMessageUtils.constructTDSOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId, shardKey);
        assertEquals(tdsOutboundMessage.getShardKey(),shardKey);

    }

}
