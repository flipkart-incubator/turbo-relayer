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
import com.flipkart.restbus.hibernate.DBBaseTest;
import com.flipkart.restbus.hibernate.client.TurboHibernateOutboundMessageRepository;
import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import com.google.gson.Gson;
import org.hibernate.Query;
import org.hibernate.Session;
import org.testng.annotations.Test;

import javax.naming.ConfigurationException;
import java.sql.Timestamp;
import java.util.*;

import static org.junit.Assert.*;

public class TurboOutboundMessageDBTest extends DBBaseTest {

    private OutboundMessage getNewOutboundMessage() {
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
        Map<String, String> options = new HashMap<String, String>();
        String appId = "hibernate-restbus-client";
        return TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId);
    }

    @Test
    public void testTurboHibernateOutboundMessageInsertion() {
        TurboHibernateOutboundMessageRepository outboundMessageRepository = new TurboHibernateOutboundMessageRepository(session,true,false, false);
        OutboundMessage outboundMessage = getNewOutboundMessage();
        outboundMessageRepository.persist(outboundMessage);

        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId());
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testTurboHibernateOutboundMessageWithNullCreatedAtInsertion() {
        TurboHibernateOutboundMessageRepository outboundMessageRepository = new TurboHibernateOutboundMessageRepository(session,true,true, false);
        OutboundMessage outboundMessage = getNewOutboundMessage();
        outboundMessage.setCreatedAt(null);
        outboundMessage.setUpdatedAt(null);
        outboundMessageRepository.persist(outboundMessage);

        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(),outboundMessage.getMessageId());
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessage.getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void testTurboHibernateOutboundMessageListInsertion() {
        TurboHibernateOutboundMessageRepository outboundMessageRepository = new TurboHibernateOutboundMessageRepository(session,true,false, false);
        List<OutboundMessage> outboundMessageList = new ArrayList<OutboundMessage>();
        outboundMessageList.add(getNewOutboundMessage());
        outboundMessageRepository.persist(outboundMessageList);

        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(), outboundMessageList.get(0).getMessageId());
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessageList.get(0).getMessageId(),persistedOutboundMessage.getMessageId());
    }

    @Test
    public void turboHibernateOutboundMessageDBShard () throws ConfigurationException {
        TurboHibernateOutboundMessageRepository outboundMessageRepository = new TurboHibernateOutboundMessageRepository(
                session,true,false, false);
        List<OutboundMessage> outboundMessageList = new ArrayList<OutboundMessage>();
        outboundMessageList.add(getNewOutboundMessage());
        String dbShard = "shard1";
        ensureTables(TurboSessionProvider.getSession(dbShard));
        outboundMessageRepository.persist(dbShard,outboundMessageList);

        OutboundMessage persistedOutboundMessage = findOutboundMessageById(
                TurboSessionProvider.getSession(dbShard), outboundMessageList.get(0).getMessageId());
        assertNotNull(persistedOutboundMessage);
        assertEquals(outboundMessageList.get(0).getMessageId(),persistedOutboundMessage.getMessageId());
    }

    private OutboundMessage convertTurboOutboundToOutboundMessage(TurboOutboundMessageEntity turboOutboundMessage){
        OutboundMessage outboundMessage = new OutboundMessage();
        outboundMessage.setGroupId(turboOutboundMessage.getGroupId());
        outboundMessage.setMessageId(turboOutboundMessage.getMessageId());
        outboundMessage.setMessage(turboOutboundMessage.getMessage());
        outboundMessage.setExchangeName(turboOutboundMessage.getExchangeName());
        outboundMessage.setExchangeType(turboOutboundMessage.getExchangeType());
        outboundMessage.setAppId(turboOutboundMessage.getAppId());
        outboundMessage.setHttpMethod(turboOutboundMessage.getHttpMethod());
        outboundMessage.setHttpUri(turboOutboundMessage.getHttpUri());
        outboundMessage.setCustomHeaders(turboOutboundMessage.getCustomHeaders());
        outboundMessage.setReplyTo(turboOutboundMessage.getReplyTo());
        outboundMessage.setReplyToHttpUri(turboOutboundMessage.getReplyToHttpUri());
        outboundMessage.setReplyToHttpMethod(turboOutboundMessage.getReplyToHttpMethod());
        outboundMessage.setTxnId(turboOutboundMessage.getTransactionId());
        outboundMessage.setCorrelationId(turboOutboundMessage.getCorrelationId());
        outboundMessage.setDestinationResponseStatus(turboOutboundMessage.getDestinationResponseStatus());
        outboundMessage.setContext(turboOutboundMessage.getContext());
        return outboundMessage;
    }

    private OutboundMessage findOutboundMessageById(Session session,String messageId) {
        Query query = session.createSQLQuery(
                "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                        +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, transaction_id, "
                        +  "correlation_id, destination_response_status, created_at from messages"
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
        outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId);
        assertEquals(outboundMessage.getExchangeName(), exchangeName);
        assertEquals(outboundMessage.getExchangeType(), exchangeType);
        assertEquals(outboundMessage.getHttpMethod(), methodType);
        assertEquals(outboundMessage.getHttpUri(), httpUri);
        assertEquals(outboundMessage.getReplyTo(), replyToExchangeName);
        assertEquals(outboundMessage.getReplyToHttpMethod(), replyToHttpMethod);
        assertEquals(outboundMessage.getReplyToHttpUri(), replyToHttpUri);
        assertEquals(outboundMessage.getMessage(), payload);
        assertEquals(outboundMessage.getGroupId(), groupId);
        assertEquals(outboundMessage.getAppId(), appId);
        assertNotNull(outboundMessage.getMessageId());

        options.put("messageIdQueryParam", "__messageId__");
        outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId);
        assertEquals(outboundMessage.getExchangeName(), exchangeName);
        assertEquals(outboundMessage.getExchangeType(), exchangeType);
        assertEquals(outboundMessage.getHttpMethod(), methodType);
        assertEquals(outboundMessage.getHttpUri(), httpUri);
        assertEquals(outboundMessage.getReplyTo(), replyToExchangeName);
        assertNotNull(outboundMessage.getMessageId());
        assertEquals(outboundMessage.getReplyToHttpMethod(), replyToHttpMethod);
        assertEquals(outboundMessage.getReplyToHttpUri(), replyToHttpUri + "?__messageId__=" + outboundMessage.getMessageId());
        assertEquals(outboundMessage.getMessage(), payload);
        assertEquals(outboundMessage.getGroupId(), groupId);
        assertEquals(outboundMessage.getAppId(), appId);

        exchangeType = "topic";
        outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId, false);
        assertEquals(outboundMessage.getExchangeName(), exchangeName);
        assertEquals(outboundMessage.getExchangeType(), exchangeType);
        assertEquals(outboundMessage.getHttpMethod(), methodType);
        assertEquals(outboundMessage.getHttpUri(), httpUri);
        assertEquals(outboundMessage.getReplyTo(), replyToExchangeName);
        assertNotNull(outboundMessage.getMessageId());
        assertEquals(outboundMessage.getReplyToHttpMethod(), replyToHttpMethod);
        assertEquals(outboundMessage.getMessage(), payload);
        assertEquals(outboundMessage.getGroupId(), groupId);
        assertEquals(outboundMessage.getAppId(), appId);
        Map<String, String> customHeaders = new Gson().fromJson(outboundMessage.getCustomHeaders(), Map.class);
        assertFalse(customHeaders.containsKey("X_EVENT_NAME"));
        assertFalse(customHeaders.containsKey("X_TOPIC_NAME"));


        outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId, true);
        customHeaders = new Gson().fromJson(outboundMessage.getCustomHeaders(), Map.class);
        assertEquals(customHeaders.get("X_EVENT_NAME"), headerEventName);
        assertEquals(customHeaders.get("X_TOPIC_NAME"), exchangeName);


        outboundMessage = TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, customHeaders, appId);
        Map<String, String> modifiedHeaders = new Gson().fromJson(outboundMessage.getCustomHeaders(), Map.class);
        assertTrue(customHeaders.equals(modifiedHeaders));


        TDSOutboundMessage tdsOutboundMessage = TurboOutboundMessageUtils.constructTDSOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId, shardKey);
        assertEquals(tdsOutboundMessage.getShardKey(),shardKey);

    }

}
