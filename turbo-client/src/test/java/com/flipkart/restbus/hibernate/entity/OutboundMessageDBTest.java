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
import com.flipkart.restbus.client.shards.OMShardConfig;
import com.flipkart.restbus.client.shards.OMShardStrategyProvider;
import com.flipkart.restbus.client.shards.ShardStrategy;
import com.flipkart.restbus.hibernate.DBBaseTest;
import com.flipkart.restbus.hibernate.client.HibernateOutboundMessageRepository;
import com.flipkart.restbus.hibernate.client.HibernateOutboundMessageShardRepository;
import com.flipkart.restbus.hibernate.client.OutboundMessageRepository;
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.security.InvalidParameterException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class OutboundMessageDBTest extends DBBaseTest {

    private static int counter = 0;
    private static final String OM_SHARD_CONFIG_FILE = "src/test/resources/relayer.time.config.yml";

    private OutboundMessage getNewOutboundMessage() {
        OutboundMessage outboundMessage = new OutboundMessage();
        outboundMessage.setMessageId("MSG-" + (++counter));
        outboundMessage.setGroupId("GRP-" + (++counter));
        outboundMessage.setMessage(outboundMessage.getGroupId() + outboundMessage.getMessageId());
        outboundMessage.setExchangeName("test-exchange");
        outboundMessage.setCreatedAt(new Timestamp(new Date().getTime()));
        outboundMessage.setUpdatedAt(new Timestamp(new Date().getTime()));
        return outboundMessage;
    }


    @Test
    public void testHibernateOutboundMessageRepository() {
        OutboundMessage outboundMessage = getNewOutboundMessage();
        OutboundMessageRepository outboundMessageRepository = new HibernateOutboundMessageRepository(session);
        outboundMessageRepository.persist(outboundMessage);
        OutboundMessage message = outboundMessageRepository.findMessageById(outboundMessage.getMessageId(), OutboundMessageUtils.DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME);
        assertOutboundMessageProperties(outboundMessage, message);
    }

    private void assertOutboundMessageProperties(OutboundMessage outboundMessage, OutboundMessage message) {
        assertEquals(outboundMessage.getAppId(), message.getAppId());
        assertEquals(outboundMessage.getContext(), message.getContext());
        assertEquals(outboundMessage.getCorrelationId(), message.getCorrelationId());
        assertEquals(outboundMessage.getMessageId(), message.getMessageId());
        assertEquals(outboundMessage.getExchangeName(), message.getExchangeName());
        assertEquals(outboundMessage.getExchangeType(), message.getExchangeType());
        assertEquals(outboundMessage.getMessage(), message.getMessage());
        assertEquals(outboundMessage.getInboundMessageId(), message.getInboundMessageId());
        assertEquals(outboundMessage.getAppId(), message.getAppId());
        assertEquals(outboundMessage.getGroupId(), message.getGroupId());
        assertEquals(outboundMessage.getCorrelationId(), message.getCorrelationId());
        assertEquals(outboundMessage.getRoutingKey(), message.getRoutingKey());
        assertEquals(outboundMessage.getHttpMethod(), message.getHttpMethod());
        assertEquals(outboundMessage.getHttpUri(), message.getHttpUri());
        assertEquals(outboundMessage.getReplyTo(), message.getReplyTo());
        assertEquals(outboundMessage.getReplyToHttpMethod(), message.getReplyToHttpMethod());
        assertEquals(outboundMessage.getReplyToHttpUri(), message.getReplyToHttpUri());
        assertEquals(outboundMessage.getTxnId(), message.getTxnId());
        assertEquals(outboundMessage.getRoutingKey(), message.getRoutingKey());
        assertEquals(outboundMessage.getContext(), message.getContext());
        assertEquals(outboundMessage.getDestinationResponseStatus(), message.getDestinationResponseStatus());
//        assertEquals(outboundMessage.getCreatedAt(), message.getCreatedAt());
//        assertEquals(outboundMessage.getUpdatedAt(), message.getUpdatedAt());
        assertEquals(outboundMessage.isRelayed(), message.isRelayed());
        assertEquals(outboundMessage.getRelayedAt(), message.getRelayedAt());
        assertEquals(outboundMessage.getRelayError(), message.getRelayError());
        assertEquals(outboundMessage.getRetries(), message.getRetries());
    }

    @Test
    public void testHibernateOutboundMessageShardRepository() throws FileNotFoundException {
        OutboundMessage outboundMessage = getNewOutboundMessage();
        OMShardConfig shardConfig = OMShardConfig.parseDocument(new FileInputStream(OM_SHARD_CONFIG_FILE));
        ShardStrategy<OutboundMessage> shardStrategy = OMShardStrategyProvider.getStrategy(shardConfig);
        OutboundMessageRepository outboundMessageRepository = new HibernateOutboundMessageShardRepository(session, shardStrategy);
        outboundMessageRepository.persist(outboundMessage);
        OutboundMessage message = outboundMessageRepository.findMessageById(outboundMessage.getMessageId(), shardStrategy.resolve(outboundMessage).getShardName());

        assertOutboundMessageProperties(outboundMessage, message);
    }

    @Test
    public void testHibernateOutboundMessageShardRepositoryForBulk() throws FileNotFoundException {
        OutboundMessage outboundMessage1 = getNewOutboundMessage();
        OutboundMessage outboundMessage2 = getNewOutboundMessage();
        OMShardConfig shardConfig = OMShardConfig.parseDocument(new FileInputStream(OM_SHARD_CONFIG_FILE));
        ShardStrategy<OutboundMessage> shardStrategy = OMShardStrategyProvider.getStrategy(shardConfig);
        OutboundMessageRepository outboundMessageRepository = new HibernateOutboundMessageShardRepository(session, shardStrategy);
        List<OutboundMessage> outboundMessages = Arrays.asList(outboundMessage1, outboundMessage2);
        outboundMessageRepository.persist(outboundMessages);

        OutboundMessage message1 = outboundMessageRepository.findMessageById(outboundMessage1.getMessageId(), shardStrategy.resolve(outboundMessage1).getShardName());
        assertOutboundMessageProperties(outboundMessage1, message1);

        OutboundMessage message2 = outboundMessageRepository.findMessageById(outboundMessage2.getMessageId(), shardStrategy.resolve(outboundMessage2).getShardName());
        assertOutboundMessageProperties(outboundMessage2, message2);
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
        String payload = "{ \"test\": 123 }";
        String groupId = "T1";
        Map<String, String> options = new HashMap<String, String>();
        String appId = "hibernate-restbus-client";
        OutboundMessage outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
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
        outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
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
        outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
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
    }


    @Test(expectedExceptions = InvalidParameterException.class)
    public void testPaylengthConstructOutboundMessageInOutboundMessageUtils() {
        String exchangeName = "exchangeName";
        String exchangeType = "queue";
        String methodType = "POST";
        String httpUri = "http://flipkart.com";
        String replyToExchangeName = null;
        String replyToHttpMethod = "GET";
        String replyToHttpUri = "http://flipkart.com/__reply__to";
        String payload = "{ \"testing_length_50\": 12345678901234567890123456789012345678901234567890 }";
        String groupId = "T1";
        Map<String, String> options = new HashMap<String, String>();
        String appId = "hibernate-restbus-client";
        OutboundMessage outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                payload, groupId, options, appId);

    }
}
