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
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.security.InvalidParameterException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class MessageUtilsCommon {
    private static final Logger logger = LoggerFactory.getLogger(MessageUtilsCommon.class);

    private static final Gson GSON = new Gson();

    static OutboundMessage constructOutboundMessage(OutboundMessage outboundMessage,
                                                           String exchangeName, String exchangeType, String httpMethod,
                                                           String httpUri, String replyToExchangeName,
                                                           String replyToHttpMethod, String replyToHttpUri,
                                                           String payload, String groupId,
                                                           Map<String, String> options, String appId,boolean modifyHeader) {


        Map<String, String> headers = addDefaultHeaders(getOption(options, "customHeaders", "{}"), payload, exchangeName, exchangeType, modifyHeader);
        return constructOutboundMessage(outboundMessage,
                exchangeName,exchangeType,httpMethod,httpUri,replyToExchangeName,replyToHttpMethod,replyToHttpUri,
                payload,groupId,options,headers,appId);
    }

    static OutboundMessage constructOutboundMessage(OutboundMessage outboundMessage,
                                                    String exchangeName, String exchangeType, String httpMethod,
                                                    String httpUri, String replyToExchangeName,
                                                    String replyToHttpMethod, String replyToHttpUri,
                                                    String payload, String groupId,
                                                    Map<String, String> options, Map<String, String> headers,String appId) {

        Long allowedPayloadLength = TurboConfigProvider.getConfig().getPayloadLength();
        if ((allowedPayloadLength != null) && (payload.length() > allowedPayloadLength)) {
            logger.error("Payload length is more that allowed length");
            throw new InvalidParameterException("Payload length is more that allowed length, length found: " + payload.length() + ", allowed length: " + allowedPayloadLength);
        }
        outboundMessage.setMessageId(getOption(options, "messageId", UUID.randomUUID().toString()));
        outboundMessage.setGroupId(groupId != null ? groupId.toUpperCase() : outboundMessage.getMessageId());

        outboundMessage.setHttpMethod(httpMethod);
        outboundMessage.setHttpUri(httpUri);
        outboundMessage.setMessage(payload);
        outboundMessage.setExchangeName(exchangeName);
        outboundMessage.setExchangeType(exchangeType);

        outboundMessage.setReplyTo(replyToExchangeName);
        String replyToUrl = MessageUtilsCommon.constructReplyTo(outboundMessage.getMessageId(), replyToHttpUri, options);
        outboundMessage.setReplyToHttpUri(replyToUrl != null ? replyToUrl : getOption(options, "replyToHttpUrl", null));
        outboundMessage.setReplyToHttpMethod(replyToHttpMethod != null ? replyToHttpMethod : getOption(options, "replyToHttpMethod", null));

        outboundMessage.setContext(getOption(options, "context", ""));
        outboundMessage.setCustomHeaders(GSON.toJson(headers));
        outboundMessage.setTxnId(getOption(options, "txnId", "TXN-" + outboundMessage.getMessageId()));
        outboundMessage.setCreatedAt(new Date());
        outboundMessage.setAppId(appId);
        outboundMessage.setRetries(0L);
        return outboundMessage;
    }

    private static Map<String, String> addDefaultHeaders(String customHeaders, String payload, String exchangeName, String exchangeType, boolean modifyHeader) {
        Map<String, String> headers = parseOrDefaultJSON(customHeaders);
        if(modifyHeader && "topic".equalsIgnoreCase(exchangeType)) {
            Map<String, String> body = parseOrDefaultJSON(payload);
            addTopicHeaders(headers, body.get("event"), exchangeName);
        }
        return headers;
    }

    private static Map<String, String> parseOrDefaultJSON(String payload) {
        try {
            return GSON.fromJson(payload, Map.class);
        } catch(JsonSyntaxException e) {
            return new HashMap<String, String>();
        }
    }

    private static void addTopicHeaders(Map<String, String> headers, String event, String exchangeName) {
        headers.put("X_EVENT_NAME", event);
        headers.put("X_TOPIC_NAME", exchangeName);
    }

    static String getOption(Map<String, String> options, String key, String defaultValue) {
        return options.containsKey(key) ? options.get(key) : defaultValue;
    }

    static String constructReplyTo(String messageId, String replyToHttpUrl, Map<String, String> options) {
        if (replyToHttpUrl != null && options != null && options.containsKey("messageIdQueryParam")) {
            String messageIdQueryParam = options.get("messageIdQueryParam");
            if(messageIdQueryParam != null) {
                if (replyToHttpUrl.contains("&"))
                    return replyToHttpUrl + "&" + messageIdQueryParam + "=" + messageId;
                return replyToHttpUrl + "?" + messageIdQueryParam + "=" + messageId;
            }
        }
        return replyToHttpUrl;
    }
}
