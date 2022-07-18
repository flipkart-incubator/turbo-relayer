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

package com.flipkart.restbus.hibernate.models;

/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */
import com.flipkart.restbus.client.entity.OutboundMessage;
import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name = "messages")
@Access(AccessType.PROPERTY)
public class TurboOutboundMessageEntity
{
    public TurboOutboundMessageEntity(OutboundMessage outboundMessage) {

        setGroupId(outboundMessage.getGroupId());
        setMessageId(outboundMessage.getMessageId());
        setMessage(outboundMessage.getMessage());
        setExchangeName(outboundMessage.getExchangeName());
        setExchangeType(outboundMessage.getExchangeType());
        setAppId(outboundMessage.getAppId());
        setHttpMethod(outboundMessage.getHttpMethod());
        setHttpUri(outboundMessage.getHttpUri());
        setCustomHeaders(outboundMessage.getCustomHeaders());
        setReplyTo(outboundMessage.getReplyTo());
        setReplyToHttpUri(outboundMessage.getReplyToHttpUri());
        setReplyToHttpMethod(outboundMessage.getReplyToHttpMethod());
        setTransactionId(outboundMessage.getTxnId());
        setCorrelationId(outboundMessage.getCorrelationId());
        setDestinationResponseStatus(outboundMessage.getDestinationResponseStatus());
        setContext(outboundMessage.getContext());
    }

    Long id;
    String groupId;
    String messageId;
    String message;
    String exchangeName;
    String exchangeType;
    String appId;
    String httpMethod;
    String httpUri;
    String customHeaders;
    String replyTo;
    String replyToHttpMethod;
    String replyToHttpUri;
    String context;
    String transactionId;
    String correlationId;
    Integer destinationResponseStatus;
    Date createdAt;
    Date updatedAt;

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    @Column(name = "group_id")
    public String getGroupId()
    {
        return groupId;
    }

    public void setGroupId(String groupId)
    {
        this.groupId = groupId;
    }

    @Column(name = "message_id")
    public String getMessageId()
    {
        return messageId;
    }

    public void setMessageId(String messageId)
    {
        this.messageId = messageId;
    }

    @Column(name = "message")
    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }


    @Column(name = "exchange_name")
    public String getExchangeName()
    {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName)
    {
        this.exchangeName = exchangeName;
    }


    @Column(name = "exchange_type")
    public String getExchangeType()
    {
        return exchangeType;
    }

    public void setExchangeType(String exchangeType)
    {
        this.exchangeType = exchangeType;
    }


    @Column(name = "app_id")
    public String getAppId()
    {
        return appId;
    }

    public void setAppId(String appId)
    {
        this.appId = appId;
    }


    @Column(name = "http_method")
    public String getHttpMethod()
    {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod)
    {
        this.httpMethod = httpMethod;
    }


    @Column(name = "http_uri")
    public String getHttpUri()
    {
        return httpUri;
    }

    public void setHttpUri(String httpUri)
    {
        this.httpUri = httpUri;
    }


    @Column(name = "custom_headers")
    public String getCustomHeaders()
    {
        return customHeaders;
    }

    public void setCustomHeaders(String customHeaders)
    {
        this.customHeaders = customHeaders;
    }


    @Column(name = "reply_to")
    public String getReplyTo()
    {
        return replyTo;
    }

    public void setReplyTo(String replyTo)
    {
        this.replyTo = replyTo;
    }


    @Column(name = "reply_to_http_method")
    public String getReplyToHttpMethod()
    {
        return replyToHttpMethod;
    }

    public void setReplyToHttpMethod(String replyToHttpMethod)
    {
        this.replyToHttpMethod = replyToHttpMethod;
    }


    @Column(name = "reply_to_http_uri")
    public String getReplyToHttpUri()
    {
        return replyToHttpUri;
    }

    public void setReplyToHttpUri(String replyToHttpUri)
    {
        this.replyToHttpUri = replyToHttpUri;
    }

    @Column(name = "transaction_id")
    public String getTransactionId()
    {
        return transactionId;
    }

    public void setTransactionId(String transactionId)
    {
        this.transactionId = transactionId;
    }

    @Column(name = "correlation_id")
    public String getCorrelationId()
    {
        return correlationId;
    }

    public void setCorrelationId(String correlationId)
    {
        this.correlationId = correlationId;
    }

    @Column(name = "destination_response_status")
    public Integer getDestinationResponseStatus()
    {
        return destinationResponseStatus;
    }

    public void setDestinationResponseStatus(Integer destinationResponseStatus)
    {
        this.destinationResponseStatus = destinationResponseStatus;
    }


    @Column(name = "created_at")
    public Date getCreatedAt()
    {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt)
    {
        this.createdAt = createdAt;
    }

    @Column(name = "updated_at")
    public Date getUpdatedAt()
    {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt)
    {
        this.updatedAt = updatedAt;
    }

    @Column(name = "context")
    public String getContext()
    {
        return context;
    }

    public void setContext(String context)
    {
        this.context = context;
    }
}