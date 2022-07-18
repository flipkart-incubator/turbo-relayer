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

import com.flipkart.restbus.client.entity.OutboundMessage;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Date;

@Entity
@Access(AccessType.PROPERTY)
@Table(name = "outbound_messages")
public class OutboundMessageEntity extends OutboundMessage {
    private Long id;
    private Byte relayedByte;

    public OutboundMessageEntity()  {
        super();
        setCreatedAt(new Timestamp(new Date().getTime()));
        setUpdatedAt(getCreatedAt());
    }

    public OutboundMessageEntity(OutboundMessage message)  {
        this();
        setMessageId(message.getMessageId());
        setExchangeName(message.getExchangeName());
        setExchangeType(message.getExchangeType());
        setMessage(message.getMessage());
        setInboundMessageId(message.getInboundMessageId());
        setAppId(message.getAppId());
        setGroupId(message.getGroupId());
        setCorrelationId(message.getCorrelationId());
        setRoutingKey(message.getRoutingKey());
        setHttpMethod(message.getHttpMethod());
        setHttpUri(message.getHttpUri());
        setReplyTo(message.getReplyTo());
        setReplyToHttpMethod(message.getReplyToHttpMethod());
        setReplyToHttpUri(message.getReplyToHttpUri());
        setTxnId(message.getTxnId());
        setRoutingKey(message.getRoutingKey());
        setContext(message.getContext());
        setDestinationResponseStatus(message.getDestinationResponseStatus());
        if(message.getCreatedAt() != null)
            setCreatedAt(message.getCreatedAt());

        if(message.getUpdatedAt() != null)
            setUpdatedAt(message.getUpdatedAt());

        setRelayed(message.isRelayed());
        setRelayedAt(message.getRelayedAt());
        setRelayError(message.getRelayError());
        setRetries(message.getRetries());
        setCustomHeaders(message.getCustomHeaders());
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    @Column(name = "message_id")
    public String getMessageId() {
        return super.getMessageId();
    }


    /////     Below part is changed for turbo backward compatibility. Status = 4 will be writtened.
    @Override
    @Transient
    public Boolean isRelayed() {
        // TODO Auto-generated method stub
        return super.isRelayed();
    }

    @Column(name = "relayed")
    public Byte getRelayedByte()
    {
        return relayedByte;
    }

    public void setRelayedByte(Byte relayedByte)
    {
        this.relayedByte = relayedByte;
    }


    /////

    @Override
    @Column(name = "relayed_at")
    public Date getRelayedAt() {
        return super.getRelayedAt();
    }

    @Override
    @Column(name = "exchange_name")
    public String getExchangeName() {
        return super.getExchangeName();
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }

    @Override
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "created_at")
    public Date getCreatedAt() {
        return super.getCreatedAt();
    }

    @Override
    @Temporal(TemporalType.TIMESTAMP)
    @Version
    @Column(name = "updated_at")
    public Date getUpdatedAt() {
        return super.getUpdatedAt();
    }

    @Override
    @Column(name = "inbound_message_id")
    public Long getInboundMessageId() {
        return super.getInboundMessageId();
    }

    @Override
    @Column(name = "exchange_type")
    public String getExchangeType() {
        return super.getExchangeType();
    }

    @Override
    @Column(name = "app_id")
    public String getAppId() {
        return super.getAppId();
    }

    @Override
    @Column(name = "correlation_id")
    public String getCorrelationId() {
        return super.getCorrelationId();
    }

    @Override
    @Column(name = "group_id")
    public String getGroupId() {
        return super.getGroupId();
    }

    @Override
    @Column(name = "http_method")
    public String getHttpMethod() {
        return super.getHttpMethod();
    }

    @Override
    @Column(name = "http_uri")
    public String getHttpUri() {
        return super.getHttpUri();
    }

    @Override
    @Column(name = "reply_to")
    public String getReplyTo() {
        return super.getReplyTo();
    }

    @Override
    @Column(name = "reply_to_http_method")
    public String getReplyToHttpMethod() {
        return super.getReplyToHttpMethod();
    }

    @Override
    @Column(name = "routing_key")
    public String getRoutingKey() {
        return super.getRoutingKey();
    }

    @Override
    @Column(name = "relay_error")
    public String getRelayError() {
        return super.getRelayError();
    }

    @Override
    @Column(name = "retries")
    public Long getRetries() {
        return super.getRetries();
    }

    @Override
    @Column(name = "reply_to_http_uri")
    public String getReplyToHttpUri() {
        return super.getReplyToHttpUri();
    }

    @Override
    @Column(name = "txn_id")
    public String getTxnId() {
        return super.getTxnId();
    }

    @Override
    public String getContext() {
        return super.getContext();
    }

    @Override
    @Column(name = "destination_response_status")
    public Integer getDestinationResponseStatus() {
        return super.getDestinationResponseStatus();
    }

    @Override
    @Column(name = "custom_headers")
    public String getCustomHeaders() {
        return super.getCustomHeaders();
    }
}