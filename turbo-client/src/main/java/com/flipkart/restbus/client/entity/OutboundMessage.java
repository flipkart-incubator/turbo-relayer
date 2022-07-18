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

/**
 * 
 */
package com.flipkart.restbus.client.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author achanta.vamsi
 *
 */
public class OutboundMessage {
	private static final Logger LOGGER = LoggerFactory.getLogger(OutboundMessage.class);
	
	private String messageId;
	
	private Boolean isRelayed;
	
	private Date relayedAt;
	
	private String exchangeName;
	
	private String message;
	
	private String httpMethod;
	
	private String httpUri;
	
	private Date createdAt;
	
	private Date updatedAt;
	
	private Long inboundMessageId;
	
	private String exchangeType;
	
	private String appId;
	
	private String correlationId;
	
	private String groupId;
	
	private String replyTo;
	
	private String replyToHttpUri;
	
	private String replyToHttpMethod;
	
	private String txnId;
	
	private String context;
	
	private Integer destinationResponseStatus;
	
	private String customHeaders;
	
	private String routingKey;
	
	private String relayError;
	
	private Long retries;

	public enum ExchangeType {
        queue, topic
	}
	
	public enum HttpMethod {
		PUT, POST
	}
	
	public OutboundMessage() {
		super();
	}
	
	/**
	 * if it is a topic
	 * @param message
	 */
	public OutboundMessage(Message message) {
		this.messageId = message.getId();
		this.exchangeName = message.getExchangeName();
		this.exchangeType = message.getExchangeType();
		this.message = message.getPayload();
		this.inboundMessageId = message.getInboundMessageId();
		this.appId = message.getAppId();
		this.groupId = message.getGroupId();
		this.correlationId = message.getCorrelationId();
		this.routingKey = message.getRoutingKey();
		this.httpMethod = message.getHttpMethod();
		this.httpUri = message.getHttpUri();
		this.replyTo = message.getReplyTo();
		this.replyToHttpMethod = message.getReplyToHttpMethod();
		this.replyToHttpUri = message.getReplyToHttpUri();
		this.txnId = message.getTransactionId();
		this.routingKey = message.getRoutingKey();
		this.context = message.getContext();
		this.destinationResponseStatus = message.getDestinationResponseStatus();
		this.createdAt = message.getCreatedAt();
		this.updatedAt = message.getUpdatedAt();
		this.isRelayed = message.getIsRelayed();
		this.relayedAt = message.getRelayedAt();
		this.relayError = message.getRelayError();
		this.retries = message.getRetries();

		final ObjectMapper mapper = new ObjectMapper();
		try {
			this.customHeaders = mapper.writeValueAsString(message.getCustomHeaders());
		} catch (JsonProcessingException e) {
			LOGGER.error("Exception while parsing headers", e);
		}
	}
	
	/**
	 * @return the isRelayed
	 */
	public Boolean isRelayed() {
		return isRelayed;
	}

	/**
	 * @param isRelayed the isRelayed to set
	 */
	public void setRelayed(Boolean isRelayed) {
		this.isRelayed = isRelayed;
	}

	/**
	 * @return the relayedAt
	 */
	public Date getRelayedAt() {
		return relayedAt;
	}

	/**
	 * @param relayedAt the relayedAt to set
	 */
	public void setRelayedAt(Date relayedAt) {
		this.relayedAt = relayedAt;
	}

	/**
	 * @return the exchangeName
	 */
	public String getExchangeName() {
		return exchangeName;
	}

	/**
	 * @param exchangeName the exchangeName to set
	 */
	public void setExchangeName(String exchangeName) {
		this.exchangeName = exchangeName;
	}

	/**
	 * @return the createdAt
	 */
	public Date getCreatedAt() {
		return createdAt;
	}

	/**
	 * @param createdAt the createdAt to set
	 */
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	/**
	 * @return the updatedAt
	 */
	public Date getUpdatedAt() {
		return updatedAt;
	}

	/**
	 * @param updatedAt the updatedAt to set
	 */
	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	/**
	 * @return the inboundMessageId
	 */
	public Long getInboundMessageId() {
		return inboundMessageId;
	}

	/**
	 * @param inboundMessageId the inboundMessageId to set
	 */
	public void setInboundMessageId(Long inboundMessageId) {
		this.inboundMessageId = inboundMessageId;
	}

	/**
	 * @return the exchangeType
	 */
	public String getExchangeType() {
		return exchangeType;
	}

	/**
	 * @param exchangeType the exchangeType to set
	 */
	public void setExchangeType(String exchangeType) {
		this.exchangeType = exchangeType;
	}

	/**
	 * @return the appId
	 */
	public String getAppId() {
		return appId;
	}

	/**
	 * @param appId the appId to set
	 */
	public void setAppId(String appId) {
		this.appId = appId;
	}

	/**
	 * @return the routingKey
	 */
	public String getRoutingKey() {
		return routingKey;
	}

	/**
	 * @param routingKey the routingKey to set
	 */
	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	/**
	 * @return the relayError
	 */
	public String getRelayError() {
		return relayError;
	}

	/**
	 * @param relayError the relayError to set
	 */
	public void setRelayError(String relayError) {
		this.relayError = relayError;
	}

	/**
	 * @return the retries
	 */
	public Long getRetries() {
		return retries;
	}

	/**
	 * @param retries the retries to set
	 */
	public void setRetries(Long retries) {
		this.retries = retries;
	}

	
	/**
	 * @return the httpMethod
	 */
	public String getHttpMethod() {
		return httpMethod;
	}

	/**
	 * @param httpMethod the httpMethod to set
	 */
	public void setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
	}

	/**
	 * @return the httpUri
	 */
	public String getHttpUri() {
		return httpUri;
	}

	/**
	 * @param httpUri the httpUri to set
	 */
	public void setHttpUri(String httpUri) {
		this.httpUri = httpUri;
	}
	
	/**
	 * @return the messageId
	 */
	public String getMessageId() {
		return messageId;
	}

	/**
	 * @param messageId the messageId to set
	 */
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	/**
	 * @return the correlationId
	 */
	public String getCorrelationId() {
		return correlationId;
	}

	/**
	 * @param correlationId the correlationId to set
	 */
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	/**
	 * @return the groupId
	 */
	public String getGroupId() {
		return groupId;
	}

	/**
	 * @param groupId the groupId to set
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	/**
	 * @return the replyTo
	 */
	public String getReplyTo() {
		return replyTo;
	}

	/**
	 * @param replyTo the replyTo to set
	 */
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}

	/**
	 * @return the replyToHttpUri
	 */
	public String getReplyToHttpUri() {
		return replyToHttpUri;
	}

	/**
	 * @param replyToHttpUri the replyToHttpUri to set
	 */
	public void setReplyToHttpUri(String replyToHttpUri) {
		this.replyToHttpUri = replyToHttpUri;
	}

	/**
	 * @return the replyToHttpMethod
	 */
	public String getReplyToHttpMethod() {
		return replyToHttpMethod;
	}

	/**
	 * @param replyToHttpMethod the replyToHttpMethod to set
	 */
	public void setReplyToHttpMethod(String replyToHttpMethod) {
		this.replyToHttpMethod = replyToHttpMethod;
	}

	/**
	 * @return the txnId
	 */
	public String getTxnId() {
		return txnId;
	}

	/**
	 * @param txnId the txnId to set
	 */
	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}

	/**
	 * @return the context
	 */
	public String getContext() {
		return context;
	}

	/**
	 * @param context the context to set
	 */
	public void setContext(String context) {
		this.context = context;
	}

	/**
	 * @return the destinationResponseStatus
	 */
	public Integer getDestinationResponseStatus() {
		return destinationResponseStatus;
	}

	/**
	 * @param destinationResponseStatus the destinationResponseStatus to set
	 */
	public void setDestinationResponseStatus(Integer destinationResponseStatus) {
		this.destinationResponseStatus = destinationResponseStatus;
	}

	/**
	 * @return the customHeaders
	 */
	public String getCustomHeaders() {
		return customHeaders;
	}

	/**
	 * @param customHeaders the customHeaders to set
	 */
	public void setCustomHeaders(String customHeaders) {
		this.customHeaders = customHeaders;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}
	
	/**
	 * @return the message
	 */
	public String getMessage() {
		return this.message;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((messageId == null) ? 0 : messageId.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OutboundMessage other = (OutboundMessage) obj;
		if (messageId == null) {
			if (other.messageId != null)
				return false;
		} else if (!messageId.equals(other.messageId))
			return false;
		return true;
	}

    @Override
    public String toString() {
        return "OutboundMessage {" +
                "messageId='" + messageId + '\'' +
                ", exchangeName='" + exchangeName + '\'' +
                ", httpMethod='" + httpMethod + '\'' +
                ", httpUri='" + httpUri + '\'' +
                ", exchangeType='" + exchangeType + '\'' +
                ", groupId='" + groupId + '\'' +
                ", txnId='" + txnId + '\'' +
                '}';
    }
}
