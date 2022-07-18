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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author achanta.vamsi
 *
 */
public class Message {
	
    private String id;

    private String payload;

    private String groupId;

    private String subscriberId;

    private String correlationId;

    private String replyTo;

    private String context;

    private String transactionId;

    private Long inboundMessageId;
    
    private String replyToHttpUri;

    private String replyToHttpMethod;

    private Integer destinationResponseStatus;

    private String user;

    private String contentType;

    private Map<String, String> headers = new HashMap<String, String>();
    
    private Map<String, Object> customHeaders = new HashMap<String, Object>();
    
	private Integer responseCode;

    private String responseBody;

	private String exchangeName;

	private String httpMethod;
	
	private String httpUri;

	private String exchangeType;
	
	private String appId;
	
	private String routingKey;
	
	private Date createdAt;
	
	private Date updatedAt;
	
	private String responseHeaders;

	private String requestBody;
	
	private Boolean isRelayed;
	
	private Date relayedAt;
	
	private String relayError;
	
	private Long retryCount;
	
	private Long retries;

	
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

	private Long timestamp;

    public Message() {
    }

    
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the payload
	 */
	public String getPayload() {
		return payload;
	}

	/**
	 * @param payload the payload to set
	 */
	public void setPayload(String payload) {
		this.payload = payload;
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
	 * @return the subscriberId
	 */
	public String getSubscriberId() {
		return subscriberId;
	}

	/**
	 * @param subscriberId the subscriberId to set
	 */
	public void setSubscriberId(String subscriberId) {
		this.subscriberId = subscriberId;
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
	 * @return the transactionId
	 */
	public String getTransactionId() {
		return transactionId;
	}

	/**
	 * @param transactionId the transactionId to set
	 */
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
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
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the contentType
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * @param contentType the contentType to set
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * @return the headers
	 */
	public Map<String, String> getHeaders() {
		return headers;
	}

	/**
	 * @param headers the headers to set
	 */
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	/**
	 * @return the responseCode
	 */
	public Integer getResponseCode() {
		return responseCode;
	}

	/**
	 * @param responseCode the responseCode to set
	 */
	public void setResponseCode(Integer responseCode) {
		this.responseCode = responseCode;
	}

	/**
	 * @return the responseBody
	 */
	public String getResponseBody() {
		return responseBody;
	}

	/**
	 * @param responseBody the responseBody to set
	 */
	public void setResponseBody(String responseBody) {
		this.responseBody = responseBody;
	}

	/**
	 * @return the timestamp
	 */
	public Long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	

    /**
	 * @return the customHeaders
	 */
	public Map<String, Object> getCustomHeaders() {
		return customHeaders;
	}


	/**
	 * @param customHeaders the customHeaders to set
	 */
	public void addCustomHeaders(String key, Object value) {
		this.customHeaders.put(key, value);
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
	 * @return the responseHeaders
	 */
	public String getResponseHeaders() {
		return responseHeaders;
	}


	/**
	 * @param responseHeaders the responseHeaders to set
	 */
	public void setResponseHeaders(String responseHeaders) {
		this.responseHeaders = responseHeaders;
	}


	/**
	 * @return the requestBody
	 */
	public String getRequestBody() {
		return requestBody;
	}


	/**
	 * @param requestBody the requestBody to set
	 */
	public void setRequestBody(String requestBody) {
		this.requestBody = requestBody;
	}
	

	/**
	 * @return the isRelayed
	 */
	public Boolean getIsRelayed() {
		return isRelayed;
	}


	/**
	 * @param isRelayed the isRelayed to set
	 */
	public void setIsRelayed(Boolean isRelayed) {
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
	 * @return the retryCount
	 */
	public Long getRetryCount() {
		return retryCount;
	}


	/**
	 * @param retryCount the retryCount to set
	 */
	public void setRetryCount(Long retryCount) {
		this.retryCount = retryCount;
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

	
    /* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		Message other = (Message) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}


	@Override
    public String toString() {
        return "Message [payload=" + payload + ", groupId=" + groupId
                + ", correlationId=" + correlationId + ", replyTo=" + replyTo
                + ", context=" + context + ", transactionId=" + transactionId
                + ", id=" + id + ", subscriberId=" + subscriberId
                + ", replyToHttpUri=" + replyToHttpUri + ", replyToHttpMethod="
                + replyToHttpMethod + ", destinationResponseStatus="
                + destinationResponseStatus + ", user=" + user + ", responseCode=" + responseCode
                + ", responseBody=" + responseBody + "," + headers.toString() + "]";
    }	
}
