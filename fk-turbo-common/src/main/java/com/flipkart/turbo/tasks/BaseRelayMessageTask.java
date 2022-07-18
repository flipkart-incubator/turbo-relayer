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

package com.flipkart.turbo.tasks;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

/*
 * *
 * Author: abhinavp
 * Date: 05-Aug-2015
 *
 */
@Getter @Setter public abstract class BaseRelayMessageTask extends BaseGroupBasedSequencingTask {
    Serializable TaskId;
    String messageId;
    String messageData;
    String exchangeName;
    String exchangeType;
    String httpMethod;
    String httpUri;
    String customHeaders;
    String replyTo;
    String replyToHttpMethod;
    String replyToHttpUri;
    String transactionId;
    String correlationId;
    String destinationResponseStatus;
    String destinationServer;
    Timestamp createDateTime;

    protected BaseRelayMessageTask(Serializable TaskId, String groupId, String messageId,
        String messageData, String exchangeName, String exchangeType, String httpMethod,
        String httpUri, String customHeaders, String replyTo, String replyToHttpMethod,
        String replyToHttpUri, String transactionId, String correlationId,
        String destinationResponseStatus, String destinationServer,Timestamp createDateTime) {
        super(groupId);
        this.TaskId = TaskId;
        super.groupId = groupId;
        this.messageId = messageId;
        this.messageData = messageData;
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.httpMethod = httpMethod;
        this.httpUri = httpUri;
        this.customHeaders = customHeaders;
        this.replyTo = replyTo;
        this.replyToHttpMethod = replyToHttpMethod;
        this.replyToHttpUri = replyToHttpUri;
        this.transactionId = transactionId;
        this.correlationId = correlationId;
        this.destinationResponseStatus = destinationResponseStatus;
        this.destinationServer = destinationServer;
        this.createDateTime=createDateTime;
    }

    @Override
    protected String getAlternateGroupId() {
        return String.valueOf(messageId.hashCode());
    }
}
