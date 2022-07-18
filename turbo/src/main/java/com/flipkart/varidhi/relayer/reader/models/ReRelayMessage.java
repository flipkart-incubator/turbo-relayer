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

package com.flipkart.varidhi.relayer.reader.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;

@Getter @Setter @AllArgsConstructor @Entity @NoArgsConstructor public class ReRelayMessage extends Message {
    long appSequenceId;

    public ReRelayMessage(Message message, long appSequenceId) {
        this.id = message.getId();
        this.groupId = message.getGroupId();
        this.messageId = message.getMessageId();
        this.messageData = message.getMessageData();
        this.exchangeName = message.getExchangeName();
        this.exchangeType = message.exchangeType;
        this.appId = message.getAppId();
        this.httpMethod = message.getHttpMethod();
        this.httpUri = message.getHttpUri();
        this.customHeaders = message.getCustomHeaders();
        this.replyTo = message.getReplyTo();
        this.replyToHttpMethod = message.getReplyToHttpMethod();
        this.replyToHttpUri = message.getReplyToHttpUri();
        this.trasactionId = message.getTrasactionId();
        this.correlationId = message.getCorrelationId();
        this.destinationResponseStatus = message.getDestinationResponseStatus();
        this.createDateTime = message.getCreateDateTime();
        this.appSequenceId = appSequenceId;
    }

    @Override public String toString() {
        return "ReRelayMessage{" +
                "appSequenceId='" + appSequenceId + '\'' +
                ", id='" + id + '\'' +
                ", groupId='" + groupId + '\'' +
                ", messageId='" + messageId +
                '}';
    }
}