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

package com.flipkart.varidhi.relayer.processor.tasks;

/*
 * *
 * Author: abhinavp
 * Date: 03-Jul-2015
 *
 */

import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;


@Getter @Setter public class RelayMessageTask extends BaseRelayMessageTask {
    public RelayMessageTask(Serializable taskId, String groupId, String messageId,
        String messageData, String exchangeName, String exchangeType, String httpMethod,
        String httpUri, String customHeaders, String replyTo, String replyToHttpMethod,
        String replyToHttpUri, String transactionId, String correlationId,
        String destinationResponseStatus , Timestamp createDateTime , String varidhiServer) {
        this(taskId, null, groupId, messageId, messageData, exchangeName, exchangeType, httpMethod,
                httpUri, customHeaders, replyTo, replyToHttpMethod, replyToHttpUri, transactionId,
                correlationId, destinationResponseStatus, createDateTime, varidhiServer);

    }
    private Long appSequenceId;
    public RelayMessageTask(Serializable taskId, Long appSequenceId, String groupId, String messageId,
                            String messageData, String exchangeName, String exchangeType, String httpMethod,
                            String httpUri, String customHeaders, String replyTo, String replyToHttpMethod,
                            String replyToHttpUri, String transactionId, String correlationId,
                            String destinationResponseStatus, Timestamp createDateTime , String varidhiServer) {
        super(taskId, groupId, messageId, messageData, exchangeName, exchangeType, httpMethod,
                httpUri, customHeaders, replyTo, replyToHttpMethod, replyToHttpUri, transactionId,
                correlationId, destinationResponseStatus ,varidhiServer ,createDateTime);
        this.appSequenceId = appSequenceId;
    }
}
