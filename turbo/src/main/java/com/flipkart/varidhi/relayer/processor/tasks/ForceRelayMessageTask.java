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

import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import lombok.Getter;

import java.sql.Timestamp;

/**
 * @author shah.dhruvik
 * @date 24/07/19.
 */

@Getter
public class ForceRelayMessageTask extends BaseRelayMessageTask {

    public ForceRelayMessageTask(long id, String groupId, String messageId, String messageData,
                                 String exchangeName, String exchangeType, String httpMethod, String httpUri,
                                 String customHeaders, String replyTo, String replyToHttpMethod, String replyToHttpUri,
                                 String transactionId, String correlationId, String destinationResponseStatus,
                                 Timestamp createDateTime, String varidhiServer) {
        super(id, groupId, messageId, messageData, exchangeName, exchangeType, httpMethod,
                httpUri, customHeaders, replyTo, replyToHttpMethod, replyToHttpUri, transactionId,
                correlationId, destinationResponseStatus ,varidhiServer ,createDateTime);
    }

}
