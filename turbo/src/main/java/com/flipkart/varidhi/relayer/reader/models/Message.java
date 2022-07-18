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

/*
 * *
 * Author: abhinavp
 * Date: 30-Jul-2015
 *
 */

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter @Setter public class Message extends BaseReadDomain {
    String groupId;
    String messageId;
    String messageData;
    String exchangeName;
    String exchangeType;
    String appId;
    String httpMethod;
    String httpUri;
    String customHeaders;
    String replyTo;
    String replyToHttpMethod;
    String replyToHttpUri;
    String trasactionId;
    String correlationId;
    String destinationResponseStatus;
    Timestamp createDateTime;

}
