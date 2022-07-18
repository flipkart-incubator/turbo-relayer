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

package com.flipkart.varidhi.relayer.processor;

import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import org.hibernate.Session;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;

/*
 * *
 * Author: abhinavp
 * Date: 21-Jul-2015
 *
 */
public interface ProcessorOutboundRepository {
    void updateDestinationResponseStatus(String messageId, int statusCode);

    void markGroupUnsidelined(String group);

    void updateControlTaskStatus(Serializable id, ControlTaskStatus status);

    void insertOrupdateSidelinedMessageStatus(Serializable messageId, Serializable groupId,
        SidelineReasonCode sidelineReasonCode, int statusCode, String details, int retries,
        SidelinedMessageStatus status, Session session);

    void insertOrupdateSidelinedMessageStatus(Serializable messageId, Serializable groupId,
        SidelineReasonCode sidelineReasonCode, SidelinedMessageStatus status, Session session);

    boolean deleteSidelinedMessage(Serializable id);

    Boolean sidelineMessage(Serializable messageId, Serializable groupId,
        SidelineReasonCode sidelineReasonCode, int statusCode, String details, int retries);

    void updateSkippedMessageStatus(Long appSeqId, SkippedIdStatus status);

    void unsidelineMessage(Serializable messageId);

    void unsidelineAllUngroupedMessage(Date fromDate, Date toDate);

    void deleteSkippedMessage(Long appSeqId);

    int deleteControlTaskEntries(Date beforeDate,int deleteBatchSize);

    HashMap <String , String > getLastProcessedMessages();

    boolean checkOrPerformLeaderElection(String relayerUID,int timeoutInSeconds, int expiryInterval);
}
