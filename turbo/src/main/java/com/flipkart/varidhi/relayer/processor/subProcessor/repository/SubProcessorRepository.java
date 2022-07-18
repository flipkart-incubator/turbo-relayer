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

package com.flipkart.varidhi.relayer.processor.subProcessor.repository;

import com.flipkart.varidhi.relayer.common.GroupStatus;
import com.flipkart.varidhi.relayer.common.Pair;

/*
 * *
 * Author: abhinavp
 * Date: 23-Jul-2015
 *
 */
public interface SubProcessorRepository {
    void updateLastProccesedMessageId(Pair<Long, String> seqIdMessageIdPair);

    Pair<Long, String> getLastProcessedMessageId();

    void markGroupAsUnsidelined(String groupId);

    GroupStatus groupStatus(String groupId);

    void updateLastSidelinedSeqId(String groupId, Long sequenceId, GroupStatus status);

    Long getLastSidelinedSeqId(String groupId);

    void removeSidelinedGroupEntry(String groupId);

    long getNumberOfMessageProcessed();


    void setSidelineDbFailure(Boolean retryStatus);

    Boolean isDbDead();

    void setMessageLagTime(Long lagTime);

    Long getMessageLagTime();
}
