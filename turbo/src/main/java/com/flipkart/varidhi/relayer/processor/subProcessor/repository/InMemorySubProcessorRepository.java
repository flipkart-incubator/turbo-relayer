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

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;


/*
 * *
 * Author: abhinavp
 * Date: 23-Jul-2015
 *
 */
public class InMemorySubProcessorRepository implements SubProcessorRepository {
    Pair<Long, String> lastProcessedMessage;
    Map<String, Long> lastUnsidelinedMessages = new HashMap<>();
    Map<String, GroupStatus> groupStatus = new HashMap<>();
    private long numberOfMessageProcessed = 0;
    private Boolean isDbDead = false;

    @Setter @Getter
    public Long messageLagTime = null;

    @Override public void updateLastProccesedMessageId(Pair<Long, String> seqIdMessageIdPair) {
        lastProcessedMessage = seqIdMessageIdPair;
        numberOfMessageProcessed++;
    }

    @Override public Pair<Long, String> getLastProcessedMessageId() {
        return lastProcessedMessage;
    }

    @Override public void markGroupAsUnsidelined(String groupId) {
        groupStatus.put(groupId, GroupStatus.UNSIDELINED);
    }

    @Override public GroupStatus groupStatus(String groupId) {
        //return groupStatus.containsKey(groupId)? groupStatus.get(groupId): GroupStatus.UNSIDELINED;
        return groupStatus.get(groupId);
    }

    @Override public void updateLastSidelinedSeqId(String groupId, Long sequenceId,
        GroupStatus currentStatus) {
        groupStatus.put(groupId, currentStatus);
        lastUnsidelinedMessages.put(groupId, sequenceId);
    }

    @Override public Long getLastSidelinedSeqId(String groupId) {
        return lastUnsidelinedMessages.get(groupId);
    }

    @Override public void removeSidelinedGroupEntry(String groupId) {
        lastUnsidelinedMessages.remove(groupId);
        groupStatus.remove(groupId);
    }

    @Override public long getNumberOfMessageProcessed() {
        return this.numberOfMessageProcessed;
    }


    @Override  public void setSidelineDbFailure(Boolean retryStatus){
        this.isDbDead = retryStatus;
    }

    @Override public Boolean isDbDead(){
        return this.isDbDead;
    }
}
