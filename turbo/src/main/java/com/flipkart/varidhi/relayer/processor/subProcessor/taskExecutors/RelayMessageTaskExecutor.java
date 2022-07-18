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

package com.flipkart.varidhi.relayer.processor.subProcessor.taskExecutors;

import com.flipkart.varidhi.relayer.common.GroupStatus;
import com.flipkart.varidhi.relayer.common.Pair;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.RelayMessageTask;

/*
 * *
 * Author: abhinavp
 * Date: 03-Jul-2015
 *
 */
public class RelayMessageTaskExecutor extends AbstractRelayMessageTaskExecutor<RelayMessageTask> {
    // private static final Logger logger = LoggerFactory.getLogger(RelayMessageTaskExecutor.class);

    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;

    public RelayMessageTaskExecutor(String executorId, SubProcessorRepository processorRepository,
                                    ProcessorOutboundRepository messageRepository, MessageRelayer messageRelayer) {
        super(executorId, processorRepository, messageRepository, messageRelayer);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
    }

    @Override void relayUngroupedMessage(RelayMessageTask relayTask) throws Exception {
        if (relayMessage(relayTask)) {
            processorRepository.updateLastProccesedMessageId(
                    Pair.of((Long) relayTask.getTaskId(), relayTask.getMessageId()));
        }
    }

    @Override void relayGroupedMessage(RelayMessageTask relayTask) throws Exception {
        if (relayMessage(relayTask)) {
            processorRepository.updateLastProccesedMessageId(
                    Pair.of((Long) relayTask.getTaskId(), relayTask.getMessageId()));
        }
    }

    @Override void putMessageInSidelinedQueueWithGroupStatus(RelayMessageTask relayTask) {
        GroupStatus status = processorRepository.groupStatus(relayTask.getGroupId());
        if(status == GroupStatus.SIDELINED) {
            sidelineMessage(relayTask, SidelineReasonCode.GROUP_SIDELINED);
        }else {
            putMessageInSidelineQueue(relayTask, SidelineReasonCode.GROUP_SEQ_MAINTENANCE,
                SidelinedMessageStatus.UNSIDELINED, status);
        }
        processorRepository.updateLastProccesedMessageId(
            Pair.of((Long) relayTask.getTaskId(), relayTask.getMessageId()));
    }

    @Override void relayLastUndsidelinedMessageForGroup(RelayMessageTask relayTask)
        throws Exception {
        if (relayMessage(relayTask)) {
            processorRepository.removeSidelinedGroupEntry(relayTask.getGroupId());
            processorRepository.updateLastProccesedMessageId(
                    Pair.of((Long) relayTask.getTaskId(), relayTask.getMessageId()));
        }
    }

    @Override void sidelineMessageOnRelayFailure(RelayMessageTask relayTask,
        SidelineReasonCode sidelineReasonCode, int statusCode, String message, int retries) {
        sidelineMessage(relayTask, sidelineReasonCode, statusCode, message,
            maxRelayRetry);
        processorRepository.updateLastProccesedMessageId(
            Pair.of((Long) relayTask.getTaskId(), relayTask.getMessageId()));
    }

}
