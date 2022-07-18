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
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.RelayUnsidelinedMessageTask;

/*
 * *
 * Author: abhinavp
 * Date: 23-Jul-2015
 *
 */
public class RelayUnsidelinedMessageTaskExecutor
    extends AbstractRelayMessageTaskExecutor<RelayUnsidelinedMessageTask> {
    // private static final Logger logger = LoggerFactory.getLogger(RelayUnsidelinedMessageTaskExecutor.class);

    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;

    public RelayUnsidelinedMessageTaskExecutor(String executorId,
                                               SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository, MessageRelayer messageRelayer) {
        super(executorId, processorRepository, messageRepository, messageRelayer);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
    }

    @Override void relayUngroupedMessage(RelayUnsidelinedMessageTask relayTask) throws Exception {
        relayMessage(relayTask);
        messageRepository.deleteSidelinedMessage(relayTask.getMessageId());

    }

    @Override void relayGroupedMessage(RelayUnsidelinedMessageTask relayTask) throws Exception {
        relayMessage(relayTask);
        messageRepository.deleteSidelinedMessage(relayTask.getMessageId());

    }

    @Override void putMessageInSidelinedQueueWithGroupStatus(
        RelayUnsidelinedMessageTask relayTask) {
        GroupStatus status = processorRepository.groupStatus(relayTask.getGroupId());
        if(status == GroupStatus.SIDELINED) {
            sidelineMessage(relayTask, SidelineReasonCode.GROUP_SIDELINED);
        }else {
            putMessageInSidelineQueue(relayTask, SidelineReasonCode.GROUP_SEQ_MAINTENANCE,
                SidelinedMessageStatus.UNSIDELINED, status);
        }

    }

    @Override void relayLastUndsidelinedMessageForGroup(RelayUnsidelinedMessageTask relayTask)
        throws Exception {
        relayMessage(relayTask);
        processorRepository.removeSidelinedGroupEntry(relayTask.getGroupId());
        messageRepository.deleteSidelinedMessage(relayTask.getMessageId());
    }

    @Override void sidelineMessageOnRelayFailure(RelayUnsidelinedMessageTask relayTask,
        SidelineReasonCode sidelineReasonCode, int statusCode, String message, int retries) {
        sidelineMessage(relayTask, sidelineReasonCode, statusCode, message,
            maxRelayRetry);
    }

}
