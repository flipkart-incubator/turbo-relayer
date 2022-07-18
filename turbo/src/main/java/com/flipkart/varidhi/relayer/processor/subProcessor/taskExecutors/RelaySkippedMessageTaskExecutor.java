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
import com.flipkart.varidhi.relayer.processor.tasks.RelaySkippedMessageTask;

/*
 * *
 * Author: abhinavp
 * Date: 31-Jul-2015
 *
 */
public class RelaySkippedMessageTaskExecutor
    extends AbstractRelayMessageTaskExecutor<RelaySkippedMessageTask> {
    //private static final Logger logger = LoggerFactory.getLogger(RelaySkippedMessageTaskExecutor.class);

    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;

    public RelaySkippedMessageTaskExecutor(String executorId,
                                           SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository, MessageRelayer messageRelayer) {
        super(executorId, processorRepository, messageRepository, messageRelayer);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
    }

    @Override void relayUngroupedMessage(RelaySkippedMessageTask relayTask) throws Exception {
        relayMessage(relayTask);
        messageRepository.deleteSkippedMessage(relayTask.getAppSequenceId());
    }

    @Override void relayGroupedMessage(RelaySkippedMessageTask relayTask) throws Exception {
        relayMessage(relayTask);
        messageRepository.deleteSkippedMessage(relayTask.getAppSequenceId());
    }

    @Override void putMessageInSidelinedQueueWithGroupStatus(RelaySkippedMessageTask relayTask) {
        GroupStatus status = processorRepository.groupStatus(relayTask.getGroupId());
        if(status == GroupStatus.SIDELINED) {
            sidelineMessage(relayTask, SidelineReasonCode.GROUP_SIDELINED);
        } else {
            putMessageInSidelineQueue(relayTask, SidelineReasonCode.GROUP_SEQ_MAINTENANCE,
                SidelinedMessageStatus.UNSIDELINED, status);
        }
        //Deleting the message from Skipped Id Table.
        messageRepository.deleteSkippedMessage(relayTask.getAppSequenceId());
    }

    @Override void relayLastUndsidelinedMessageForGroup(RelaySkippedMessageTask relayTask)
        throws Exception {
        relayMessage(relayTask);
        processorRepository.removeSidelinedGroupEntry(relayTask.getGroupId());
        messageRepository.deleteSkippedMessage(relayTask.getAppSequenceId());
    }

    @Override void sidelineMessageOnRelayFailure(RelaySkippedMessageTask relayTask,
        SidelineReasonCode sidelineReasonCode, int statusCode, String message, int retries) {
        //TODO: Do in single transaction
        sidelineMessage(relayTask, sidelineReasonCode, statusCode, message,
            maxRelayRetry);
        messageRepository.deleteSkippedMessage(relayTask.getAppSequenceId());
    }

}
