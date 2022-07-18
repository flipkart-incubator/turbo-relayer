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


import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.RelayMessageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReRelayMessageTaskExecutor extends RelayMessageTaskExecutor {
    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;
    private Long lastRelayedMessageID;
    private static Logger logger;

    public ReRelayMessageTaskExecutor(String executorId, SubProcessorRepository processorRepository,
                                      ProcessorOutboundRepository messageRepository, MessageRelayer messageRelayer,
                                      Long lastRelayedMessageID) {
        super(executorId, processorRepository, messageRepository, messageRelayer);
        this.messageRepository = messageRepository;
        this.processorRepository = processorRepository;
        logger = LoggerFactory.getLogger(ReRelayMessageTaskExecutor.class.getCanonicalName() + " " + executorId);
        this.lastRelayedMessageID = lastRelayedMessageID;
    }

    @Override
    protected Boolean relayMessage(RelayMessageTask messageTask) throws Exception {
        if (lastRelayedMessageID == null || messageTask.getAppSequenceId() > lastRelayedMessageID) {
            logger.info("Relaying ReRelayMessage with message_id " + messageTask.getMessageId() + " and group_id " + messageTask.getGroupId());
            return super.relayMessage(messageTask);
        }
        logger.warn("MESSAGEID:" + messageTask.getMessageId() +" GROUPID:" + messageTask.getGroupId()
                + " ACTION:Skipping STATUS:successful (Already Relayed Messages that's why skipping it) PAYLOAD:" + messageTask.getMessageData() );
        return false;
    }

}

