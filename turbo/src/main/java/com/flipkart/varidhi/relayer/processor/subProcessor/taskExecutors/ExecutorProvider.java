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

import com.flipkart.turbo.tasks.ProcessorTask;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 24-Jul-2015
 *
 */
public class ExecutorProvider {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorProvider.class);

    RelayMessageTaskExecutor relayTaskExecutor;
    RelayUnsidelinedMessageTaskExecutor relayUnsidelinedMessageTaskExecutor;
    RelaySkippedMessageTaskExecutor relaySkippedMessageTaskExecutor;
    UnsidelineGroupTaskExecutor unsidelineGroupTaskExecutor;
    UnsidelineMessageTaskExecutor unsidelineMessageTaskExecutor;
    UnsidelineUngroupedForPeriodTaskExecutor unsidelineUngroupedForPeriodTaskExecutor;
    InformExistingSidelinedGroupTaskExecutor informExistingSidelinedGroupTaskExecutor;
    ReRelayMessageTaskExecutor reRelayMessageTaskExecutor;
    private PartitionManagementTaskExecutor partitionManagementTaskExecutor;
    private ForceRelayMessageTaskExecutor forceRelayMessageTaskExecutor;

    ProcessorOutboundRepository messageRepository;
    SubProcessorRepository processorRepository;
    private MessageRelayer messageRelayer;
    String executorId;
    private Long lastRelayedMessageID;

    public ExecutorProvider(String executorId, ProcessorOutboundRepository messageRepository,
                            SubProcessorRepository processorRepository, MessageRelayer messageRelayer,
                            Long lastRelayedMessageID) {
        this.executorId = executorId;
        this.messageRepository = messageRepository;
        this.processorRepository = processorRepository;
        this.messageRelayer = messageRelayer;
        this.lastRelayedMessageID = lastRelayedMessageID;
    }

    public ProcessorTaskExecutor getTaskExecutor(ProcessorTask processorTask) {
        if (processorTask instanceof ReRelayMessageTask) {
            if (reRelayMessageTaskExecutor == null) {
                reRelayMessageTaskExecutor =
                        new ReRelayMessageTaskExecutor(this.executorId, this.processorRepository,
                                this.messageRepository, messageRelayer, this.lastRelayedMessageID);
            }
            return reRelayMessageTaskExecutor;
        } else if (processorTask instanceof RelayMessageTask) {
            if (relayTaskExecutor == null)
                relayTaskExecutor =
                        new RelayMessageTaskExecutor(this.executorId, this.processorRepository,
                                this.messageRepository, messageRelayer);
            return relayTaskExecutor;
        } else if (processorTask instanceof RelayUnsidelinedMessageTask) {
            if (relayUnsidelinedMessageTaskExecutor == null)
                relayUnsidelinedMessageTaskExecutor =
                        new RelayUnsidelinedMessageTaskExecutor(this.executorId,
                                this.processorRepository, this.messageRepository, messageRelayer);
            return relayUnsidelinedMessageTaskExecutor;
        } else if (processorTask instanceof RelaySkippedMessageTask) {
            if (relaySkippedMessageTaskExecutor == null)
                relaySkippedMessageTaskExecutor =
                        new RelaySkippedMessageTaskExecutor(this.executorId, this.processorRepository,
                                this.messageRepository, messageRelayer);
            return relaySkippedMessageTaskExecutor;
        } else if (processorTask instanceof UnsidelineGroupTask) {
            if (unsidelineGroupTaskExecutor == null)
                unsidelineGroupTaskExecutor =
                        new UnsidelineGroupTaskExecutor(this.executorId, this.processorRepository,
                                this.messageRepository);
            return unsidelineGroupTaskExecutor;
        } else if (processorTask instanceof UnsidelineMessageTask) {
            if (unsidelineMessageTaskExecutor == null)
                unsidelineMessageTaskExecutor =
                        new UnsidelineMessageTaskExecutor(this.executorId, this.processorRepository,
                                this.messageRepository);
            return unsidelineMessageTaskExecutor;
        } else if (processorTask instanceof UnsidelineUngroupedForPeriodTask) {
            if (unsidelineUngroupedForPeriodTaskExecutor == null)
                unsidelineUngroupedForPeriodTaskExecutor =
                        new UnsidelineUngroupedForPeriodTaskExecutor(this.executorId,
                                this.processorRepository, this.messageRepository);
            return unsidelineUngroupedForPeriodTaskExecutor;
        } else if (processorTask instanceof InformExistingSidelinedGroupTask) {
            if (informExistingSidelinedGroupTaskExecutor == null)
                informExistingSidelinedGroupTaskExecutor =
                        new InformExistingSidelinedGroupTaskExecutor(this.executorId,
                                this.processorRepository);
            return informExistingSidelinedGroupTaskExecutor;
        } else if (processorTask instanceof ForceRelayMessageTask) {
            if (forceRelayMessageTaskExecutor == null)
                forceRelayMessageTaskExecutor =
                        new ForceRelayMessageTaskExecutor(this.executorId, this.messageRepository, this.messageRelayer);
            return forceRelayMessageTaskExecutor;
        } else if (processorTask instanceof PartitionManagementTask) {
            if (partitionManagementTaskExecutor == null)
                partitionManagementTaskExecutor =
                        new PartitionManagementTaskExecutor(this.executorId, this.messageRepository);
            return partitionManagementTaskExecutor;
        } else
            return null;
    }
}
