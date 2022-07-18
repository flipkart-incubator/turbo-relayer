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

import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.tasks.PartitionManagementTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionManagementTaskExecutor
        extends ProcessorTaskExecutor<PartitionManagementTask> {

    private final Logger logger;
    private ProcessorOutboundRepository messageRepository;

    public PartitionManagementTaskExecutor(String executorId, ProcessorOutboundRepository messageRepository) {
        super(executorId);
        this.messageRepository = messageRepository;
        logger = LoggerFactory.getLogger(PartitionManagementTaskExecutor.class.getCanonicalName() + " " + executorId);
    }

    @Override
    public void execute(PartitionManagementTask processorTask) {

        try {
            processorTask.getPartitionManagementJob().run();
            messageRepository.updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.DONE);
            logger.debug("Control task successfully executed for Partition Management for " + processorTask.getRelayerId());
        } catch (Exception e) {
            messageRepository.updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.FAILED);
            logger.error("Failed to execute control task for Partition Management for " + processorTask.getRelayerId());
        }

    }
}
