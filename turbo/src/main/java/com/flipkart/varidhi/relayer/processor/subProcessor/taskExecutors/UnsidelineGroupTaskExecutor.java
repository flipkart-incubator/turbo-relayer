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
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.UnsidelineGroupTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 21-Jul-2015
 *
 */
public class UnsidelineGroupTaskExecutor extends ProcessorTaskExecutor<UnsidelineGroupTask> {
    ProcessorOutboundRepository messageRepository;
    SubProcessorRepository processorRepository;
    private Logger logger;// = LoggerFactory.getLogger(UnsidelineGroupTaskExecutor.class);

    public UnsidelineGroupTaskExecutor(String executorId,
        SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository) {
        super(executorId);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
        logger = LoggerFactory
            .getLogger(UnsidelineGroupTaskExecutor.class.getCanonicalName() + " " + executorId);
    }

    @Override public void execute(UnsidelineGroupTask processorTask) {
        String groupId = processorTask.getGroupId();
        logger.info("Control task received for groupId : " + groupId);
        try {
            messageRepository.markGroupUnsidelined(groupId);
            messageRepository
                .updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.DONE);
            processorRepository.markGroupAsUnsidelined(groupId);
            logger.info("Control task successfully executed for groupId : " + groupId);
        } catch (Exception e) {
            messageRepository
                .updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.FAILED);
            logger.error("Control task execution failed for groupId : " + groupId, e);
        }
    }
}
