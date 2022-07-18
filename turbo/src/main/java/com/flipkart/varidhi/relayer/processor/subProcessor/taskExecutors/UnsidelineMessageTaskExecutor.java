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
import com.flipkart.varidhi.relayer.processor.tasks.UnsidelineMessageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 04-Aug-2015
 *
 */
public class UnsidelineMessageTaskExecutor extends ProcessorTaskExecutor<UnsidelineMessageTask> {
    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;
    private Logger logger;// = LoggerFactory.getLogger(UnsidelineGroupTaskExecutor.class);

    public UnsidelineMessageTaskExecutor(String executorId,
        SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository) {
        super(executorId);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
        logger = LoggerFactory
            .getLogger(UnsidelineMessageTaskExecutor.class.getCanonicalName() + " " + executorId);
    }

    @Override public void execute(UnsidelineMessageTask unsidelineMessageTask) {
        logger
            .info("Control task received for messageId : " + unsidelineMessageTask.getMessageId());

        try {
            messageRepository.unsidelineMessage(unsidelineMessageTask.getMessageId());
            messageRepository
                .updateControlTaskStatus(unsidelineMessageTask.getId(), ControlTaskStatus.DONE);
            logger.info(
                "Control task successfully executed for messageId : " + unsidelineMessageTask
                    .getMessageId());
        } catch (Exception e) {
            messageRepository
                .updateControlTaskStatus(unsidelineMessageTask.getId(), ControlTaskStatus.FAILED);
            logger.error("Failed to execute Control task for messageId : " + unsidelineMessageTask
                .getMessageId(), e);
        }
    }
}
