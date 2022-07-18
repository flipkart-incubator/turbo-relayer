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
import com.flipkart.varidhi.relayer.processor.tasks.UnsidelineUngroupedForPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 04-Aug-2015
 *
 */
public class UnsidelineUngroupedForPeriodTaskExecutor
    extends ProcessorTaskExecutor<UnsidelineUngroupedForPeriodTask> {
    SubProcessorRepository processorRepository;
        // = LoggerFactory.getLogger(UnsidelineGroupTaskExecutor.class.getCanonicalName());
    ProcessorOutboundRepository messageRepository;
    private Logger logger;

    public UnsidelineUngroupedForPeriodTaskExecutor(String executorId,
        SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository) {
        super(executorId);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
        logger = LoggerFactory
            .getLogger(UnsidelineGroupTaskExecutor.class.getCanonicalName() + " " + executorId);
    }

    @Override public void execute(UnsidelineUngroupedForPeriodTask processorTask) {
        logger.info(
            "Control task received for ungrouped messages from " + processorTask.getFromDate()
                + " to " + processorTask.getToDate());
        try {
            messageRepository.unsidelineAllUngroupedMessage(processorTask.getFromDate(),
                processorTask.getToDate());
            messageRepository
                .updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.DONE);
            logger.info(
                "Control task successfully executed for ungrouped messages from " + processorTask
                    .getFromDate() + " to " + processorTask.getToDate());
        } catch (Exception e) {
            messageRepository
                .updateControlTaskStatus(processorTask.getId(), ControlTaskStatus.FAILED);
            logger.error(
                "Failed to execute control task for ungrouped messages from " + processorTask
                    .getFromDate() + " to " + processorTask.getToDate(), e);
        }
    }
}
