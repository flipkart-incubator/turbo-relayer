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
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.InformExistingSidelinedGroupTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 10-Aug-2015
 *
 */
public class InformExistingSidelinedGroupTaskExecutor
    extends ProcessorTaskExecutor<InformExistingSidelinedGroupTask> {
    SubProcessorRepository subProcessorRepository;
    private Logger logger;
        // = LoggerFactory.getLogger(InformExistingSidelinedGroupTaskExecutor.class);

    public InformExistingSidelinedGroupTaskExecutor(String executorId,
        SubProcessorRepository subProcessorRepository) {
        super(executorId);
        this.subProcessorRepository = subProcessorRepository;
        logger = LoggerFactory.getLogger(
            InformExistingSidelinedGroupTaskExecutor.class.getCanonicalName() + " " + executorId);

    }

    @Override public void execute(InformExistingSidelinedGroupTask processorTask) {
        // Assumption : Actual sequencing will start from at least 0.
        logger.debug(
            "Received group as already sidelined group with id : " + processorTask.getGroupId());
        subProcessorRepository
            .updateLastSidelinedSeqId(processorTask.getGroupId(), -1L, GroupStatus.SIDELINED);
    }
}
