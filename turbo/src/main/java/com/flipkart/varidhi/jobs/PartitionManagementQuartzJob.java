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

package com.flipkart.varidhi.jobs;

import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.relayer.Relayer;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionManagementQuartzJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManagementQuartzJob.class);

    @Override
    public void execute(JobExecutionContext context){
        SchedulerContext schedulerContext;
        try {
            schedulerContext = context.getScheduler().getContext();
        } catch (SchedulerException e) {
            logger.error(String.format("error occurred while getting context job details : %s, error : ",context.getJobDetail()),e);
            return;
        }
        RelayerHandleContainer handleContainer = (RelayerHandleContainer)
                schedulerContext.get(RelayerHandleContainer.class.getCanonicalName());
        String relayerId = context.getJobDetail().getKey().getGroup();
        logger.info("Started Partition Management for relayer: " + relayerId);
        Relayer relayer = handleContainer.getRelayerHandle(relayerId);
        if(relayer == null) {
            logger.error("Relayer not found. Not running Partition Management for relayer: " + relayerId);
            return;
        }
        relayer.runPartitionManagementJob();
    }
}
