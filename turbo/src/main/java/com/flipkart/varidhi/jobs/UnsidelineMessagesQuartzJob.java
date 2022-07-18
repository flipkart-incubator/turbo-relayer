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
import com.flipkart.varidhi.core.RelayerMetricHandleContainer;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.resources.ControlTaskInput;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnsidelineMessagesQuartzJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(UnsidelineMessagesQuartzJob.class);

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
        Relayer relayer = handleContainer.getRelayerHandle(relayerId);
        if (!relayer.isActive() || !relayer.isRunning()) {
            logger.warn("UnsidelineMessagesQuartzJob cannot be initiated for non active relayer.");
            return;
        }
        logger.info(String.format("running UnsidelineMessagesQuartzJob for relayer '%s', job details '%s'",
                relayer.getRelayerId(),context.getJobDetail()));
        ControlTaskInput controlTaskInput = ControlTaskInput.getDefault();
        relayer.createUnsidelineMessagesBetweenDatesTask(controlTaskInput.getFromDate(),
                controlTaskInput.getToDate());
        relayer.createUnsidelineAllUngroupedMessageTask(controlTaskInput.getFromDate(),
                controlTaskInput.getToDate());
        logger.info(String.format("UnsidelineMessagesQuartzJob complete for relayer '%s', job details '%s'",
                relayer.getRelayerId(),context.getJobDetail()));
    }
}
