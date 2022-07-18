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

package com.flipkart.varidhi.relayer.distributor;

import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.relayer.distributor.taskExecutors.DistributorTaskExecutor;
import com.flipkart.varidhi.relayer.distributor.tasks.DistributorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 06-Jul-2015
 *
 */
public class Distributor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Distributor.class);

    DistributorTaskExecutor distributorTaskExecutor;
    DistributorTask distributorTask;
    String distributorId;

    public Distributor(String distributorId, DistributorTask distributorTask,RelayerMetrics relayerMetrics) {
        this.distributorId = distributorId;
        this.distributorTask = distributorTask;
        this.distributorTaskExecutor =
            DistributorTaskExecutorProvider.getTaskExecutor(distributorTask,relayerMetrics);
    }

    @Override public void run() {
        try {
            distributorTaskExecutor.execute(distributorTask);
        } catch (Exception e) {
            logger.error("Stopping Relayer::System.exit::Distributor::run::Exception in Distributor Task Executor::" + e.getMessage(), e);
            System.exit(-1);
        }
    }

    public void stop() {
        logger.info("Stopping distributor with id :" + this.distributorId);
        distributorTaskExecutor.stopExecution();
        logger.info("Distributor successfully stopped with id :" + this.distributorId);
    }

    public void halt() {
        logger.info("Halting distributor with id :" + this.distributorId);
        distributorTaskExecutor.haltExecution();
        logger.info("Distributor successfully halted with id :" + this.distributorId);
    }
}
