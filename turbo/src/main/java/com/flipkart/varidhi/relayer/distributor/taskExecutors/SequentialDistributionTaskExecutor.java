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

package com.flipkart.varidhi.relayer.distributor.taskExecutors;

import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.relayer.distributor.tasks.SequentialDistributionTask;
import com.flipkart.varidhi.relayer.distributor.tasks.output.DistributorOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * *
 * Author: abhinavp
 * Date: 06-Jul-2015
 *
 */
public class SequentialDistributionTaskExecutor<T>
    extends DistributorTaskExecutor<SequentialDistributionTask<T>> {
    private static final Logger logger =
        LoggerFactory.getLogger(SequentialDistributionTaskExecutor.class);

    AtomicInteger shouldProcess = new AtomicInteger(1);

    private final RelayerMetrics relayerMetrics;

    public SequentialDistributionTaskExecutor(RelayerMetrics relayerMetrics) {
        this.relayerMetrics = relayerMetrics;
    }

    @Override public void execute(SequentialDistributionTask readerTask) {
        BlockingQueue<T> queue = readerTask.getQueue();
        DistributorOutput distributorOutput = readerTask.getDistributorOutput();

        T elem;

        try {
            shouldProcess = new AtomicInteger(1);

            while ((elem = queue.take()) != null && shouldProcess.get() == 1) {
                distributorOutput.submit(elem);
                relayerMetrics.updateLastDistributionEpochTime(System.currentTimeMillis());
            }

            logger.warn("Stop signal received by distributor. Discarding the value in the InMemoryQueue (not draining it).");

            shouldProcess.decrementAndGet();
        } catch (InterruptedException e) {
            logger.error("SequentialDistributionTaskExecutor::execute::Error in Distribution::" + e.getMessage(), e);
        }
    }

    @Override public void stopExecution() {
        shouldProcess.incrementAndGet();
    }

    @Override public void haltExecution() {
        shouldProcess.incrementAndGet();
    }
}
