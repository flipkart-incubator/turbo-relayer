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
import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.core.ThreadPoolExecutorProvider;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.common.ProcessedMessageLagTime;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.atomic.AtomicInteger;


public class MetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    ThreadPoolExecutor threadPoolExecutor;
    RelayerMetricHandleContainer relayerMetricHandleContainer;
    RelayerHandleContainer relayerHandleContainer;
    long monitorSleepTimeInMs;
    AtomicInteger shouldPublishMetrics = new AtomicInteger(1);

    @Inject public MetricsCollector(RelayerHandleContainer relayerHandleContainer,
                                    RelayerMetricHandleContainer relayerMetricHandleContainer, long monitorSleepTimeInMs) {
        this.relayerMetricHandleContainer = relayerMetricHandleContainer;
        this.relayerHandleContainer = relayerHandleContainer;
        this.monitorSleepTimeInMs = monitorSleepTimeInMs;
    }

    public void startMetricsCollection() {
        if (threadPoolExecutor == null || !threadPoolExecutor.isShutdown()) {
            threadPoolExecutor = ThreadPoolExecutorProvider
                .provideExecutor("MetricsPublisher", "Relayer", 2, 2, 500, 10, new AbortPolicy(), logger);
        }
        threadPoolExecutor.submit(new MetricsCollectionTask());
    }

    public void stopMetricsCollection() {
        shouldPublishMetrics.decrementAndGet();
        threadPoolExecutor.shutdown();
    }


    private class MetricsCollectionTask implements Runnable {
        @Override public void run() {
            while (shouldPublishMetrics.get() == 1) {
                try {
                    for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
                        RelayerMetrics relayerMetrics = relayerMetricHandleContainer.getRelayerMetricsHandle(relayer.getRelayerId());
                        relayerMetrics.updateIsActive(relayer.isActive());
                        if(!relayer.isActive() || !relayer.isRunning()){
                            continue;
                        }
                        logger.debug("Collecting Metrics for "+relayer.getRelayerId());

                        Long processedCount = relayer.numberOfMessagesProcessedFromLastLookup();
                        relayerMetrics.updateNumberOfMessagesProcessed(processedCount);
                        //keeping the fail count here because if we keep  it in external metrics collector then even though relayer will stop
                        //but the value of the metric will not change .By keeping it here if the relayer stops there will not be any data points.
                        relayerMetrics.updateDbHealthStatus(relayer.isDbDead());

                        //Getting the value of the message lag time and then updating the values for the relayerMetric
                        HashMap <String , Long > messageLagTimeMap=relayer.getMessageLagTime();
                        relayerMetrics.updateMessageLagTime(messageLagTimeMap);
                        relayerMetrics.updateReaderQueueRemainingCapacity(relayer.getMainQueueRemainingCapacity());
                        relayer.publishSubProcessorRemainingQueueSize();


                        // external metrics
                        relayerMetrics.updatecurrentSidelinedMessagesCount(relayer.currentSidelinedMessageCount());
                        relayerMetrics.updatecurrentSidelinedGroupsCount(relayer.currentSidelinedGroupsCount());
                        relayerMetrics.updatePendingMessageCount(relayer.pendingMessageCount());
                        relayerMetrics.updateMaxPartitionIdVal(relayer.getMaximumPartitionId());
                        relayerMetrics.updateMaxMessageId(relayer.getMaxMessageID());
                        ProcessedMessageLagTime processedMessageLagTime = relayer.getProcessedMessageMinMaxLagTime();
                        relayerMetrics.updateLagTimeForMinMaxProcessedMessage(processedMessageLagTime.getLagTimeInMillsForMinMsg(),
                                processedMessageLagTime.getLagTimeInMillsForMaxMsg());


                        logger.debug("Collecting Metrics complete for "+relayer.getRelayerId());
                    }
                    Thread.sleep(monitorSleepTimeInMs);
                } catch (InterruptedException e) {
                    logger.error("MetricsCollectionTask::Thread Sleep Error::" + e.getMessage(), e);
                }
            }
            shouldPublishMetrics.incrementAndGet();
        }

    }

}
