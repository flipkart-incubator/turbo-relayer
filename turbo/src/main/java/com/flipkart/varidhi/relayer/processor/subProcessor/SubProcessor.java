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

package com.flipkart.varidhi.relayer.processor.subProcessor;

import com.flipkart.varidhi.core.ThreadPoolExecutorProvider;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.common.Pair;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.InMemorySubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.taskExecutors.ExecutorProvider;
import com.flipkart.varidhi.relayer.processor.subProcessor.taskExecutors.ProcessorTaskExecutor;
import com.flipkart.turbo.tasks.ProcessorTask;
import com.flipkart.varidhi.utils.LeadershipExpiryHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * *
 * Author: abhinavp
 * Date: 23-Jul-2015
 *
 */
public class SubProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SubProcessor.class);

    String id;
    int queueSize;
    ThreadPoolExecutor threadPoolExecutor;
    Map<String, Long> sidelinedGroupMaxMessageIdMap = new HashMap<>();
    Long lastProcessedMessageId;
    String THREAD_GROUP_PREFIX = "SubProcessor_";
    String threadGroup;
    ExecutorProvider executorProvider;
    Relayer relayer;
    private static final int TERMINATION_TIMEOUT_IN_SECONDS = 10;
    private static final int HALT_TIMEOUT_IN_SECONDS = 1;

    ProcessorOutboundRepository messageRepository;
    SubProcessorRepository processorRepository = new InMemorySubProcessorRepository();

    public SubProcessor(String id, int queueSize, ProcessorOutboundRepository messageRepository, MessageRelayer messageRelayer ,
                        Long lastRelayedMessageID,@NotNull Relayer relayer) {
        this.id = id;
        this.queueSize = queueSize;
        this.threadGroup = THREAD_GROUP_PREFIX + id;
        this.messageRepository = messageRepository;
        this.executorProvider = new ExecutorProvider(id, messageRepository, processorRepository, messageRelayer , lastRelayedMessageID);
        threadPoolExecutor = ThreadPoolExecutorProvider
            .provideExecutor(threadGroup, this.id, 1, 1, 1, this.queueSize,
                new SubProcessorWaitPolicy(this.id), logger);
        this.relayer = relayer;
    }

    public Integer getQueueRemainingCapacity() {
        if(threadPoolExecutor != null) {
            return threadPoolExecutor.getQueue().remainingCapacity();
        }
        return null;
    }

    public void submitTask(ProcessorTask processorTask) {
        if(relayer.getRelayerConfiguration().isLeaderElectionEnabled() &&
                LeadershipExpiryHolder.isLeadershipExpired(relayer.getRelayerUUID())){
            logger.error(String.format("SubProcessor :: Relayer lease expired halting relayer %s, uuid : %s",
                    relayer.getRelayerId(),relayer.getRelayerUUID()));
            relayer.halt();
        }
        ProcessorTaskExecutor processorTaskExecutor =
            executorProvider.getTaskExecutor(processorTask);
        SubProcessorJob processorJob = new SubProcessorJob(processorTaskExecutor, processorTask);
        threadPoolExecutor.submit(processorJob);

    }

    public void shutdown() {
        try {
            // Not to remove these lines since shutdown is still taking time because it executes each and every task in its queue
            // The code given below is to drain the sub processor queue.
            logger.warn("Draining the subprocessor queue for shutdown");
            BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
            List<Runnable> list = new ArrayList<>();
            queue.drainTo(list);
            if (threadPoolExecutor != null){
                threadPoolExecutor.shutdown();
                logger.info("Killing Subprocessor Thread :" +  this.id + "_" + threadGroup );
                //Graceful shutdown
                //Busy wait to check whether the thread has been killed or not
                //The thread will first complete its task and then terminates
                if (!threadPoolExecutor.awaitTermination(TERMINATION_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)){
                    logger.error("Threads didn't finish in 10 seconds! Trying force kill");
                    threadPoolExecutor.shutdownNow();
                    logger.info("Subprocessor Thread Killed:" +  this.id + "_" + threadGroup );
                }
                logger.info("Subprocessor Thread Stopped:" +  this.id + "_" + threadGroup );
            }
        }catch (Exception e) {
            logger.error("shutdown::Error during shutdown::" + e.getMessage(), e);
        }
    }

    public void halt() {
        try {
            logger.warn("Halting the subprocessor queue");
            BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
            List<Runnable> list = new ArrayList<>();
            queue.drainTo(list);
            if (threadPoolExecutor != null){
                threadPoolExecutor.shutdown();
                logger.info("Killing Subprocessor Thread :" +  this.id + "_" + threadGroup );
                //Graceful shutdown
                //Busy wait to check whether the thread has been killed or not
                //The thread will first complete its task and then terminates
                if (!threadPoolExecutor.awaitTermination(HALT_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)){
                    logger.error("Threads didn't finish in 1 seconds! Trying force kill");
                    threadPoolExecutor.shutdownNow();
                    logger.info("Subprocessor Thread Killed:" +  this.id + "_" + threadGroup );
                }
                logger.info("Subprocessor Thread Halted:" +  this.id + "_" + threadGroup );
            }
        }catch (Exception e) {
            logger.error("shutdown::Error during halt::" + e.getMessage(), e);
        }
    }

    public String getId() {
        return this.id;
    }

    public String getMaxProccessedMessageId() {
        Pair<Long, String> lastProcessedMessageIdPair =
            processorRepository.getLastProcessedMessageId();
        if (lastProcessedMessageIdPair != null)
            return lastProcessedMessageIdPair.getRight();
        return null;
    }

    public long getNumberOfMessageProcessed() {
        return processorRepository.getNumberOfMessageProcessed();
    }


    public Boolean isDbDead(){ return processorRepository.isDbDead(); }

    // Helper method to return the message lag time
    public Long getMessageLagTime(){
        return processorRepository.getMessageLagTime();
    }
}
