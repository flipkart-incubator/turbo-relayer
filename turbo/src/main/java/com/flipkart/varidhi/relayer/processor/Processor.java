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

package com.flipkart.varidhi.relayer.processor;

import com.flipkart.varidhi.config.HashingAlgorithm;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.SubProcessor;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.turbo.tasks.ProcessorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/*
 * *
 * Author: abhinavp
 * Date: 02-Jul-2015
 *
 */
public class Processor {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    String processorId;
    //    String THREAD_GROUP = "Message_Processor_Thread";
    //    String EXECUTOR_GROUP = "Message_Processor_Executor";
    int subProcessorCount;
    int subProcessorQueueSize;
    //    int corePoolSize;
    //    int maxPoolSize;
    //    long keepAliveTime;
    int lastProcessedPersistInterval;
    Map<String, SubProcessor> subProcessors;
    ProcessorOutboundRepository messageRepository;
    ProcessorMetadataRepository processorMetadataRepository;
    private HashingAlgorithm hashingAlgorithm;
    SubProcessorMaxSeqPersister subProcessorMaxSeqPersister;
    QpsCollector qpsCollector;
    private static final int MAX_SUPROCESSOR_THREAD_COUNT = 50;

    public Processor(String processorId,String relayerId, int subProcessorCount, int subProcessorQueueSize,
                     int lastProcessedPersistInterval, ProcessorOutboundRepository messageRepository,
                     ProcessorMetadataRepository processorMetadataRepository, MessageRelayer messageRelayer, HashingAlgorithm hashingAlgorithm,
                     HashMap <String , Long> subprocessorToMessageIdMap,@NotNull Relayer relayer) {
        this.processorId = processorId;
        this.subProcessorCount = subProcessorCount;
        this.subProcessorQueueSize = subProcessorQueueSize;
        this.lastProcessedPersistInterval = lastProcessedPersistInterval;
        this.messageRepository = messageRepository;
        this.processorMetadataRepository = processorMetadataRepository;
        this.hashingAlgorithm = hashingAlgorithm;
        subProcessors = new HashMap<>();
        for (Integer index = 0; index < subProcessorCount; index++) {
            String subprocessorName = processorId + "_SP_" + index.toString();
            SubProcessor subProcessor = new SubProcessor(subprocessorName,
                this.subProcessorQueueSize, messageRepository, messageRelayer , subprocessorToMessageIdMap.get(subprocessorName),
                    relayer);
            subProcessors.put(index.toString(), subProcessor);
        }

        subProcessorMaxSeqPersister = new SubProcessorMaxSeqPersister(this.processorId,
            new ArrayList<>(subProcessors.values()), processorMetadataRepository,
            this.lastProcessedPersistInterval,relayer.getRelayerMetrics());
        Thread sequenceCollectorThread = new Thread(subProcessorMaxSeqPersister,"SubProcessorMaxSeqPersister_thread");
        sequenceCollectorThread.start();

        qpsCollector =
            new QpsCollector(this.processorId,relayerId, new ArrayList<>(subProcessors.values()));

    }

    public void submitTask(ProcessorTask processorTask) {
        if(subProcessors.isEmpty()) {
            logger.error("Processor is shutting down, discarding any new tasks");
            return;
        }
        subProcessors.get(getProcessorId(processorTask)).submitTask(processorTask);
    }

    public void publishSubProcessorRemainingQueueSize() {
        if(qpsCollector != null) {
            qpsCollector.publishRemainingQueueSize();
        }
    }

    private String getProcessorId(ProcessorTask processorTask) {
        int processIdInt = Math.abs(hashingAlgorithm.getHashFunction().apply(processorTask.sequencingKey())) % subProcessorCount;
        if (processIdInt < 0) {
            return "0";
        } else {
            return Integer.toString(processIdInt);
        }
    }

    //    public List<String> getLastProcessedMessageIdsWithSystemExit()
    //    {
    //        return processorMetadataRepository.getLastProcessedMessageIdsWithSystemExit();
    //    }

    public Long numberOfMessagesProcessedFromLastLookup() {
        return qpsCollector.collectMessageCount();
    }


    public int isDbDead() {
        return qpsCollector.isDbDead();
    }
  
    public HashMap< String , Long> getMessageLagTime() {
        return qpsCollector.getMessageMaxAndAvgLagTime();
    }

    public void stopExecution() {
        logger.info("Stopping subProcessors of Processor : " + processorId);
        //executor service to stop all the sub processors in a parallel fashion
        // only create new threads in a particular amount otherwise can throw a memory error.
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_SUPROCESSOR_THREAD_COUNT < subProcessorCount ? MAX_SUPROCESSOR_THREAD_COUNT : subProcessorCount);;

        if (subProcessors != null && subProcessors.size() != 0) {
            //adding the tasks to futures list and stopping all the parallel running sub processors .
            List <Future<?>> futures = new ArrayList<>();
            //adding the tasks to the executor service so that they can run in parallel.
            for (SubProcessor subProcessor : subProcessors.values()) {
                futures.add( executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        subProcessor.shutdown();
                    }
                }));
            }
            //checking whether the runnable tasks assigned to the executor service is finished or not (Blocking main thread).
            for (Future<?> future:futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        //stopping the executor service.
        executorService.shutdown();
        logger.info("Successfully stopped subProcessors of Processor :" + processorId);
        logger.info("Stopping maxSeqPersister of Processor :" + processorId);
        subProcessorMaxSeqPersister.stop();
        logger.info("Successfully stopped maxSeqPersister of Processor :" + processorId);
        subProcessors.clear();

    }

    public void haltExecution() {
        logger.info("Halting subProcessors of Processor : " + processorId);
        //executor service to stop all the sub processors in a parallel fashion
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(MAX_SUPROCESSOR_THREAD_COUNT,subProcessorCount));

        if (subProcessors != null && subProcessors.size() != 0) {
            List <Future<?>> futures = new ArrayList<>();
            for (SubProcessor subProcessor : subProcessors.values()) {
                futures.add( executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        subProcessor.halt();
                    }
                }));
            }
            //checking whether the runnable tasks assigned to the executor service is finished or not (Blocking main thread).
            for (Future<?> future:futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("exception occurred in haltExecution",e);
                }
            }
        }
        //stopping the executor service.
        executorService.shutdown();
        logger.info("Successfully halted subProcessors of Processor :" + processorId);
        logger.info("Stopping maxSeqPersister of Processor :" + processorId);
        subProcessorMaxSeqPersister.stop();
        logger.info("Successfully stopped maxSeqPersister of Processor :" + processorId);
        subProcessors.clear();

    }
}
