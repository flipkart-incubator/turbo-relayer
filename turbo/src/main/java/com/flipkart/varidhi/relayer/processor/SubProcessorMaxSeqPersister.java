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

import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.relayer.processor.subProcessor.SubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * *
 * Author: abhinavp
 * Date: 20-Jul-2015
 *
 */
public class SubProcessorMaxSeqPersister implements Runnable {
    ProcessorMetadataRepository processorMetadataRepository;
    String processorId;
    Map<String, String> subProcessorCheckpointMap = new HashMap<>();
    private Logger logger;
    private List<SubProcessor> subProcessors;
    private long writeInterval = 10000;
    private AtomicInteger shouldProcess = new AtomicInteger(1);
    private Boolean initialPersistenceHappened = false;
    private Boolean oldProcessorsCleard = false;
    private final RelayerMetrics relayerMetrics;

    public SubProcessorMaxSeqPersister(String processorId, List<SubProcessor> subProcessors,
        ProcessorMetadataRepository processorMetadataRepository, long writeInterval,RelayerMetrics relayerMetrics) {
        logger = LoggerFactory
            .getLogger(SubProcessorMaxSeqPersister.class.getCanonicalName() + " " + processorId);
        this.processorId = processorId;
        this.subProcessors = subProcessors;
        this.processorMetadataRepository = processorMetadataRepository;
        this.writeInterval = writeInterval;
        this.relayerMetrics = relayerMetrics;
        registerProcessors(this.subProcessors);
    }

    @Override public void run() {
        while (shouldProcess.get() == 1) {
            try {
                Thread.sleep(this.writeInterval);
                updateMaxSequenceId();
                if (initialPersistenceHappened && !oldProcessorsCleard) {
                    clearOldProcessorRecords();
                    oldProcessorsCleard = true;
                }
                relayerMetrics.updateLastPersistedEpochTime(System.currentTimeMillis());
            } catch (Exception e) {
                logger.error("SubProcessorMaxSeqPersister::run::Error in thread sleep", e);
            }
        }
        relayerMetrics.updateLastPersistedEpochTime(null);
    }

    private void registerProcessors(List<SubProcessor> subProcessors) {
        List<String> processorIds = new ArrayList<>();
        for (SubProcessor subProcessor : subProcessors) {
            String id = subProcessor.getId();
            processorIds.add(id);
        }

        Set<String> alreadyRegisteredProcesses =
            processorMetadataRepository.getRegisteredProcesses(processorIds);
        List<String> newProcesses = new ArrayList<>();
        for (String processId : processorIds) {
            if (!alreadyRegisteredProcesses.contains(processId))
                newProcesses.add(processId);
        }
        if (newProcesses.size() == 0)
            return;

        logger.debug("Registering new processes with ids : " + newProcesses);
        processorMetadataRepository.registerNewProcess(newProcesses);

    }

    private void updateMaxSequenceId() {

        for (SubProcessor subProcessor : subProcessors) {
            String lastProccessedMessageId = subProcessor.getMaxProccessedMessageId();
            if (lastProccessedMessageId != null && !lastProccessedMessageId
                .equals(subProcessorCheckpointMap.get(subProcessor.getId()))) {
                logger.debug(
                    "Updating last processed message id for subProcessor :" + subProcessor.getId()
                        + " messageId : " + lastProccessedMessageId);
                processorMetadataRepository
                    .persistLastProcessedMessageId(subProcessor.getId(), lastProccessedMessageId,
                        null);
                subProcessorCheckpointMap.put(subProcessor.getId(), lastProccessedMessageId);
                initialPersistenceHappened = true;
            }
        }
    }

    private void clearOldProcessorRecords() {
        List<String> subProcessorIds = new ArrayList<>();
        for (SubProcessor subProcessor : subProcessors) {
            subProcessorIds.add(subProcessor.getId());
        }
        logger.debug("Clearing old processors records with Ids :" + subProcessorIds);
        processorMetadataRepository.clearOldProcessorRecords(subProcessorIds);
    }

    public void stop() {
        updateMaxSequenceId();
        shouldProcess.decrementAndGet();
    }


}
