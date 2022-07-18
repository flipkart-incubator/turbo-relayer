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

import com.flipkart.varidhi.core.RelayerMetric;
import com.flipkart.varidhi.relayer.processor.subProcessor.SubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/*
 * *
 * Author: abhinavp
 * Date: 17-Sep-2015
 *
 */
public class QpsCollector /*implements Runnable*/ {
    private String relayerId;
    private List<SubProcessor> subProcessors;
    //    Map<String, String> subProcessorCheckpointMap = new HashMap<String, String>();
    String processorId;
    private Logger logger;// = LoggerFactory.getLogger(SubProcessorMaxSeqPersister.class);

    //Hashmap for storing the values of the average and the max lag time
    private HashMap< String, Long> avgAndMaxLagTimeMap = new HashMap< String,Long>();
    private HashMap< String, RelayerMetric> subProcessorQueueMetric = new HashMap<>();

    public QpsCollector(String processorId, String relayerId, List<SubProcessor> subProcessors) {
        logger = LoggerFactory
            .getLogger(SubProcessorMaxSeqPersister.class.getCanonicalName() + " " + processorId);
        this.processorId = processorId;
        this.relayerId = relayerId;
        this.subProcessors = subProcessors;
        createQueueCapacityMetricForEachSubProcessor();
    }

    private void createQueueCapacityMetricForEachSubProcessor() {
        for(SubProcessor subProcessor : subProcessors) {
            subProcessorQueueMetric.put(subProcessor.getId(),
                    new RelayerMetric(SubProcessor.class,"relayer.exchange." + relayerId + ".subprocessor." + subProcessor.getId(),
                            "subprocessorQueue.remainingCapacity", true));
        }
    }

    public void publishRemainingQueueSize() {
        for (SubProcessor subProcessor : subProcessors) {
            subProcessorQueueMetric.get(subProcessor.getId())
                    .updateMetric(subProcessor.getQueueRemainingCapacity());
        }
    }


    public Long collectMessageCount() {
        Long totalNumberOfMessages = 0L;
        for (SubProcessor subProcessor : subProcessors) {
            totalNumberOfMessages  += subProcessor.getNumberOfMessageProcessed();
        }
        logger.debug("Total messages processed : " + totalNumberOfMessages);
        return totalNumberOfMessages;
    }

    public int isDbDead(){
        Boolean atLeastOneFailure = false;
        for (SubProcessor subProcessor : subProcessors) {
            atLeastOneFailure  =  subProcessor.isDbDead() || atLeastOneFailure;
        }
        return atLeastOneFailure ? 1 : 0 ;
    }

    //Method to calculate the max lag time of the subprocessors also calculating the average of the lag time passed.
    public HashMap< String , Long> getMessageMaxAndAvgLagTime(){

        // Initializing Variables for the use.
        long totalSubprocessors = subProcessors.size();
        long totalLagTime = 0;
        long maxLagTime = 0;
        Long lagTime = null;

        //Calculating the Maximum and the average value of the lag time.
        for (SubProcessor subProcessor :subProcessors){
            lagTime = subProcessor.getMessageLagTime();
            if(lagTime!=null) {
                if (maxLagTime < lagTime) {
                    maxLagTime = lagTime;
                }
                totalLagTime += lagTime;
            }
        }

        //Checking the case when there is no task processor assigned to the relayer at that time method returns null to the metricsCollector.
        if (totalSubprocessors > 0 && maxLagTime != 0) {
            long avgLagTime = totalLagTime / totalSubprocessors;
            //storing the values in the Map
            avgAndMaxLagTimeMap.put("Average", avgLagTime);
            avgAndMaxLagTimeMap.put("Maximum", maxLagTime);
            return avgAndMaxLagTimeMap;
        }
        return null;
    }

}
