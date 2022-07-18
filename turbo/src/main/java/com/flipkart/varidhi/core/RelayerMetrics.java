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

package com.flipkart.varidhi.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

/*
 * *
 * Author: abhinavp
 * Date: 03-Sep-2015
 *
 */
public class RelayerMetrics {
    private final Logger logger = LoggerFactory.getLogger(RelayerMetrics.class.getCanonicalName());
    private final RelayerMetric<Integer> readerQueueRemainingCapacity;
    private RelayerMetric<Integer> isActive;
    private RelayerMetric<Integer> sidelinedMessagesCount;
    private RelayerMetric<Integer> sidelinedGroupsCount;
    private RelayerMetric<Long> processedMessagesCount;
    private RelayerMetric<Integer> pendingMessagesCount;
    private RelayerMetric<Long> maxPartitionIdVal;
    private RelayerMetric<Long> maxMessageID;
    private RelayerMetric<Long> minProcessedMessageLagTime;
    private RelayerMetric<Long> maxProcessedMessageLagTime;
    private RelayerMetric<Integer> sidelinedDbFailure;
    private RelayerMetric<Long> maxMessageLagTime;
    private RelayerMetric<Long> avgMessageLagTime;
    private final RelayerMetric<Long> leaderElectionMetric;
    private final RelayerMetric<Long> lastReadEpochTime;
    private final RelayerMetric<Long> lastDistributionEpochTime;
    private final RelayerMetric<Long> lastPersistedEpochTime;

    public RelayerMetrics(String relayerId) {
        isActive = new RelayerMetric<>(relayerId, "active", true);
        sidelinedMessagesCount = new RelayerMetric<>(relayerId, "messages.sidelined", true);
        sidelinedGroupsCount = new RelayerMetric<>(relayerId, "groups.sidelined", true);
        processedMessagesCount = new RelayerMetric<>(relayerId, "messages.processed", true);
        pendingMessagesCount = new RelayerMetric<>(relayerId, "messages.pending", true);
        maxPartitionIdVal = new RelayerMetric<>(relayerId, "messages.maxpartitionid", true);
        maxMessageID = new RelayerMetric<>(relayerId, "messages.maxmessageid", true);
        minProcessedMessageLagTime = new RelayerMetric<>(relayerId, "messages.minProcessedMessageLagTime", true);
        maxProcessedMessageLagTime = new RelayerMetric<>(relayerId, "messages.maxProcessedMessageLagTime", true);
        sidelinedDbFailure = new RelayerMetric<>(relayerId, "sidelinedDbFailure", true);
        maxMessageLagTime = new RelayerMetric<>(relayerId, "messages.maxLagTime", true);
        avgMessageLagTime = new RelayerMetric<>(relayerId, "messages.avgLagTime", true);
        readerQueueRemainingCapacity = new RelayerMetric<>(relayerId, "readerQueue.remainingCapacity", true);
        leaderElectionMetric = new RelayerMetric<>(relayerId, "leader.elected", true);
        lastReadEpochTime = new RelayerMetric<>(relayerId, "epoch.lastRead", true);
        lastDistributionEpochTime = new RelayerMetric<>(relayerId, "epoch.lastDistribution", true);
        lastPersistedEpochTime = new RelayerMetric<>(relayerId, "epoch.lastPersisted", true);
    }

    public void resetAllMetrics(){
        try {
            for(Field field : this.getClass().getDeclaredFields()){
                if(RelayerMetric.class == field.getType()){
                    field.setAccessible(true);
                    Object fieldValue = field.get(this);
                    Method method = fieldValue.getClass().getDeclaredMethod("updateMetric",Object.class);
                    method.invoke(fieldValue,new Object[]{ null });
                }
            }
        } catch (Exception e){
            logger.error("Error occurred while resetAllMetrics ",e);
        }
    }

    public void updatecurrentSidelinedMessagesCount(Integer count) {
        sidelinedMessagesCount.updateMetric(count);
    }

    public void updatecurrentSidelinedGroupsCount(Integer count) {
        sidelinedGroupsCount.updateMetric(count);
    }

    public void updateNumberOfMessagesProcessed(Long count) {
        processedMessagesCount.updateMetric(count);
    }

    public void updatePendingMessageCount(Integer count) {
        pendingMessagesCount.updateMetric(count);
    }

    public void updateIsActive(Boolean isActive) {
        this.isActive.updateMetric(isActive ? 1 : null);
    }

    public void updateLeaderElectionMetric(Long val){
        leaderElectionMetric.updateMetric(val);
    }

    public void updateLastReadEpochTime(Long val){
        lastReadEpochTime.updateMetric(val);
    }

    public void updateLastDistributionEpochTime(Long val){
        lastDistributionEpochTime.updateMetric(val);
    }

    public void updateLastPersistedEpochTime(Long val){
        lastPersistedEpochTime.updateMetric(val);
    }

    public void updateMaxPartitionIdVal(Long val) {
        maxPartitionIdVal.updateMetric(val);
    }

    public void updateMaxMessageId(Long val) {
        maxMessageID.updateMetric(val);
    }

    public void updateLagTimeForMinMaxProcessedMessage(Long lagTimeForMinMessage, Long lagTimeForMaxMessage) {
        minProcessedMessageLagTime.updateMetric(lagTimeForMinMessage);
        maxProcessedMessageLagTime.updateMetric(lagTimeForMaxMessage);

    }

    public void updateDbHealthStatus(Integer val) {
        sidelinedDbFailure.updateMetric(val);
    }

    public void updateMessageLagTime(HashMap<String, Long> lagTimeMap) {
        if (lagTimeMap == null) {
            //So that we set null in metric when lagTimeMap is null
            lagTimeMap = new HashMap<>();
        }
        maxMessageLagTime.updateMetric(lagTimeMap.get("Maximum"));
        avgMessageLagTime.updateMetric(lagTimeMap.get("Average"));
    }

    public void updateReaderQueueRemainingCapacity(Integer val) {
        readerQueueRemainingCapacity.updateMetric(val);
    }
}
