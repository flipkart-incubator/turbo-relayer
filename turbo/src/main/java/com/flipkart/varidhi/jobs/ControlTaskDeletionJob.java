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

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.RelayerMetric;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.OutboundRepository;
import com.flipkart.varidhi.repository.OutboundRepositoryImpl;
import lombok.NonNull;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


public class ControlTaskDeletionJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ControlTaskDeletionJob.class);
    private final RelayerMetric<Long> controlTaskDeletionFailure;
    private Relayer relayer;
    private SessionFactoryContainer sessionFactoryContainer;

    public ControlTaskDeletionJob(@NonNull Relayer relayer, SessionFactoryContainer sessionFactoryContainer) {
        this.relayer = relayer;
        this.sessionFactoryContainer = sessionFactoryContainer;
        this.controlTaskDeletionFailure = new RelayerMetric<>(relayer.getRelayerId(), "controlTaskDeletion.fail",
                true);
    }

    @Override
    public void run() {
        try {
            invokeControlTaskDeletionJob(this.relayer);
        } catch (Exception e) {
            logger.error("This run of ControlTaskDeletionJob failed with error: " + e.getMessage(), e);
        }
    }


    private void invokeControlTaskDeletionJob(@NonNull Relayer relayer) {
        if (!relayer.isActive() || !relayer.isRunning()) {
            logger.warn("ControlTaskDeletionJob cannot be initiated for non active relayer.");
            return;
        }
        RelayerConfiguration relayerConfiguration = relayer.getRelayerConfiguration();
        int noOfDaysToPreserve = relayerConfiguration.getControlTaskKeepDataInDays();
        int controlTaskDeletionBatchSize = relayerConfiguration.getControlTaskDeletionBatchSize();

        try {
            ExchangeTableNameProvider exchangeTableNameProvider =
                    new ExchangeTableNameProvider(relayerConfiguration.getName());

            OutboundRepository outboundRepository = new OutboundRepositoryImpl(
                    sessionFactoryContainer.getSessionFactory(relayerConfiguration.getOutboundDbRef().getId()), exchangeTableNameProvider);
            logger.info("Starting ControlTaskDeletionJob for Relayer: " + relayerConfiguration.getRelayerId());
            while(outboundRepository.deleteControlTaskEntries(
                    DateUtils.addDays(new Date(),noOfDaysToPreserve*(-1)),
                    controlTaskDeletionBatchSize) > 0){
                Thread.sleep(relayerConfiguration.getReaderSleepTime());
            }
            logger.info("Ending ControlTaskDeletionJob for Relayer: " + relayerConfiguration.getRelayerId());
            controlTaskDeletionFailure.updateMetric(0L);
        } catch (Exception ex) {
            logger.error("ControlTaskDeletionJob failed for Relayer: " + relayerConfiguration.getRelayerId(), ex.getMessage(), ex);
            controlTaskDeletionFailure.updateMetric(1L);
        }

    }

}
