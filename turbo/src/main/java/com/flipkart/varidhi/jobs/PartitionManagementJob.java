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

import com.flipkart.varidhi.RelayerModule;
import com.flipkart.varidhi.config.PartitionConfiguration;
import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.RelayerMetric;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.partitionManager.PartitionArchiver;
import com.flipkart.varidhi.partitionManager.PartitionCreator;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.repository.*;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by manmeet.singh on 03/03/16.
 */
public class PartitionManagementJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManagementJob.class);
    private final RelayerMetric<Long> partitionManagementFailure;
    private SessionFactoryContainer sessionFactoryContainer;
    private final TRPartitionQueriesLogger queriesLogger;
    private final PartitionAlterMonitor partitionAlterMonitor;
    private Relayer relayer;
    public static final int HOURS_IN_A_DAY = 24;

    public PartitionManagementJob(@NonNull Relayer relayer,
                                  SessionFactoryContainer sessionFactoryContainer,
                                  TRPartitionQueriesLogger queriesLogger, PartitionAlterMonitor partitionAlterMonitor) {
        this.relayer = relayer;
        this.sessionFactoryContainer = sessionFactoryContainer;
        this.queriesLogger = queriesLogger;
        this.partitionAlterMonitor = partitionAlterMonitor;
        this.partitionManagementFailure = new RelayerMetric<>(relayer.getRelayerId(), "partition.fail",
                true);
    }

    @Override
    public void run() {
        try {
            invokePartitionManager(this.relayer, false);
        } catch (Exception e) {
            logger.error("This run of partition management failed with error: " + e.getMessage(), e);
        }
    }

    public void invokePartitionManager(@NonNull Relayer relayer, boolean shouldOnlyLogQueries) {
        if (!relayer.isActive() || !relayer.isRunning()) {
            logger.warn("Partition Configuration cannot be initiated for inactive/non running relayer.");
            return;
        }
        RelayerConfiguration relayerConfiguration = relayer.getRelayerConfiguration();
        PartitionConfiguration partitionConfiguration = relayerConfiguration.getPartitionConfiguration();
        if (partitionConfiguration == null || PartitionMode.INACTIVE == partitionConfiguration.getMode()) {
            logger.error("Partition Configuration Cannot be NULL/INACTIVE to do partition management in relayer: " + relayerConfiguration.getRelayerId());
            queriesLogger.collectException("Partition Configuration Cannot be NULL/INACTIVE to do partition management in relayer: " + relayerConfiguration.getRelayerId());
            return;
        }
        RelayerModule relayerModule = new RelayerModule();
        long partitionSize = partitionConfiguration.getSize();
        if (partitionSize <= 0) {
            throw new IllegalArgumentException("Illegal Argument : Partition Size is 0");
        }
        if (partitionConfiguration.isInObserverMode() || shouldOnlyLogQueries)
            queriesLogger.init();
        try {
            ExchangeTableNameProvider exchangeTableNameProvider =
                    new ExchangeTableNameProvider(relayerConfiguration.getName());
            ApplicationPartitionRepository applicationPartitionRepository = null;
            if(relayerConfiguration.getTurboReadMode() != TurboReadMode.OUTBOUND_READER) {
                applicationPartitionRepository = relayerModule
                        .prepareApplicationPartitionRepository(relayerConfiguration,
                                sessionFactoryContainer, exchangeTableNameProvider, partitionConfiguration, partitionAlterMonitor);
            }

            OutboundPartitionRepository outboundPartitionRepository = relayerModule
                    .prepareOutboundPartitionRepository(relayerConfiguration,
                            sessionFactoryContainer, exchangeTableNameProvider, partitionConfiguration, partitionAlterMonitor);

            if (relayerConfiguration.getTurboReadMode() != TurboReadMode.OUTBOUND_READER){
                applicationPartitionRepository.setTrPartitionQueriesLogger(queriesLogger,shouldOnlyLogQueries);
            }

            outboundPartitionRepository.setTrPartitionQueriesLogger(queriesLogger,shouldOnlyLogQueries);
            boolean partitionManagementFailed = false;
            try {
                logger.info("Starting Archival Job for Relayer: " + relayerConfiguration.getRelayerId());
                new PartitionArchiver(getNoOfHoursToPreserve(partitionConfiguration), partitionSize,
                        applicationPartitionRepository, outboundPartitionRepository,
                        partitionConfiguration.getMode(),relayerConfiguration.getTurboReadMode()).archiveAndDropShard();
            } catch (Exception ex) {
                logger.error("Partition Archival Job failed for Relayer: " + relayerConfiguration.getRelayerId(), ex.getMessage(), ex);
                queriesLogger.collectException(ex.getMessage());
                partitionManagementFailed = true;
            }


            try {
                logger.info("Starting Partition Creation Job for Relayer: " + relayerConfiguration.getRelayerId());
                new PartitionCreator(partitionConfiguration.getNoOfExtraPartitions(), partitionSize,
                        applicationPartitionRepository, outboundPartitionRepository,relayerConfiguration.getTurboReadMode(), partitionConfiguration.getMode()).createPartitions();
            } catch (Exception ex) {
                logger.error("Partition Creation Job failed for Relayer: " + relayerConfiguration.getRelayerId(), ex.getMessage(), ex);
                queriesLogger.collectException(ex.getMessage());
                partitionManagementFailed = true;
            }
            partitionManagementFailure.updateMetric(partitionManagementFailed ? 1L : 0L);

        } catch (Exception ex) {
            logger.error("Partition Job failed for Relayer: " + relayerConfiguration.getRelayerId(), ex.getMessage(), ex);
            queriesLogger.collectException(ex.getMessage());
            partitionManagementFailure.updateMetric(1L);
        } finally {
            logger.info("Ending Partition Job for Relayer: " + relayerConfiguration.getRelayerId());
        }
        if (partitionConfiguration.isInObserverMode() || shouldOnlyLogQueries) {
            queriesLogger.finish();
        }
    }

    /**
     * Return Configuration in Hours only.
     * @param partitionConfiguration @{@link PartitionConfiguration}
     * @return int
     */
    private int getNoOfHoursToPreserve(PartitionConfiguration partitionConfiguration) {

        if(partitionConfiguration.getNoOfHoursToPreserve()!=null) {
            return partitionConfiguration.getNoOfHoursToPreserve();
        }

        return partitionConfiguration.getNoOfDaysToPreserve() * HOURS_IN_A_DAY;
    }
}
