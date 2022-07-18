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

package com.flipkart.varidhi.utils;

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.RelayerOutboundRepository;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;


public class LeaderElection implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    private Relayer relayer;
    private RelayerOutboundRepository relayerOutboundRepository;
    private RelayerMetrics relayerMetrics;
    private final ExecutorService relayerStarter;
    private final LinkedBlockingQueue<Future> pendingFutures;

    public LeaderElection(@NonNull Relayer relayer,@NonNull RelayerOutboundRepository relayerOutboundRepository, @NonNull RelayerMetrics relayerMetrics){
        this.relayer = relayer;
        this.relayerOutboundRepository = relayerOutboundRepository;
        this.relayerMetrics = relayerMetrics;
        this.relayerStarter = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("RelayerStarter")
                .setUncaughtExceptionHandler((t, e) -> logger.error(" Error occurred while starting relayer " + t + ", error: ",e))
                .build()
        );
        this.pendingFutures = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        try {
            this.invokeLeaderElection();
        } catch (Exception e) {
            logger.error("Exception occurred while performing a leader election: " + e.getMessage(), e);
        }
    }

    private void invokeLeaderElection() {
        if (!relayer.isActive()) {
            return;
        }

        RelayerConfiguration configuration = relayer.getRelayerConfiguration();
        logger.info("leader election running.. for relayer :"+configuration.getRelayerId()+" uid "+relayer.getRelayerUUID());


        DateTime leaseExpiryTime = new DateTime().plusSeconds(configuration.getLeaderElectionExpiryInterval());
        boolean leaderElectionResult = false;
        try {
            leaderElectionResult = relayerOutboundRepository.checkOrPerformLeaderElection(
                    relayer.getRelayerUUID(),configuration.getLeaderElectionQueryTimeout(),
                    configuration.getLeaderElectionExpiryInterval());
        } catch (Exception e) {
            logger.error("Error occurred while checkOrPerformLeaderElection " + e.getMessage(), e);
        }
        if(leaderElectionResult){
            logger.info(String.format("updating lease for %s, with duration %s", configuration.getRelayerId(),leaseExpiryTime));
            LeadershipExpiryHolder.updateLeadershipExpiry(relayer.getRelayerUUID(),leaseExpiryTime);
        }


        try {
            if (leaderElectionResult && !relayer.isRunning()){
                logger.warn(String.format("LeaderElection :: New leader elected for %s, uuid : %s",
                        configuration.getRelayerId(),relayer.getRelayerUUID()));
                Future future = relayerStarter.submit(() -> {
                    relayer.init();
                    relayerMetrics.updateLeaderElectionMetric(CommonUtils.getDotRemovedIp());
                });
                pendingFutures.add(future);
            }
            if (!leaderElectionResult && relayer.isRunning()){
                logger.error(String.format("LeaderElection :: Relayer lease expired halting relayer %s, uuid : %s",
                        configuration.getRelayerId(),relayer.getRelayerUUID()));
                while (!pendingFutures.isEmpty()){
                    logger.warn("Halt is waiting on relayer init");
                    pendingFutures.take().get();
                }
                relayer.halt();
            }
        } catch (Exception e) {
            logger.error("Error occurred while starting or stopping the relayer " + e.getMessage(), e);
        }


    }

}
