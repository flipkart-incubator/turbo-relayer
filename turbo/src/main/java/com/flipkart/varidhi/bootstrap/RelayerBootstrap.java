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

package com.flipkart.varidhi.bootstrap;

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.core.RelayerMetricHandleContainer;
import com.flipkart.varidhi.jobs.MetricsCollector;
import com.flipkart.varidhi.relayer.Relayer;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;

/*
 * *
 * Author: abhinavp
 * Date: 08-Jul-2015
 *
 */
public class RelayerBootstrap implements Managed {

    RelayerHandleContainer relayerHandleContainer;
    RelayerMetricHandleContainer relayerMetricHandleContainer;
    MetricsCollector metricsCollector;

    @Inject public RelayerBootstrap(RelayerHandleContainer relayerHandleContainer,
                                    RelayerMetricHandleContainer relayerMetricHandleContainer, ApplicationConfiguration appConfig) {
        this.relayerHandleContainer = relayerHandleContainer;
        this.relayerMetricHandleContainer = relayerMetricHandleContainer;
        metricsCollector =
            new MetricsCollector(relayerHandleContainer, relayerMetricHandleContainer, appConfig.getMetricCollectorSleepTimeInMs());
    }

    @Override public void start() {
        for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
            if(relayer.getRelayerConfiguration().getActive()){
                relayer.markRelayerActive();
                if(!relayer.getRelayerConfiguration().isLeaderElectionEnabled()){
                    relayer.init();
                }
            }
        }
        metricsCollector.startMetricsCollection();
    }

    @Override public void stop() {
        for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
            relayer.markRelayerInactive();
            relayer.stop();
        }
        metricsCollector.stopMetricsCollection();
    }
}
