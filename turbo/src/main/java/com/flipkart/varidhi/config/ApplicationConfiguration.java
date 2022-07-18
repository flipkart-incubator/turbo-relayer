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

package com.flipkart.varidhi.config;

import com.flipkart.turbo.config.AlertProviderConfig;
import com.flipkart.turbo.config.AuthConfig;
import com.flipkart.turbo.config.HttpAuthConfig;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import io.dropwizard.validation.ValidationMethod;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
 * *
 * Author: abhinavp
 * Date: 30-Jun-2015
 *
 */


@Getter
@Setter
@ToString
public class ApplicationConfiguration {
    String hostName;
    String appUserName;
    @NotNull
    String teamName;
    String appLogLevel = "INFO";
    String relayingLogLevel = "INFO";
    long metricCollectorSleepTimeInMs = 2000;
    @NotNull
    Map<String, Properties> mysql;
    @Valid
    @NotNull
    List<RelayerConfiguration> relayers;
    MockConfig mockConfig;
    @NotNull
    String defaultDestinationServer;
    PartitionConfiguration partitionConfiguration;
    EmailConfiguration emailConfiguration;
    @NotNull
    private String appName;
    private HashingAlgorithm hashingAlgorithm;
    private String alertzEndpoint = "";
    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfiguration.class);
    private Boolean defaultCreateAlert = false;
    private @Valid AuthConfig authConfig;
    private @Valid HttpAuthConfig httpAuthConfig;
    private @Valid AlertProviderConfig alertProviderConfig;


    @ValidationMethod(message="Partition Management Configuration Invalid for Relayer")
    public boolean isPartitionConfigurationPresent() {
        if (relayers == null) {
            logger.error("Relayers Shouldn't be null");
            return false;
        }
        if (this.partitionConfiguration == null) {
            for (RelayerConfiguration relayer : relayers) {
                if (relayer == null || relayer.getPartitionConfiguration() == null) {
                    logger.error("Partition Configuration Not Present for relayer: " + relayer.getRelayerId());
                    return false;
                }
            }
        }
        return true;
    }

    @ValidationMethod(message="Mysql Configuration Invalid for Relayer")
    public boolean isRepositoryConfigurationValid() {
        if (relayers == null) {
            logger.error("Relayers Shouldn't be null");
            return false;
        }
        if (mysql == null) {
            logger.error("mysql Configuration Shouldn't be null");
            return false;
        }
        for (RelayerConfiguration relayer : relayers) {
            if (relayer == null || relayer.getOutboundDbRef() == null ||
                    !mysql.containsKey(relayer.getOutboundDbRef().id)) {
                logger.error("Mysql Config for relayer: " + relayer.getRelayerId() + " not present.");
                return false;
            }
            if(relayer.getTurboReadMode() != TurboReadMode.OUTBOUND_READER && (
                    relayer.getAppDbRef() == null ||  !mysql.containsKey(relayer.getAppDbRef().id))){
                logger.error("Mysql Config for relayer: " + relayer.getRelayerId() + " not present.");
                return false;
            }
        }
        return true;
    }

    @ValidationMethod(message="Hashing Algorithm should be present in Relayer or globally")
    public boolean isHashingAlgorithmPresent() {
        if (relayers == null) {
            logger.error("Relayers Shouldn't be null");
            return false;
        }
        if (this.hashingAlgorithm == null) {
            for (RelayerConfiguration relayer : relayers) {
                if (relayer == null || relayer.getHashingAlgorithm() == null) {
                    logger.error("Hashing Algorithm Not Present for relayer: " + relayer.getRelayerId());
                    return false;
                }
            }
        }
        return true;
    }

    @ValidationMethod(message="Leader Election Config is Not Valid")
    public boolean isLeaderElectionConfigValid() {
        for (RelayerConfiguration relayer : relayers) {
            if (relayer == null) {
                logger.error("Relayer shouldn't be null");
                return false;
            }
            if (relayer.getLeaderElectionPingInterval() < 5){
                logger.error("LeaderElectionPingInterval can't be less then 5 secs " + relayer.getRelayerId());
                return false;
            }
            if (!(2*relayer.getLeaderElectionPingInterval() < relayer.getLeaderElectionExpiryInterval()) ) {
                logger.error("LeaderElectionExpiryInterval must be more then 2*LeaderElectionPingInterval " + relayer.getRelayerId());
                return false;
            }
        }
        return true;
    }

    public PartitionConfiguration getPartitionConfigForRelayerConfig(RelayerConfiguration relayerConfiguration) {
        if (relayerConfiguration == null) {
            logger.error("Relayer Configuration cannot be null for partition configuration");
            throw new IllegalArgumentException("Relayer Configuration cannot be null");
        }
        if (relayerConfiguration.getPartitionConfiguration() == null && partitionConfiguration == null) {
            logger.error("Partition Configuration cannot be null for relayer: " + relayerConfiguration.getRelayerId());
            System.exit(1);
        }

        if (relayerConfiguration.getPartitionConfiguration() == null) {
            relayerConfiguration.setPartitionConfiguration(new PartitionConfiguration());
        }
        try {
            relayerConfiguration.getPartitionConfiguration().setDefaultPartitionConfigurations(this.partitionConfiguration,relayerConfiguration);
        } catch (RuntimeException e) {
            logger.error("Error while mapping partition configuration for relayer: " + relayerConfiguration.getRelayerId(),e);
            System.exit(1);
        }
        return relayerConfiguration.getPartitionConfiguration();

    }

}
