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

import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.turbo.config.*;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import io.dropwizard.validation.ValidationMethod;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.List;

import static com.flipkart.varidhi.utils.Constants.DEFAULT_ENCODING;

/*
 * *
 * Author: abhinavp
 * Date: 31-Jul-2015
 *
 */


@Getter
@Setter
@ToString
public class RelayerConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(RelayerConfiguration.class);

    String name;
    String characterEncoding = DEFAULT_ENCODING;
    String relayerId;
    @NotNull
    Boolean active;
    int readerMainQueueSize = 20000;     // Queue size in which reader puts the messages.
    int readerBatchSize = 10000;         // How many messages to fetch in a single iteration.
    int readerParallelismDegree = 10;    // While reading how many threads to spawn.
    int processorParallelismDegree = 53; // Number of subProcessors / Consumers
    int processorQueueSize = 1000;       // Number of messages per subProcessor/Consumer
    int lastProcessedPersistInterval = 1000;
    int delayedReadIntervalInSeconds = 0;
    String controlTaskDeletionJobTime = "02:00:00";
    int controlTaskDeletionFrequencyInHrs = 24;
    int controlTaskDeletionBatchSize = 1000;
    int controlTaskKeepDataInDays = 10;
    boolean leaderElectionEnabled;
    int leaderElectionPingInterval = 10 ;
    int leaderElectionExpiryInterval = 30;
    int leaderElectionQueryTimeout = 1;
    long readerSleepTime = 100;
    long maxApplicationTransactionTime =1800000;
    TurboReadMode turboReadMode;
    String destinationServer;
    @Valid
    RepositoryConfiguration appDbRef;
    @NotNull
    @Valid
    RepositoryConfiguration outboundDbRef;
    RepositoryConfiguration metricDbRef;
    @Valid
    private PartitionConfiguration partitionConfiguration;
    private HashingAlgorithm hashingAlgorithm;
    String appUserName;
    boolean preventReRelayOnStartUp;
    private List<AlertzConfig> alertzConfig;
    private Boolean createAlert;
    private boolean enableCustomRelay;
    private boolean ignoreSideliningOf4xxFailures;
    private boolean ignoreSideliningOfNon4xxFailures;
    private String varadhiAuthTargetClientID;
    private String cronScheduleUnsideline = "10 * * * *";

    public boolean isNameProvided() {
        return name != null && name.trim().length() > 0;
    }

    public String getRelayerId() {
        if(StringUtil.isNotBlank(relayerId)) {
            return relayerId;
        }
        return this.isNameProvided() ? this.getName() : Relayer.DEFAULT_RELAYER_ID;
    }

    @ValidationMethod(message="Character Encoding provided isn't Valid")
    public boolean isCharacterEncodingProvidedValid() {
        try {
            return Charset.isSupported(characterEncoding);
        } catch (IllegalCharsetNameException e){
            logger.error("Character Encoding Invalid : "+ characterEncoding, e);
        }

        return false;
    }

    public boolean isPartitionConfigurationProvided() {
        return  null != partitionConfiguration;
    }

    public void setTurboReadMode(String turboReadMode) {
        this.turboReadMode = TurboReadMode.getTurboReadMode(turboReadMode);
    }
}
