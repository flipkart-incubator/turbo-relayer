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

package com.flipkart.varidhi;

import com.flipkart.turbo.config.AuthConfig;
import com.flipkart.turbo.config.HttpAuthConfig;
import com.flipkart.turbo.utils.ZoneType;
import com.flipkart.varidhi.config.*;
import com.flipkart.varidhi.config.RepositoryConfiguration.DbType;
import com.flipkart.varidhi.core.*;
import com.flipkart.varidhi.jobs.PartitionManagementJob;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.RelayerMetricRepository;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaDiff;
import com.flipkart.varidhi.relayer.schemavalidator.validator.DBSchemaValidator;
import com.flipkart.varidhi.repository.*;
import com.flipkart.varidhi.utils.CommonUtils;
import com.flipkart.varidhi.utils.Constants;
import com.flipkart.varidhi.utils.LeaderElection;
import com.flipkart.varidhi.utils.LoggerUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.StringUtil;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.validation.Validation;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varidhi.utils.Constants.HOST;
import static com.flipkart.varidhi.utils.Constants.VARADHI_AUTH_TARGET_CLIENT_ID;

//import com.flipkart.varidhi.jobs.AlertServiceHelper;

/*
 * *
 * Author: abhinavp
 * Date: 30-Jun-2015
 *
 */
public class RelayerModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(RelayerModule.class);

    @Override
    protected void configure() {
        //bind(RelayerHandleContainer.class).to(MapRelayerContainer.class).asEagerSingleton();
    }

    private ApplicationConfiguration readConfigFromYAML(String fileName) throws Exception {
        ConfigurationFactory<ApplicationConfiguration> factory =
                new YamlConfigurationFactory<>(ApplicationConfiguration.class,
                        Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
                        "");
        try {
            File file = new File(fileName);
            logger.warn("reading config from: "+ file.getAbsolutePath());
            return factory.build(file);
        } catch (Exception e) {
            logger.error("Error occurred while prepareApplicationConfiguration", e);
            throw e;
        }
    }

    private String readFileContents(String fileName) throws IOException {
        try {
            Path path = Paths.get(fileName);
            logger.warn("Reading Contents of File: "+path.toAbsolutePath().toString());
            if(!Files.exists(path)) {
                return "";
            } else {
                return new String(Files.readAllBytes(path));
            }

        } catch (IOException e) {
            logger.error("Error while reading file : ", e);
            throw e;
        }
    }

    @Provides
    @Singleton
    public ApplicationConfiguration prepareApplicationConfiguration(){
        try {
            String configFile = StringUtil.isNotBlank(System.getProperty("relayerConfigFile")) ?
                    System.getProperty("relayerConfigFile") : Constants.DEFAULT_CONFIG_FILE;

            final ApplicationConfiguration configuration = readConfigFromYAML(configFile);
            LoggerUtil.setLogLevel(LoggerUtil.APPLICATION_LOGGER,configuration.getAppLogLevel());
            LoggerUtil.setLogLevel(LoggerUtil.RELAYING_MESSAGE_LOGGER,configuration.getRelayingLogLevel());


            AuthConfig authConfig = configuration.getAuthConfig();
            if(authConfig == null || !authConfig.isValid()){
                logger.warn("com.flipkart.turbo.config.AuthConfig is null/invalid. Won't be using AuthN for calls !!");
            } else {
                AuthenticationService authenticationService = new AuthenticationService(authConfig);
                authenticationService.authenticate(authConfig);
                logger.info("AuthTokenService Initialized for clientID: " + authConfig.getClientId());
            }

            String secretsFolderPath = StringUtil.isNotBlank(System.getProperty("secretsFolderPath")) ?
                    System.getProperty("secretsFolderPath") : Constants.DEFAULT_SECRETS_CONFIG_FILE;
            for(Map.Entry<String,Properties> mysqlConfig: configuration.getMysql().entrySet()){
                Properties config = mysqlConfig.getValue();
                String passwordFile = String.format(
                        Constants.DEFAULT_MYSQL_PASSWORD_PATTERN,
                        secretsFolderPath,
                        mysqlConfig.getKey()
                );
                String fileContents = readFileContents(passwordFile);
                if(!StringUtils.isEmpty(fileContents)) {
                    config.setProperty(Constants.HIBERNATE_CONNECTION_PASSWORD, fileContents);
                }
            }

            return configuration;
        } catch (Exception e){
            logger.error("System.exit::error occurred in prepareApplicationConfiguration",e);
            System.exit(-1);
            return null;
        }
    }

    @Provides
    @Singleton
    public HttpAuthenticationService providesHttpAuthenticationService(ApplicationConfiguration configuration) {
        HttpAuthConfig httpAuthConfig = configuration.getHttpAuthConfig();
        return new HttpAuthenticationService(httpAuthConfig);
    }

    @Provides
    @Singleton
    public SessionFactoryContainer provideSessionFactoryContainer(
            ApplicationConfiguration applicationConfiguration) {
        SessionFactoryContainer sessionFactoryContainer = new MapSessionFactoryContainer();
        try {
            for (Entry<String, Properties> propertyMap : applicationConfiguration.getMysql()
                    .entrySet()) {
                ServiceRegistry serviceRegistry =
                        new ServiceRegistryBuilder().applySettings(propertyMap.getValue())
                                .buildServiceRegistry();
                SessionFactory sessionFactory =
                        new Configuration().addProperties(propertyMap.getValue())
                                .buildSessionFactory(serviceRegistry);
                sessionFactoryContainer.addSessionFactory(propertyMap.getKey(), sessionFactory);
            }
        } catch (Exception e) {
            logger.error("System.exit::Error while initializing Session Factory", e);
            System.exit(-1);
        }
        return sessionFactoryContainer;
    }

    @Provides
    @Singleton
    public AlertCreationService providesAlertCreationService(ApplicationConfiguration configuration, RelayerHandleContainer relayerHandleContainer) {
        String appID = System.getProperty("relayer.fcpAppId");
        String zone = System.getProperty("relayer.fcpZone");
        if(StringUtils.isBlank(appID) || StringUtils.isBlank(zone)){
            logger.error(String.format("Failed to get appID: [%s] and zone: [%s] for alert creation",appID,zone));
        } else {
            logger.info(String.format("Initializing alert for appID: [%s] and zone: [%s]",appID,zone));
        }
        AlertCreationService alertCreationService =  new AlertCreationService(configuration.getAlertProviderConfig(),
                configuration.getAlertzEndpoint(), configuration.getAppName(), appID, ZoneType.getZoneType(zone));
        if(configuration.getAlertProviderConfig() != null) {
            alertCreationService.createAlertsForAllRelayers(relayerHandleContainer, configuration.getAlertProviderConfig().getAlertMethodName());
        }
        return alertCreationService;
    }

    @Provides
    @Singleton
    public RelayerHandleContainer provideRelayerHandleContainer(
            SessionFactoryContainer sessionFactoryContainer, ApplicationConfiguration applicationConfiguration,
            HttpAuthenticationService httpAuthenticationService) {

        RelayerHandleContainer relayerHandleContainer = new MapRelayerContainer();

        try {
            for (RelayerConfiguration relayerConfiguration : applicationConfiguration.getRelayers()) {

                // initialize repositories
                ExchangeTableNameProvider exchangeTableNameProvider =
                        new ExchangeTableNameProvider(relayerConfiguration.getName());
                ApplicationRepository applicationRepository = null;
                if (relayerConfiguration.getTurboReadMode() != TurboReadMode.OUTBOUND_READER) {
                    applicationRepository = getApplicationRepository(sessionFactoryContainer, applicationConfiguration,
                            relayerConfiguration, exchangeTableNameProvider);
                }

                OutboundRepository outboundRepository = getOutboundRepository(sessionFactoryContainer, applicationConfiguration,
                        relayerConfiguration, exchangeTableNameProvider);

                RelayerMetricRepository metricRepository = getMetricRepository(sessionFactoryContainer, outboundRepository,
                        relayerConfiguration, exchangeTableNameProvider);
                metricRepository = metricRepository != null ? metricRepository : outboundRepository;



                // validate db schema
                List<DBSchemaDiff> dbSchemaDiffList = new ArrayList<>();
                DBSchemaValidator dbSchemaValidator = new DBSchemaValidator(exchangeTableNameProvider,applicationRepository,outboundRepository);
                if (relayerConfiguration.getTurboReadMode() != TurboReadMode.OUTBOUND_READER) {
                    dbSchemaDiffList.addAll(dbSchemaValidator.validateAppSchema());
                }
                dbSchemaDiffList.addAll(dbSchemaValidator.validateOutboundSchema());

                /*if(!Boolean.valueOf(System.getProperty("relayer.forceStart")) && dbSchemaDiffList.size()>0){
                    throw new DBSchemaValidationException("Invalid Schema, use --forceStart=true to force start turbo");
                }*/


                String relayerId = relayerConfiguration.getRelayerId();
                int readerMainQueueSize = relayerConfiguration.getReaderMainQueueSize();
                int readerBatchSize = relayerConfiguration
                        .getReaderBatchSize();         // How many messages to fetch in a single iteration.
                int readerParallelismDegree = relayerConfiguration
                        .getReaderParallelismDegree();    // While reading how many threads to spawn.
                int processorParallelismDegree = relayerConfiguration
                        .getProcessorParallelismDegree(); // Number of subProcessors / Consumers
                int processorQueueSize = relayerConfiguration
                        .getProcessorQueueSize();       // Number of messages per subProcessor/Consumer
                int lastProcessedPersistInterval =
                        relayerConfiguration.getLastProcessedPersistInterval();

                String varidhiServer = StringUtils.isNotBlank(relayerConfiguration.getDestinationServer()) ?
                        relayerConfiguration.getDestinationServer() :
                        applicationConfiguration.getDefaultDestinationServer();

                URL url = new URL(varidhiServer);
                String varadhiAuthTargetClientID = VARADHI_AUTH_TARGET_CLIENT_ID.replace(HOST, url.getHost());
                relayerConfiguration.setVaradhiAuthTargetClientID(varadhiAuthTargetClientID);

                HashingAlgorithm hashingAlgorithm = relayerConfiguration.getHashingAlgorithm() != null ?
                        relayerConfiguration.getHashingAlgorithm() : applicationConfiguration.getHashingAlgorithm();
                //choosing the default app username if the username at shard level is not present
                String appUserName = StringUtils.isNotBlank(relayerConfiguration.getAppUserName()) ?
                        relayerConfiguration.getAppUserName() : applicationConfiguration.getAppUserName();

                boolean preventReRelayOnStartUp = shouldPreventReRelayOnStartUp(relayerConfiguration, outboundRepository);
                relayerConfiguration.setPreventReRelayOnStartUp(preventReRelayOnStartUp);

                boolean shouldCreateAlert = relayerConfiguration.getCreateAlert() == null ?
                        applicationConfiguration.getDefaultCreateAlert() :
                        relayerConfiguration.getCreateAlert();

                Relayer relayer = new Relayer(CommonUtils.generateRelayerUUID(), relayerId, varidhiServer, readerMainQueueSize, readerBatchSize,
                        readerParallelismDegree, processorParallelismDegree, processorQueueSize,
                        lastProcessedPersistInterval, applicationRepository, outboundRepository, metricRepository,
                        relayerConfiguration, hashingAlgorithm, appUserName,
                        applicationConfiguration.getMockConfig(), dbSchemaValidator,
                        shouldCreateAlert, relayerConfiguration.getActive(),
                        applicationConfiguration.getHttpAuthConfig(),httpAuthenticationService,applicationConfiguration.getAlertProviderConfig());

                relayerHandleContainer.addRelayerHandle(relayerId, relayer);

            }
        } catch (Exception e) {
            logger.error("System.exit::Error while initializing Relayers", e);
            System.exit(-1);
        }
        return relayerHandleContainer;
    }

    @Inject
    @Singleton
    private void createLeaderElectionExecutor(RelayerHandleContainer relayerHandleContainer,
                                              RelayerMetricHandleContainer relayerMetricHandleContainer,
                                              ApplicationConfiguration applicationConfiguration) {

        ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(
                applicationConfiguration.getRelayers().size(),
                new ThreadFactoryBuilder().setDaemon(true).setPriority(Thread.MAX_PRIORITY).setNameFormat("LeaderElectionThread").build());
        for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
            RelayerConfiguration relayerConfiguration = relayer.getRelayerConfiguration();
            if (relayerConfiguration.isLeaderElectionEnabled()) {
                RelayerMetrics relayerMetrics = relayerMetricHandleContainer.getRelayerMetricsHandle(relayer.getRelayerId());
                Runnable leaderElector = new LeaderElection(relayer, relayer.getOutboundRepository(), relayerMetrics);
                scheduledService.scheduleAtFixedRate(leaderElector, (long) (Math.random() * 100),
                        relayerConfiguration.getLeaderElectionPingInterval() * 1000, TimeUnit.MILLISECONDS);
            }
        }
    }

    private ApplicationRepository getApplicationRepository(SessionFactoryContainer sessionFactoryContainer, ApplicationConfiguration applicationConfiguration, RelayerConfiguration relayerConfiguration, ExchangeTableNameProvider exchangeTableNameProvider) {
        ApplicationRepository applicationRepository = null;
        if (relayerConfiguration.getAppDbRef().getType() == DbType.MYSQL) {
            PartitionConfiguration partitionConfiguration = applicationConfiguration.getPartitionConfigForRelayerConfig(relayerConfiguration);
            SchemaCreator schemaCreator;
            SessionFactory sessionFactory = sessionFactoryContainer.getSessionFactory(relayerConfiguration.getAppDbRef().getId());
            if (PartitionMode.INACTIVE == partitionConfiguration.getMode()) {
                logger.warn("PartitionConfiguration is in INACTIVE state. Creating App Tables (if required) without Partition.");
                schemaCreator = new SchemaCreator(() -> sessionFactory.openSession(), exchangeTableNameProvider);
            } else {
                schemaCreator = new SchemaCreator(() -> sessionFactory.openSession(), exchangeTableNameProvider,
                        partitionConfiguration.getSize(), partitionConfiguration.getNoOfExtraPartitions(), partitionConfiguration.getMode());
            }
            schemaCreator.ensureAppSchema();


            applicationRepository = new ApplicationRepositoryImpl(sessionFactoryContainer
                    .getSessionFactory(relayerConfiguration.getAppDbRef().getId()),
                    exchangeTableNameProvider);
        } else if (relayerConfiguration.getAppDbRef().getType() == DbType.TDS) {
            applicationRepository = new TDSApplicationRepositoryImpl(sessionFactoryContainer
                    .getSessionFactory(relayerConfiguration.getAppDbRef().getId()),
                    exchangeTableNameProvider);
        }
        return applicationRepository;
    }

    private OutboundRepository getOutboundRepository(SessionFactoryContainer sessionFactoryContainer, ApplicationConfiguration applicationConfiguration, RelayerConfiguration relayerConfiguration, ExchangeTableNameProvider exchangeTableNameProvider) {
        OutboundRepository outboundRepository = null;
        if (relayerConfiguration.getOutboundDbRef().getType() == DbType.MYSQL) {
            SessionFactory sessionFactory = sessionFactoryContainer
                    .getSessionFactory(relayerConfiguration.getOutboundDbRef().getId());
            PartitionConfiguration partitionConfiguration = applicationConfiguration.getPartitionConfigForRelayerConfig(relayerConfiguration);
            if (partitionConfiguration == null) {
                logger.error("Partition configuration is null for relayer: " + relayerConfiguration.getRelayerId());
                System.exit(1);
            }
            SchemaCreator schemaCreator;
            if (PartitionMode.INACTIVE == partitionConfiguration.getMode()) {
                logger.warn("PartitionConfiguration is in INACTIVE state. Creating Outbound Tables (if required) without Partition.");
                schemaCreator = new SchemaCreator(() -> sessionFactory.openSession(), exchangeTableNameProvider);
            } else {
                schemaCreator = new SchemaCreator(() -> sessionFactory.openSession(), exchangeTableNameProvider,
                        partitionConfiguration.getSize(), partitionConfiguration.getNoOfExtraPartitions(), partitionConfiguration.getMode());
            }
            schemaCreator.ensureOutboundSchema();


            outboundRepository = new OutboundRepositoryImpl(sessionFactory, exchangeTableNameProvider);
        }
        return outboundRepository;
    }

    private RelayerMetricRepository getMetricRepository(SessionFactoryContainer sessionFactoryContainer, OutboundRepository outboundRepository,RelayerConfiguration relayerConfiguration, ExchangeTableNameProvider exchangeTableNameProvider) {
        RelayerMetricRepository repository = null;
        if (relayerConfiguration.getMetricDbRef() != null && relayerConfiguration.getMetricDbRef().getType() == DbType.MYSQL) {
            SessionFactory sessionFactory = sessionFactoryContainer
                    .getSessionFactory(relayerConfiguration.getMetricDbRef().getId());
            repository = new OutboundRepositoryImpl(sessionFactory, exchangeTableNameProvider);
        }
        return repository;
    }

    private boolean shouldPreventReRelayOnStartUp(RelayerConfiguration relayerConfiguration, OutboundRepository outboundRepository) {
        return Boolean.valueOf(System.getProperty("relayer.preventReRelayOnStartUp")) &&
                ((Integer)outboundRepository.getNumberOfProccessors()).equals(relayerConfiguration.getProcessorParallelismDegree());
    }

    public void setPartitionManagementJobForRelayerContainer(RelayerConfiguration relayerConfiguration,
                                                             PartitionManagementJob partitionManagementJob) {
        RelayerHandleContainer relayerHandleContainer = RelayerMainService.getInstance(RelayerHandleContainer.class);
        relayerHandleContainer.getRelayerHandle(relayerConfiguration.getRelayerId()).setPartitionManagementJob(partitionManagementJob);

    }

    @Provides
    @Singleton
    public RelayerMetricHandleContainer provideRelayerMetricHandleContainer(
            RelayerHandleContainer relayerHandleContainer) throws Exception {
        RelayerMetricHandleContainer relayerMetricHandleContainer = new MapMetricContainer();
        for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
            relayer.setRelayerMetrics(new RelayerMetrics(relayer.getRelayerId()));
            relayerMetricHandleContainer.addRelayerMetrics(relayer.getRelayerId(), relayer.getRelayerMetrics());
        }
        return relayerMetricHandleContainer;
    }

    public ApplicationPartitionRepository prepareApplicationPartitionRepository(
            RelayerConfiguration relayerConfiguration, SessionFactoryContainer sessionFactoryContainer,
            ExchangeTableNameProvider exchangeTableNameProvider,
            PartitionConfiguration partitionConfiguration, PartitionAlterMonitor partitionAlterMonitor) throws IllegalArgumentException {

        if (relayerConfiguration.getPartitionConfiguration() == null || relayerConfiguration.getPartitionConfiguration().getAppPartitionDbRef() == null) {
            throw new IllegalArgumentException("Invalid Partition Configuration for relayer: " + relayerConfiguration.getRelayerId());
        }
        return new ApplicationPartitionRepositoryImpl(sessionFactoryContainer
                .getSessionFactory(relayerConfiguration.getPartitionConfiguration().getAppPartitionDbRef().getId()),
                exchangeTableNameProvider, partitionConfiguration, relayerConfiguration, partitionAlterMonitor);
    }

    public OutboundPartitionRepository prepareOutboundPartitionRepository(
            RelayerConfiguration relayerConfiguration, SessionFactoryContainer sessionFactoryContainer,
            ExchangeTableNameProvider exchangeTableNameProvider,
            PartitionConfiguration partitionConfiguration, PartitionAlterMonitor partitionAlterMonitor) {
        OutboundPartitionRepository outboundRepository = null;
        if (relayerConfiguration.getOutboundDbRef().getType() == RepositoryConfiguration.DbType.MYSQL) {
            outboundRepository = new OutboundPartitionRepositoryImpl(sessionFactoryContainer
                    .getSessionFactory(relayerConfiguration.getOutboundDbRef().getId()),
                    exchangeTableNameProvider, partitionConfiguration, relayerConfiguration, partitionAlterMonitor);
        }
        return outboundRepository;
    }
}
