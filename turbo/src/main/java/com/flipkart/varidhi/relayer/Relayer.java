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

package com.flipkart.varidhi.relayer;


import com.flipkart.turbo.config.AlertProviderConfig;
import com.flipkart.turbo.config.HttpAuthConfig;
import com.flipkart.turbo.tasks.ProcessorTask;
import com.flipkart.varidhi.config.HashingAlgorithm;
import com.flipkart.varidhi.config.MockConfig;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.HttpAuthenticationService;
import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.jobs.PartitionManagementJob;
import com.flipkart.varidhi.relayer.common.ProcessedMessageLagTime;
import com.flipkart.varidhi.relayer.distributor.Distributor;
import com.flipkart.varidhi.relayer.distributor.tasks.DistributorTask;
import com.flipkart.varidhi.relayer.distributor.tasks.SequentialDistributionTask;
import com.flipkart.varidhi.relayer.distributor.tasks.output.DistributorOutput;
import com.flipkart.varidhi.relayer.distributor.tasks.output.DistributorProcessorTypeOutput;
import com.flipkart.varidhi.relayer.processor.Processor;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.HttpMessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MetricedMessageRelayer;
import com.flipkart.varidhi.relayer.processor.tasks.InformExistingSidelinedGroupTask;
import com.flipkart.varidhi.relayer.reader.Reader;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.ForcedRelayMessage;
import com.flipkart.varidhi.relayer.reader.models.Message;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.outputs.QueueOutputHandler;
import com.flipkart.varidhi.relayer.reader.tasks.BatchReadMessagesTask;
import com.flipkart.varidhi.relayer.reader.tasks.ReaderTask;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaDiff;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBValidatorResult;
import com.flipkart.varidhi.relayer.schemavalidator.validator.DBSchemaValidator;
import com.flipkart.varidhi.utils.CommonUtils;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * *
 * Author: abhinavp
 * Date: 04-Jul-2015
 *
 */
public class Relayer {
    public static final String DEFAULT_RELAYER_ID = "default";
    String relayerId;
    @Getter
    private final String relayerUUID;
    Reader reader;
    Distributor taskDistributor;
    Processor taskProcessor;
    int readerMainQueueSize;
    int readerBatchSize;
    int readerParallelismDegree;
    int processorParallelismDegree;
    int processorQueueSize;
    int lastProcessedPersistInterval;
    BlockingQueue<ProcessorTask> mainQueue;
    String varidhiServer;
    RelayerApplicationRepository applicationRepository;
    RelayerOutboundRepository outboundRepository;
    RelayerMetricRepository metricRepository;
    private RelayerConfiguration relayerConfiguration;
    private final HashingAlgorithm hashingAlgorithm;
    private PartitionManagementJob partitionManagementJob;
    private DBSchemaValidator dbSchemaValidator;
    private Logger logger;
    private Boolean isActive = false;
    private Boolean isRunning = false;
    private String appUserName;
    private MockConfig mockConfig;
    private boolean shouldCreateAlert;
    private OutputHandler outputHandler;
    private RelayerMetrics relayerMetrics;
    private HttpAuthConfig httpAuthConfig;
    private HttpAuthenticationService httpAuthenticationService;
    @Getter
    private AlertProviderConfig alertProviderConfig;

    public Relayer(String relayerUUID, String relayerId, String varidhiServer, int readerMainQueueSize,
                   int readerBatchSize, int readerParallelismDegree, int processorParallelismDegree,
                   int processorQueueSize, int lastProcessedPersistInterval,
                   RelayerApplicationRepository applicationRepository,
                   RelayerOutboundRepository outboundRepository, RelayerMetricRepository metricRepository,
                   @NotNull RelayerConfiguration relayerConfiguration,
                   HashingAlgorithm hashingAlgorithm, String appUserName, MockConfig mockConfig,
                   DBSchemaValidator dbSchemaValidator, boolean shouldCreateAlert, boolean isActive,
                   HttpAuthConfig httpAuthConfig, HttpAuthenticationService httpAuthenticationService,
                   AlertProviderConfig alertProviderConfig)  {
        logger = LoggerFactory.getLogger(Relayer.class.getCanonicalName() + " " + relayerId);
        this.relayerUUID = relayerUUID;
        this.relayerId = relayerId;
        this.readerMainQueueSize = readerMainQueueSize;
        this.readerBatchSize = readerBatchSize;
        this.readerParallelismDegree = readerParallelismDegree;
        this.processorParallelismDegree = processorParallelismDegree;
        this.processorQueueSize = processorQueueSize;
        this.lastProcessedPersistInterval = lastProcessedPersistInterval;
        this.varidhiServer = varidhiServer;
        this.applicationRepository = applicationRepository;
        this.outboundRepository = outboundRepository;
        this.metricRepository = metricRepository;
        this.relayerConfiguration = relayerConfiguration;
        this.hashingAlgorithm = hashingAlgorithm;
        this.appUserName = appUserName;
        this.mockConfig = mockConfig;
        this.dbSchemaValidator = dbSchemaValidator;
        this.shouldCreateAlert = shouldCreateAlert;
        this.isActive = isActive;
        this.httpAuthConfig = httpAuthConfig;
        this.httpAuthenticationService = httpAuthenticationService;
        this.alertProviderConfig = alertProviderConfig;
    }

    public RelayerOutboundRepository getOutboundRepository() {
        return outboundRepository;
    }

    public void setPartitionManagementJob(PartitionManagementJob partitionManagementJob) {
        this.partitionManagementJob = partitionManagementJob;
    }

    public void init() {
        synchronized (this) {
            if(isRunning){
                logger.info("Relayer already running : " + relayerId);
                return;
            }
            try {
                initializeRelayer();
            } catch (Exception e){
                logger.error("Exception occurred while initializing relayer ",e);
                throw e;
            }
        }
    }

    private void initializeRelayer() {
        logger.info("Starting Relayer : " + relayerId);

        mainQueue = new LinkedBlockingQueue<>(readerMainQueueSize);

        resetIncompleteStates();

        logger.info("Initializing processor for relayer :" + relayerId);
        HashMap <String , Long > subprocessorToMessageIdMap;
        if(this.relayerConfiguration.getTurboReadMode() == TurboReadMode.OUTBOUND_READER) {
            subprocessorToMessageIdMap = outboundRepository.processorsIdMap(outboundRepository.getLastProcessedMessages());
        } else {
            subprocessorToMessageIdMap = applicationRepository.processorsIdMap(outboundRepository.getLastProcessedMessages());
        }


        this.taskProcessor = new Processor("Processor_" + relayerId,relayerId, this.processorParallelismDegree,
                this.processorQueueSize, lastProcessedPersistInterval, outboundRepository,
                outboundRepository, new MetricedMessageRelayer(relayerId,
                new HttpMessageRelayer(appUserName, this.relayerConfiguration.isEnableCustomRelay(),
                        this.relayerConfiguration, mockConfig, httpAuthConfig, httpAuthenticationService)),
                hashingAlgorithm ,subprocessorToMessageIdMap,this);


        informExisitingSidelinedGroupsToProcessor();
        logger.info("Processor Ready for relayer :" + relayerId);

        logger.info("Initializing distributor for relayer :" + relayerId);
        DistributorOutput<ProcessorTask> distributorOutput =
            new DistributorProcessorTypeOutput(this.taskProcessor);
        DistributorTask sequentialDistributionTask =
            new SequentialDistributionTask<>(mainQueue, distributorOutput);
        this.taskDistributor =
            new Distributor("Distributor_" + relayerId, sequentialDistributionTask,relayerMetrics);

        Thread distributorThread = new Thread(taskDistributor,"Distributor_Thread");
        distributorThread.start();
        logger.info("Distributor Ready for relayer :" + relayerId);

        logger.info("Initializing Reader for relayer :" + relayerId);
        outputHandler =
            new QueueOutputHandler<>(new ProcessorTaskFactory(this.varidhiServer,this.partitionManagementJob), mainQueue);


        AppMessageMetaData lastProcessedMessage = getMinLastProcessedMessage();
        logger.info("Starting reading messages from seqId : " + lastProcessedMessage.getId());
        ReaderTask readerTask =
            new BatchReadMessagesTask(lastProcessedMessage, this.readerBatchSize);
        this.reader =
            new Reader("Reader_" + relayerId, readerParallelismDegree, readerTask, outputHandler,
                applicationRepository, outboundRepository, relayerConfiguration, this);
        Thread readerThread = new Thread(reader,"Reader_Thread");
        readerThread.start();
        logger.info("Reader ready for relayer :" + relayerId);

        isActive = true;
        isRunning = true;
        logger.info("Successfully Started Relayer : " + relayerId);

    }

    private void informExisitingSidelinedGroupsToProcessor() {
        logger.warn("Informing existing sidelined groups to processor!");
        List<String> groupIds = outboundRepository.getSidelinedGroups();
        for (String groupId : groupIds) {
            logger.debug("Informing existing sidelined group to processor with id : " + groupId);
            taskProcessor.submitTask(new InformExistingSidelinedGroupTask(groupId));
        }
        logger.warn("Informing existing sidelined groups complete");
    }

    private AppMessageMetaData getMinLastProcessedMessage() {
        List<String> messageIds = outboundRepository.getLastProcessedMessageIdsWithSystemExit();

        if(this.relayerConfiguration.getTurboReadMode() == TurboReadMode.OUTBOUND_READER) {
            return outboundRepository.getMinMessageFromMessages(messageIds);
        }
        return applicationRepository.getMinMessageFromMessages(messageIds);
    }


    //Scenario to note : If relayer is down for more than skippedId timeout considered, even after reset messages will not
    //be picked as we don't support messages earlier than timeout (i.e. 30 minutes)

    private void resetProcessingStateSkippedIds() {
        logger.debug("Resetting states of processing skippedIds starting!");
        outboundRepository.resetProcessingStateSkippedIds();
        logger.debug("Resetting states of processing skippedIds completed!");
    }

    private void resetProcessingStateControlTasks() {
        logger.debug("Resetting states of processing Control Tasks starting!");
        outboundRepository.resetProcessingStateControlTasks();
        logger.debug("Resetting states of processing Control Tasks completed!");
    }

    private void resetProcessingStateSidelinedMessages() {
        logger.debug("Resetting states of processing Sideline messages starting!");
        outboundRepository.resetProcessingStateSidelinedMessages();
        logger.debug("Resetting states of processing Sideline messages completed!");
    }

    private void resetIncompleteStates() {
        resetProcessingStateSkippedIds();
        resetProcessingStateControlTasks();
        resetProcessingStateSidelinedMessages();
    }

    // Below are handles to outside world, to talk to Relayer
    public Boolean createUnsidelineGroupTask(String groupId) {
        return outboundRepository.createUnsidelineGroupTask(groupId);
    }

    public Boolean createUnsidelineMessageTask(String messageId) {
        return outboundRepository.createUnsidelineMessageTask(messageId);
    }

    public Boolean createUnsidelineAllUngroupedMessageTask(Date fromDate, Date toDate) {
        return outboundRepository.createUnsidelineAllUngroupedMessageTask(fromDate, toDate);
    }

    public Boolean createUnsidelineMessagesBetweenDatesTask(Date fromDate, Date toDate) {
        return outboundRepository.createUnsidelineMessagesBetweenDatesTask(fromDate, toDate);
    }



    /*
     *  methods related to metric collector
     */
    public ProcessedMessageLagTime getProcessedMessageMinMaxLagTime(){
        return metricRepository.getProcessedMessageMinMaxLagTime();
    }

    public Integer currentSidelinedMessageCount() {
        return metricRepository.getCurrentSidelinedMessageCount();
    }

    public Integer currentSidelinedGroupsCount() {
        return metricRepository.getCurrentSidelinedGroupsCount();
    }

    public Integer pendingMessageCount() {
        return metricRepository.getPendingMessageCount();
    }

    public Long getMaximumPartitionId() {
        return metricRepository.getMaxParitionID();
    }

    public Long getMaxMessageID() {
        return metricRepository.getMaxMessageId();
    }

    //method to provide average and the maximum lag time to the metrics
    public HashMap < String , Long > getMessageLagTime(){
        if(taskProcessor == null) {
            return null;
        }
        return taskProcessor.getMessageLagTime();
    }

    public Long numberOfMessagesProcessedFromLastLookup() {
        if(taskProcessor == null) {
            return null;
        }
        return taskProcessor.numberOfMessagesProcessedFromLastLookup();
    }




    /*
     *   relayer related methods
     */
    public Boolean createPartitionManagementTask() {
        if (this.partitionManagementJob == null) {
            logger.warn("Partition Configuration Not found/Active for Relayer: " + this.relayerId);
            return false;
        }
        return outboundRepository.createPartitionManagementTask(this.relayerId);
    }

    public void runPartitionManagementJob() {
        if(partitionManagementJob == null) {
            logger.error("PartitionManagementJob is null in relayer. Not running partition Management in relayer: " + relayerId);
            return;
        }
        partitionManagementJob.run();
    }

    public String createForcedMessageRelayerTask(@NonNull List<String> messageIDs) {
        if(messageIDs.size() > readerBatchSize/readerParallelismDegree) {
            return "Size of Unique MessageIDs exceeded the threshold (readerBatchSize/readerParallelismDegree)";
        }
        Map<String, Message> messages = outboundRepository.readMessages(messageIDs);
        if(messageIDs.size() != messages.size()) {
            String error = "Not all message IDs given for Relay Messsage task are present in outboundDB. ";
            logger.error(error + messageIDs);
            return error;
        }
        for(String messageID : messageIDs) {
            ForcedRelayMessage forcedRelayMessage = new ForcedRelayMessage(messages.get(messageID));
            outputHandler.submit(forcedRelayMessage);
        }
        return "Successfully Queued Messages. Check logs and destination_response_status column in messages table.";
    }

    public RelayerConfiguration getRelayerConfiguration() {
        return relayerConfiguration;
    }


    public Integer isDbDead(){
        if(taskProcessor == null) {
            return  null;
        }
        return taskProcessor.isDbDead();
    }


    public Boolean isActive() {
        return this.isActive;
    }

    public Boolean markRelayerActive() {
        return this.isActive = true;
    }

    public Boolean markRelayerInactive() {
        return this.isActive = false;
    }

    public Boolean isRunning() {
        return this.isRunning;
    }

    public String getActiveRunningRelayerIP(){
        String leaderRelayerUUID = outboundRepository.getLeaderRelayerUUID(relayerId);
        return CommonUtils.getRelayerIPFromUUID(leaderRelayerUUID);
    }

    public void stop(){
        synchronized (this) {
            if(!isRunning){
                logger.info("Relayer already stopped : " + relayerId);
                return;
            }
            try {
                stopRelayer();
            } catch (Exception e){
                logger.error("Exception occurred while stopping relayer ",e);
                throw e;
            } finally {
                relayerMetrics.resetAllMetrics();
            }
        }
    }

    public void halt(){
        synchronized (this) {
            if(!isRunning){
                logger.info("Relayer already halted : " + relayerId);
                return;
            }
            try {
                haltRelayer();
            } catch (Exception e){
                logger.error("Exception occurred while halting relayer ",e);
                throw e;
            } finally {
                relayerMetrics.resetAllMetrics();
            }
        }
    }

    private void stopRelayer() {
        // Stopping the Main Reader Thread and main Distributor Thread along with the child process
        logger.info("Stopping the relayer with id : " + relayerId + "  (starting the shutdown process)");
        this.reader.stop();
        this.taskDistributor.stop();
        this.taskProcessor.stopExecution();
        //this.isActive = false;
        this.isRunning = false;
        logger.info("RELAYER WITH id :" + relayerId + " STOPPED COMPLETELY :)");
    }

    private void haltRelayer() {
        logger.info("Halting the relayer with id : " + relayerId + "  (starting the halting process)");
        this.reader.halt();
        this.taskDistributor.halt();
        this.taskProcessor.haltExecution();
        this.isRunning = false;
        logger.info("RELAYER WITH id :" + relayerId + " HALTED COMPLETELY :)");
    }

    public RelayerMetrics getRelayerMetrics() {
        return relayerMetrics;
    }

    public void setRelayerMetrics(RelayerMetrics relayerMetrics) {
        this.relayerMetrics = relayerMetrics;
    }


    public String getRelayerId() {
        return relayerId;
    }


    public boolean shouldCreateAlert() {
        return shouldCreateAlert;
    }

    public Integer getMainQueueRemainingCapacity() {
        if(mainQueue != null) {
            return mainQueue.remainingCapacity();
        }
        return null;
    }

    public void publishSubProcessorRemainingQueueSize() {
        if(taskProcessor != null) {
            taskProcessor.publishSubProcessorRemainingQueueSize();
        }
    }

    public DBValidatorResult validateDBSchema() {
        DBValidatorResult response = new DBValidatorResult();
        List<DBSchemaDiff> dbSchemaDiffList = new ArrayList<>();

        if (this.relayerConfiguration.getTurboReadMode() != TurboReadMode.OUTBOUND_READER) {
            List<DBSchemaDiff> appSchemaDiffList = dbSchemaValidator.validateAppSchema();
            dbSchemaDiffList.addAll(appSchemaDiffList);
        }

        List<DBSchemaDiff> outboundSchemaDiffList = dbSchemaValidator.validateOutboundSchema();
        dbSchemaDiffList.addAll(outboundSchemaDiffList);

        Map<String,Set<String>> charsetDiff = dbSchemaValidator.validateCharSet();

        response.setRelayerName(this.relayerId);
        response.setSchemaDiff(dbSchemaDiffList);
        response.setCharset(charsetDiff);
        return response;
    }

}
