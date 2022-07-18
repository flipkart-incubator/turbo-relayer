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

package com.flipkart.varidhi.relayer.reader.taskExecutors;

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.ThreadPoolExecutorProvider;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.reader.*;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.BaseReadDomain;
import com.flipkart.varidhi.relayer.reader.models.ControlTask;
import com.flipkart.varidhi.relayer.reader.models.OutboundMessage;
import com.flipkart.varidhi.relayer.reader.models.SkippedMessage;
import com.flipkart.varidhi.relayer.reader.models.UnsidelinedMessage;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.relayer.reader.tasks.BatchReadMessagesTask;
import com.flipkart.varidhi.utils.LeadershipExpiryHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * *
 * Author: abhinavp
 * Date: 02-Jul-2015
 *
 */
public class BatchReadTaskExecutor extends ReaderTaskExecutor<BatchReadMessagesTask> {
    AtomicInteger shouldProcess = new AtomicInteger(1);
    OutputHandler outputHandler;
    ThreadPoolExecutor readerExecutor;
    private Logger logger;
    RelayerConfiguration relayerConfiguration;
    private MessageReader messageReader;
    private Relayer relayer;

    public BatchReadTaskExecutor(String executorId, int readerMaxParallelismDegree,
                                 ReaderApplicationRepository readerApplicationRepository,
                                 ReaderOutboundRepository readerOutboundRepository, OutputHandler outputHandler,
                                 RelayerConfiguration relayerConfiguration,@NotNull Relayer relayer) {
        super(executorId);

        logger = LoggerFactory
                .getLogger(BatchReadTaskExecutor.class.getCanonicalName() + " " + executorId);
        this.outputHandler = outputHandler;
        this.readerExecutor = ThreadPoolExecutorProvider
                .provideExecutor("BatchReader", "Relayer", readerMaxParallelismDegree,
                        2 * readerMaxParallelismDegree, 500, 2 * readerMaxParallelismDegree,
                        new CallerRunsPolicy(), logger);
        this.relayerConfiguration = relayerConfiguration;
        this.relayer = relayer;
        this.messageReader = ReaderFactory.getMessageReader(
                this.relayerConfiguration.getTurboReadMode(), executorId, readerMaxParallelismDegree,
                readerApplicationRepository, readerOutboundRepository, outputHandler,this.readerExecutor,relayerConfiguration);
    }

    /**
     * ##Reader Execute Logic##
     * Read Control task
     * Read Unsidelined messages
     * Read Skipped messages
     * Read Latest messages
     * <p>
     * This follow will help in managing messages in order.
     *
     * @param readerTask
     * @throws InterruptedException
     */
    @Override
    public void execute(BatchReadMessagesTask readerTask) {
        Long start = readerTask.getMessageStart().getId();
        int batchSize = readerTask.getMessageCount();
        long lastAppSeqId = start - 1;
        Timestamp lastAppMsgRelayedCreatedTime = readerTask.getMessageStart().getCreateDateTime();
        if (null == lastAppMsgRelayedCreatedTime) {
            lastAppMsgRelayedCreatedTime = new Timestamp(System.currentTimeMillis());
        }

        /* Code segment to skip all the Relayed messages in the previous instance of the relayer.
         * We defined ReRelayed message to be a message who has been already relayed and we dont want to relay it again on startup of the relayer.
         * Prerequisites -: The number of subprocessors and the hashing algo should not change.
         * If both of the variables are unchanged then only we can send ReRelayMessage.
         * */

        // Read all the ReRelay messages .
        if (relayerConfiguration.isPreventReRelayOnStartUp()) {
            logger.info("Reading ReRelayMessage from " + start + " and time " + lastAppMsgRelayedCreatedTime.toString());
            List<AppMessageMetaData> reRelayedMessages = messageReader.readAndSubmitReRelayMessages(batchSize);
            logger.info("Read messages from between minLastProcessedMessageId till maxLastProcessedMessageId ");
            // submit all the messages to the queue .
            // updating the value of the start variable and lastAppSeqId for the normal operation of the reader (normal Batch Read operations).
            if (reRelayedMessages != null && reRelayedMessages.size() > 0) {
                AppMessageMetaData newStartMessage = reRelayedMessages.get(reRelayedMessages.size() - 1);
                start = newStartMessage.getId();
                lastAppSeqId = start - 1;
                lastAppMsgRelayedCreatedTime = newStartMessage.getCreateDateTime() == null ? lastAppMsgRelayedCreatedTime :
                        newStartMessage.getCreateDateTime();
            }
        }

        logger.info("Batch Read Task started from " + start + " and time " + lastAppMsgRelayedCreatedTime.toString());

        while (shouldProcess.get() == 1) {
            try {
                if(relayer.getRelayerConfiguration().isLeaderElectionEnabled() &&
                        LeadershipExpiryHolder.isLeadershipExpired(relayer.getRelayerUUID())){
                    logger.error(String.format("BatchReadTaskExecutor :: Relayer lease expired halting relayer %s, uuid : %s",
                                    relayer.getRelayerId(),relayer.getRelayerUUID()));
                    relayer.halt();
                }
                // logger.debug("Reading control tasks!");

                List<ControlTask> tasks = messageReader.readAndLockControlTasks(batchSize);

                for (BaseReadDomain baseReadDomain : tasks) {
                    outputHandler.submit(baseReadDomain);
                }

                if (tasks.size() != 0)
                    logger.debug("Control tasks received :" + tasks.size());

                if (tasks.size() == batchSize) {
                    continue;
                }

                logger.debug("Reading unsidelined messages!");
                List<UnsidelinedMessage> unsidelinedMessages = messageReader.readAndLockUnsidelineMessages(batchSize);
                for (UnsidelinedMessage unsidelinedMessage : unsidelinedMessages) {
                    logger.info(unsidelinedMessage.toString());
                    outputHandler.submit(unsidelinedMessage);
                }

                if (unsidelinedMessages.size() != 0)
                    logger.info("Total Unsidelined messages received :" + unsidelinedMessages.size());

                if (unsidelinedMessages.size() == batchSize) {
                    continue;
                }


                logger.debug("Reading messages from : " + start + " count: " + batchSize);
                // THIS IS NEEDED FOR MESSAGE ORDERING AND SHOULD NOT BE MOVED!!!!!
                List<OutboundMessage> messages = messageReader.getOutboundMessagesInParallel(start, batchSize,
                        relayerConfiguration.getDelayedReadIntervalInSeconds());

                logger.debug("Reading skipped messages!");
                List<SkippedMessage> skippedMessages = messageReader.readAndLockSkippedMessages(batchSize, lastAppMsgRelayedCreatedTime);
                for (SkippedMessage skippedMessage : skippedMessages) {
                    logger.info(skippedMessage.toString());
                    outputHandler.submit(skippedMessage);
                }

                if (skippedMessages.size() != 0)
                    logger.info("Total Skipped messages received :" + skippedMessages);

                if (skippedMessages.size() == batchSize) {
                    continue;
                }

                for (OutboundMessage outboundMessage : messages) {
                    //Condition checks the next seqId of message to be processed is lastSeqId + 1 or not
                    //If Not, then all the sequence between them are added in skipIds
                    long msgId = outboundMessage.getAppSequenceId();
                    if (lastAppSeqId != readerTask.getMessageStart().getId() - 1
                            && msgId - lastAppSeqId != 1) {
                        messageReader.addToSkipMessages(lastAppSeqId + 1, msgId);
                    }
                    outputHandler.submit(outboundMessage);
                    lastAppSeqId = msgId;
                    lastAppMsgRelayedCreatedTime = outboundMessage.getCreateDateTime();
                }

                if (messages.size() == 0) {
                    //If there are no messages and future messages exists then start if increment for next batch
                    AppMessageMetaData msgMetaData = messageReader.messagesExistForFurtherOffset(start,relayerConfiguration.getDelayedReadIntervalInSeconds());
                    if (lastAppSeqId < start && msgMetaData != null) {

                        //Add to skip_messages table before moving start
                        if (msgMetaData.getId() > start) {
                            messageReader.addToSkipMessagesInBatches(start, msgMetaData.getId(),batchSize);
                        }

                        lastAppSeqId = msgMetaData.getId() - 1;
                        start = msgMetaData.getId();
                        lastAppMsgRelayedCreatedTime = msgMetaData.getCreateDateTime();
                        logger.debug("New messages found! Reader will continue!");
                    } else {
                        // If condition is not satisfied then, Cursor is at the end of messages;
                        logger.debug(
                                "No new message found! Reader sleeping for " + relayerConfiguration.getReaderSleepTime() + " ms!");
                        Thread.sleep(relayerConfiguration.getReaderSleepTime());
                    }
                } else {
                    // Batch size might be bigger than current number of messages.
                    start = messages.get(messages.size() - 1).getAppSequenceId() + 1;
                    logger.info("Last new messageId read is :" + (start - 1));
                }

                relayer.getRelayerMetrics().updateLastReadEpochTime(System.currentTimeMillis());
            } catch (Exception e) {
                logger.error("Error in BatchReadTaskExecutor execute Method: " + e.getMessage(), e);
            }
        }

        logger.info("Reading messages stopped! with last app message ID: " + lastAppSeqId);
    }

    @Override
    public void stopExecution() {
        shouldProcess.incrementAndGet();
        this.readerExecutor.shutdown();
    }

    @Override
    public void haltExecution() {
        shouldProcess.incrementAndGet();
        this.readerExecutor.shutdownNow();
    }

}
