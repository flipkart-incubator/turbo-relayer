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

package com.flipkart.varidhi.relayer.reader;

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.RelayerMetric;
import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.relayer.reader.models.*;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.utils.Constants;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


@Getter
public class DefaultMessageReader implements MessageReader {

    Logger logger;
    int readerMaxParallelismDegree;
    ReaderApplicationRepository readerApplicationRepository;
    ReaderOutboundRepository readerOutboundRepository;
    OutputHandler outputHandler;
    ThreadPoolExecutor readerExecutor;
    RelayerConfiguration relayerConfiguration;
    RelayerMetric<Integer> messageNotInOutboundDb;

    public DefaultMessageReader(String executorId, int readerMaxParallelismDegree,
                                ReaderApplicationRepository readerApplicationRepository, ReaderOutboundRepository readerOutboundRepository,
                                OutputHandler outputHandler, ThreadPoolExecutor readerExecutor, RelayerConfiguration relayerConfiguration) {
        this.logger = LoggerFactory
                .getLogger(DefaultMessageReader.class.getCanonicalName() + " " + executorId);
        this.messageNotInOutboundDb = new RelayerMetric<>(relayerConfiguration.getRelayerId(), "messages.messageNotInOutboundDb", true);
        if (readerMaxParallelismDegree <= 0) {
            throw new RuntimeException(
                    "Read Thread Count should be greater than 0: " + readerMaxParallelismDegree);
        }

        this.readerMaxParallelismDegree = readerMaxParallelismDegree;
        this.readerApplicationRepository = readerApplicationRepository;
        this.readerOutboundRepository = readerOutboundRepository;
        this.outputHandler = outputHandler;
        this.readerExecutor = readerExecutor;
        this.relayerConfiguration = relayerConfiguration;

    }

    @Override
    public void addToSkipMessagesInBatches(long start, long nextId, int batchSize) {
        logger.info("Keeping in SkippedIds from: [" + start + "->" + nextId + ") in batches of " + batchSize);
        for (long missedId = start; missedId <= nextId; ) {
            if (missedId + batchSize >= nextId) {
                addToSkipMessages(missedId, nextId);
                break;
            } else {
                addToSkipMessages(missedId, missedId + batchSize);
                missedId += batchSize;
            }
        }
        logger.info("Completed keeping in SkippedIds from: [" + start + "->" + nextId + ") in batches of " + batchSize);
    }

    @Override
    public void addToSkipMessages(long start, long nextId) {
        List<Long> missedIds = new ArrayList<>();
        for (long missedId = start; missedId < nextId; missedId++) {
            missedIds.add(missedId);
            logger.debug("Skipped ID Found: " + missedId + " putting into Skipped ID queue.");
        }
        logger.info("Putting SkippedIds from: [" + start + "->" + nextId + ") in the Skipped ID queue");
        readerOutboundRepository.persistSkippedIds(missedIds);
    }

    @Override
    public List<ControlTask> readAndLockControlTasks(int count) {
        List<ControlTask> tasks = readerOutboundRepository.readControlTasks(count);

        //LOCK
        readerOutboundRepository.updateControlTaskStatus(tasks, ControlTaskStatus.PROCESSING);
        return tasks;
    }

    @Override
    public List<UnsidelinedMessage> readAndLockUnsidelineMessages(int count) {
        List<String> unsidelinedMessageIds =
                readerOutboundRepository.readUnsidelinedMessageIds(count);
        List<UnsidelinedMessage> unsidelinedMessages = new ArrayList<>();

        if (unsidelinedMessageIds == null || unsidelinedMessageIds.size() == 0)
            return unsidelinedMessages;

        Map<String, Message> messages =
                readerOutboundRepository.readMessages(unsidelinedMessageIds);
        for (Message messageObj : messages.values()) {
            if (messageObj != null) {
                // Ideally unsidelinedMessageSeqId should be sent. But there is no use of that as of now. So saving one query.
                UnsidelinedMessage unsidelinedMessage =
                        new UnsidelinedMessage(messageObj, messageObj.getId());
                unsidelinedMessages.add(unsidelinedMessage);
            }
        }

        //LOCK
        readerOutboundRepository
                .updateSidelinedMessageStatus(unsidelinedMessageIds, SidelinedMessageStatus.PROCESSING);
        return unsidelinedMessages;
    }

    @Override
    public List<SkippedMessage> readAndLockSkippedMessages(int count, Timestamp lastMsgTime) {
        List<SkippedMessage> skippedMessages = new ArrayList<>();

        long maxApplicationTransactionTimeInMin =
                relayerConfiguration.getMaxApplicationTransactionTime()
                        / (Constants.MILLISECONDS_TO_MIN_FACTOR);
        List<Long> skippedAppSequenceIds = readerOutboundRepository
                .readSkippedAppSequenceIds(maxApplicationTransactionTimeInMin, lastMsgTime);
        if (skippedAppSequenceIds == null || skippedAppSequenceIds.size() == 0)
            return skippedMessages;

        Map<Long, AppMessageMetaData> skippedMessageIds =
                readerApplicationRepository.getMessageMetaData(skippedAppSequenceIds, count);

        Map<String, Long> messageIdToAppSequenceIdMap = new HashMap<>();
        for (Map.Entry<Long, AppMessageMetaData> entry : skippedMessageIds.entrySet()) {
            if (entry.getValue() != null) {
                messageIdToAppSequenceIdMap.put(entry.getValue().getMessageId(), entry.getKey());
            }
        }

        Map<String, Message> messages = readerOutboundRepository
                .readMessages(new ArrayList<>(messageIdToAppSequenceIdMap.keySet()));
        for (Message messageObj : messages.values()) {
            SkippedMessage skippedMessage = new SkippedMessage(messageObj,
                    messageIdToAppSequenceIdMap.get(messageObj.getMessageId()));
            skippedMessages.add(skippedMessage);
        }

        //LOCK
        readerOutboundRepository
                .updateSkippedIdStatus(new ArrayList<>(messageIdToAppSequenceIdMap.values()),
                        SkippedIdStatus.PROCESSING);
        return skippedMessages;
    }


    private List<OutboundMessage> getOutboundMessages(Map<String, Long> messageIdtoAppSeqIdMap) {
        List<String> messageNotInOutboundDbList = new ArrayList<>();
        Map<String, Message> messages = readerOutboundRepository
                .readMessages(new ArrayList<>(messageIdtoAppSeqIdMap.keySet()));
        List<OutboundMessage> outboundMessages = new ArrayList<>();
        for (Map.Entry<String, Long> messageIdToSeq : messageIdtoAppSeqIdMap.entrySet()) {
            Message messageObj = messages.get(messageIdToSeq.getKey());
            if (messageObj != null) {
                OutboundMessage outboundMessage = new OutboundMessage(messageObj, messageIdToSeq.getValue());
                outboundMessages.add(outboundMessage);
            } else {
                messageNotInOutboundDbList.add(messageIdToSeq.getKey());
            }
        }
        logger.warn("Message ids not found in outbound database: " + messageNotInOutboundDbList);
        messageNotInOutboundDb.updateMetric(messageNotInOutboundDbList.size());
        return outboundMessages;
    }

    @Override
    public List<OutboundMessage> getOutboundMessagesInParallel(Long start, int count, int delayedReadIntervalInSeconds) {
        List<OutboundMessage> outboundMessages = new ArrayList<>();

        List<AppMessageMetaData> appMessageMetaDataList =
                readerApplicationRepository.getMessageMetaData(start, count, delayedReadIntervalInSeconds);
        int appMsgCount = appMessageMetaDataList.size();

        if (appMsgCount > 0) {
            int perThreadMapSize =
                    (int) Math.ceil(((double) appMsgCount) / this.readerMaxParallelismDegree);

            final Map<Integer, List<OutboundMessage>> resultMap = new ConcurrentHashMap<>();

            List<Callable<Void>> commands = new ArrayList<>();

            int readMsgCount = 0;
            int remainingMsgCount = appMsgCount;
            for (int threadId = 0; threadId < this.readerMaxParallelismDegree; threadId++) {
                final int currentThreadId = threadId;
                final Map<String, Long> messageIdtoAppSeqIdMap = new LinkedHashMap<>();
                for (int mapSize = 0; (mapSize < perThreadMapSize
                        && remainingMsgCount > 0); mapSize++, readMsgCount++, remainingMsgCount--) {
                    messageIdtoAppSeqIdMap
                            .put(appMessageMetaDataList.get(readMsgCount).getMessageId(),
                                    appMessageMetaDataList.get(readMsgCount).getId());
                }
                commands.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        List<OutboundMessage> objects = getOutboundMessages(messageIdtoAppSeqIdMap);
                        resultMap.put(currentThreadId, objects);
                        return null;
                    }
                });
            }

            outboundMessages = fetchMessages(commands,resultMap);
        }
        return outboundMessages;
    }


    List<OutboundMessage> fetchMessages(final List<Callable<Void>> commands, final Map<Integer, List<OutboundMessage>> resultMap){
        List<OutboundMessage> outboundMessages = new ArrayList<>();
        try {
            List<Future<Void>> futures = readerExecutor.invokeAll(commands);
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            logger.error("getOutboundMessagesInParallel: Error in Running readerExecutor: " + e
                    .getMessage(), e);
        }

        for (int threadId = 0; threadId < this.readerMaxParallelismDegree; threadId++) {
            List<OutboundMessage> resultList = resultMap.get(threadId);

            if (resultList == null || resultList.size() == 0)
                continue;

            outboundMessages.addAll(resultList);
        }
        return outboundMessages;
    }

    @Override
    public AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds) {
        return readerApplicationRepository.messagesExistForFurtherOffset(currentOffset, delayedReadIntervalInSeconds);
    }

    @Override
    public List<AppMessageMetaData> readAndSubmitReRelayMessages(int count) {
        int batchSize = count;
        if(count > readerMaxParallelismDegree) {
            batchSize = count / readerMaxParallelismDegree;
        }
        // read all the ReRelay messages from app db (whose ids is between max and min of the max seq Id table )
        List<String> messageIds = readerOutboundRepository.getLastProcessedMessageids();
        List<AppMessageMetaData> messageMetaDataList = readerApplicationRepository.getMessageMetaData(messageIds);
        // empty list that means no messages to read .
        if (messageMetaDataList.size() == 0) {
            return null;
        }
        AppMessageMetaData maxMessageMetaData = messageMetaDataList.get(messageMetaDataList.size() - 1);
        AppMessageMetaData minMessageMetaData = messageMetaDataList.get(0);
        if (maxMessageMetaData.getId() == minMessageMetaData.getId()) {
            return null;
        }
        Long start = minMessageMetaData.getId();
        // Reading all the message between start and count until maxMessageId.
        while (start < maxMessageMetaData.getId()) {
            List<ReRelayMessage> reRelayMessageList;
            List<AppMessageMetaData> reRelayMessageMetaDataList;
            if ((start + batchSize) <= maxMessageMetaData.getId()) {
                reRelayMessageMetaDataList = readerApplicationRepository.getMessageMetaData(start, batchSize);
            } else {
                reRelayMessageMetaDataList = readerApplicationRepository.getMessageMetaData(start, (int) (maxMessageMetaData.getId() - start));
            }
            if (reRelayMessageMetaDataList.size() > 0) {
                // read the complete message object from the outbound repo
                HashMap<String, Long> messageIdToAppIdMap = new HashMap<>();
                for (AppMessageMetaData metaData : reRelayMessageMetaDataList) {
                    messageIdToAppIdMap.put(metaData.getMessageId(), metaData.getId());
                }
                reRelayMessageList = getReRelayMessages(messageIdToAppIdMap);
                // create and ReRelay message obj from the messages content
                // submit it to the queue
                submitReRelayMessages(reRelayMessageList);
            }
            start += batchSize;
        }
        return messageMetaDataList;
    }

    private List<ReRelayMessage> getReRelayMessages(HashMap<String, Long> messageIdToAppIdMap) {

        Map<String, Message> messages = readerOutboundRepository
                .readMessages(new ArrayList<>(messageIdToAppIdMap.keySet()));
        List<ReRelayMessage> reRelayMessages = new ArrayList<>();
        for (Message messageObj : messages.values()) {
            ReRelayMessage reRelayMessage = new ReRelayMessage(messageObj,
                    messageIdToAppIdMap.get(messageObj.getMessageId()));
            reRelayMessages.add(reRelayMessage);
        }

        return reRelayMessages;
    }

    protected void submitReRelayMessages(List<ReRelayMessage> reRelayMessages) {
        for (ReRelayMessage reRelayMessage : reRelayMessages) {
            logger.info("Submitting ReRelayMessage to Distributor: " + reRelayMessage.toString());
            outputHandler.submit(reRelayMessage);
        }
    }
}
