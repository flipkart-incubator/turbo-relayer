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
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.relayer.reader.models.*;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;


public class OutboundMessageReader extends DefaultMessageReader{

    private Logger logger;
    public OutboundMessageReader(String executorId, int readerMaxParallelismDegree, ReaderOutboundRepository readerOutboundRepository, OutputHandler outputHandler, ThreadPoolExecutor readerExecutor, RelayerConfiguration relayerConfiguration) {
        super(executorId, readerMaxParallelismDegree, null, readerOutboundRepository, outputHandler, readerExecutor, relayerConfiguration);
        this.logger = LoggerFactory
                .getLogger(OutboundMessageReader.class.getCanonicalName() + " " + executorId);
    }

    private List<OutboundMessage> getOutboundMessages(List<Long> messageSequenceIds) {

        List<OutboundMessage> outboundMessages = new ArrayList<>();
        List<Message> messages = readerOutboundRepository.readMessagesUsingSequenceIds(messageSequenceIds);

        for(Message message : messages){
            outboundMessages.add(new OutboundMessage(message, message.getId()));
        }
        return outboundMessages;
    }

    @Override
    public List<OutboundMessage> getOutboundMessagesInParallel(Long start, int count, int delayedReadIntervalInSeconds) {
        List<OutboundMessage> outboundMessages = new ArrayList<>();

        List<AppMessageMetaData> appMessageMetaDataList =
                readerOutboundRepository.getMessageMetaData(start, count, delayedReadIntervalInSeconds);
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
                final List<Long> messageSequenceIds = new ArrayList<>();
                for (int mapSize = 0; (mapSize < perThreadMapSize
                        && remainingMsgCount > 0); mapSize++, readMsgCount++, remainingMsgCount--) {
                    messageSequenceIds.add(appMessageMetaDataList.get(readMsgCount).getId());
                }
                commands.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        List<OutboundMessage> objects = getOutboundMessages(messageSequenceIds);
                        resultMap.put(currentThreadId, objects);
                        return null;
                    }
                });
            }

            outboundMessages = super.fetchMessages(commands,resultMap);
        }
        return outboundMessages;
    }

    @Override
    public List<SkippedMessage> readAndLockSkippedMessages(int count, Timestamp lastMsgTime) {
        List<SkippedMessage> skippedMessages = new ArrayList<>();

        long maxApplicationTransactionTimeInMin =
                relayerConfiguration.getMaxApplicationTransactionTime() / (Constants.MILLISECONDS_TO_MIN_FACTOR);
        List<Long> skippedAppSequenceIds = readerOutboundRepository
                .readSkippedAppSequenceIds(maxApplicationTransactionTimeInMin, lastMsgTime);

        if (skippedAppSequenceIds == null || skippedAppSequenceIds.size() == 0)
            return skippedMessages;

        List<Message> messages = readerOutboundRepository.readMessagesUsingSequenceIds(skippedAppSequenceIds,count);
        List<Long> foundSkippedIds = new ArrayList<>();
        for (Message messageObj : messages) {
            skippedMessages.add(new SkippedMessage(messageObj, messageObj.getId()));
            foundSkippedIds.add(messageObj.getId());
        }

        //LOCK
        readerOutboundRepository.updateSkippedIdStatus(foundSkippedIds, SkippedIdStatus.PROCESSING);
        return skippedMessages;
    }

    @Override
    public AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds) {
        return readerOutboundRepository.messagesExistForFurtherOffset(currentOffset, delayedReadIntervalInSeconds);
    }

    @Override
    public List<AppMessageMetaData> readAndSubmitReRelayMessages(int count) {
        int batchSize = count;
        if(count > readerMaxParallelismDegree) {
            batchSize = count / readerMaxParallelismDegree;
        }
        // read all the ReRelay messages
        List<String> messageIds = readerOutboundRepository.getLastProcessedMessageids();
        List<Long> messageSequenceIds = readerOutboundRepository.getMessageSequenceIds(messageIds);
        List<AppMessageMetaData> messageMetaDataList = readerOutboundRepository.getMessageMetaData(messageIds);


        if (messageSequenceIds.size() == 0) {
            return null;
        }
        Long maxMessageSequenceId = messageSequenceIds.get(messageSequenceIds.size() - 1);
        Long minMessageSequenceId = messageSequenceIds.get(0);
        if (minMessageSequenceId.equals(maxMessageSequenceId)) {
            return null;
        }

        // Reading all the message between start and count until maxMessageId.
        Long start = minMessageSequenceId;
        while (start < maxMessageSequenceId) {
            List<ReRelayMessage> reRelayMessageList;
            if ((start + batchSize) <= maxMessageSequenceId) {
                reRelayMessageList = getReRelayMessages(start, batchSize);
            } else {
                reRelayMessageList = getReRelayMessages(start, (int) (maxMessageSequenceId - start));
            }
            if (reRelayMessageList.size() > 0) {
                submitReRelayMessages(reRelayMessageList);
            }
            start += batchSize;
        }
        return messageMetaDataList;
    }

    private List<ReRelayMessage> getReRelayMessages(Long start, int batchSize) {
        List<ReRelayMessage> reRelayMessages = new ArrayList<>();
        List<Message> messages = readerOutboundRepository.readMessages(start,batchSize,0);
        for(Message message : messages){
            reRelayMessages.add(new ReRelayMessage(message, message.getId()));
        }
        return reRelayMessages;
    }

}
