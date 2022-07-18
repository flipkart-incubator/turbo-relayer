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
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.Message;
import com.flipkart.varidhi.relayer.reader.models.OutboundMessage;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


public class SequenceMessageReader extends DefaultMessageReader{

    private Logger logger;
    public SequenceMessageReader(String executorId, int readerMaxParallelismDegree, ReaderApplicationRepository readerApplicationRepository, ReaderOutboundRepository readerOutboundRepository, OutputHandler outputHandler, ThreadPoolExecutor readerExecutor, RelayerConfiguration relayerConfiguration) {
        super(executorId, readerMaxParallelismDegree, readerApplicationRepository, readerOutboundRepository, outputHandler, readerExecutor, relayerConfiguration);
        this.logger = LoggerFactory
                .getLogger(SequenceMessageReader.class.getCanonicalName() + " " + executorId);
    }


    private List<OutboundMessage> getOutboundMessages(List<Long> messageSequenceIds) {

        List<OutboundMessage> outboundMessages = new ArrayList<>();
        List<Message> messages = readerOutboundRepository.readMessagesUsingSequenceIds(messageSequenceIds);

        for(Message message : messages){
            outboundMessages.add(new OutboundMessage(message, message.getId()));
        }
        if(messageSequenceIds.size() != messages.size()){
            Set<Long> fetchedIdsSet = new HashSet<>();
            Set<Long> messageSequenceIdsSet = new HashSet<>(messageSequenceIds);
            for(Message message : messages){
                fetchedIdsSet.add(message.getId());
            }
            messageSequenceIdsSet.removeAll(fetchedIdsSet);
            logger.warn("Sequence ids not found in outbound database: " + messageSequenceIdsSet);
            messageNotInOutboundDb.updateMetric(messageSequenceIdsSet.size());
        }
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
}
