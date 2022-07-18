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

import com.flipkart.varidhi.relayer.reader.models.*;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public interface MessageReader {

    void addToSkipMessagesInBatches(long start, long nextId, int batchSize);

    void addToSkipMessages(long start, long nextId);

    List<ControlTask> readAndLockControlTasks(int count);

    List<UnsidelinedMessage> readAndLockUnsidelineMessages(int count);

    List<SkippedMessage> readAndLockSkippedMessages(int count, Timestamp lastMsgTime);

    List<OutboundMessage> getOutboundMessagesInParallel(Long start, int count, int delayedReadIntervalInSeconds);

    AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds);

    List<AppMessageMetaData> readAndSubmitReRelayMessages(int count);
}
