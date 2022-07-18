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

package com.flipkart.varidhi.relayer.reader.repository;

import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.ControlTask;
import com.flipkart.varidhi.relayer.reader.models.Message;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchema;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaCharset;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 05-Jul-2015
 *
 */
public interface ReaderOutboundRepository {
    Map<String, Message> readMessages(List<String> messageIds);

    List<Message> readMessagesUsingSequenceIds(List<Long> messageSequenceIds);

    List<Message> readMessagesUsingSequenceIds(List<Long> messageSequenceIds, int count);

    List<Message> readMessages(Long start, int batchSize, int delayedReadIntervalInSeconds);

    AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds);

    List<AppMessageMetaData> getMessageMetaData(List<String> messageIds);

    List<AppMessageMetaData> getMessageMetaData(Long start, int count, int delayedReadIntervalInSeconds);

    HashMap<String ,Long > processorsIdMap(HashMap <String,String> processorsMsgIdMap);

    AppMessageMetaData getMinMessageFromMessages(List<String> messageIds);

    List<String> readUnsidelinedMessageIds(int count);

    void updateSidelinedMessageStatus(List<String> messageIds, SidelinedMessageStatus status);

    List<ControlTask> readControlTasks(int count);

    void updateControlTaskStatus(List<ControlTask> tasks, ControlTaskStatus status);

    void persistSkippedIds(List<Long> skippedIds);

    List<Long> readSkippedAppSequenceIds(long maxApplicationTransactionTime, Timestamp lastMsgTime);

    void updateSkippedIdStatus(List<Long> ids, SkippedIdStatus status);

    List <String> getLastProcessedMessageids();

    List<Long> getMessageSequenceIds(List<String> messageIds);

    int getNumberOfProccessors();

    DBSchema getTableDescription(String tableName);

    List<DBSchemaCharset> getTablesCharSet(List<String> tableNames);
}
