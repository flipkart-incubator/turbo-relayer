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

import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchema;

import java.util.List;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 28-Jul-2015
 *
 */
public interface ReaderApplicationRepository {
    List<AppMessageMetaData> getMessageMetaData(Long start, int count);

    List<AppMessageMetaData> getMessageMetaData(Long start, int count, int delayedReadIntervalInSeconds);

    Map<Long, AppMessageMetaData> getMessageMetaData(List<Long> ids, int count);

    AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds);

    List <AppMessageMetaData> getMessageMetaData(List<String> messageIds);

    AppMessageMetaData getMinMessageFromMessages(List<String> messageIds);

    DBSchema getTableDescription(String tableName);
}
