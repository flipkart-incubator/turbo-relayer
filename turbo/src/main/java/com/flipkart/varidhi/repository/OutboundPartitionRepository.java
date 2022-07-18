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

package com.flipkart.varidhi.repository;

import com.flipkart.varidhi.core.Partition;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by manmeet.singh on 23/02/16.
 */
public interface OutboundPartitionRepository {

    Long getLastMessageId();

    void createNewPartition(ExchangeTableNameProvider.TableType tableName, long endId, long partitionSize);

    boolean hasPendingMessages(long startId, long endId);

    boolean hasSidelinedMessages(long startId, long endId);

    void backupPartition(Partition messagesPartition);

    Date lastCreatedDateInPartition(long startId, long endId);

    long getPartitionMaxId(ExchangeTableNameProvider.TableType tableType);

    void dropPartition(Partition messagesPartition);

    void backupSkippedIdsPartition(Partition partition);

    void dropSkippedIdsPartition(Partition partition);

    Partition getMessagesPartition(long partitionStartId);

    void setTrPartitionQueriesLogger(TRPartitionQueriesLogger trPartitionQueriesLogger, boolean shouldOnlyLogQueries);

    ArrayList<Partition> getSkippedIdsPartitionList(long partitionStartId, long partitionEndId);
}
