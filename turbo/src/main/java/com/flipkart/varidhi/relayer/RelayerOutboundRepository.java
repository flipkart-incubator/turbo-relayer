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

import com.flipkart.varidhi.relayer.processor.ProcessorMetadataRepository;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;

import java.util.Date;
import java.util.List;

/*
 * *
 * Author: abhinavp
 * Date: 04-Aug-2015
 *
 */
public interface RelayerOutboundRepository
    extends ReaderOutboundRepository, ProcessorOutboundRepository, ProcessorMetadataRepository, RelayerMetricRepository{
    Boolean createUnsidelineGroupTask(String groupId);

    Boolean createUnsidelineMessageTask(String messageId);

    Boolean createUnsidelineMessagesBetweenDatesTask(Date fromDate, Date toDate);

    Boolean createUnsidelineAllUngroupedMessageTask(Date fromDate, Date toDate);

    void resetProcessingStateSkippedIds();

    void resetProcessingStateControlTasks();

    void resetProcessingStateSidelinedMessages();

    List<String> getSidelinedGroups();

    Boolean createPartitionManagementTask(String relayerId);

    String getLeaderRelayerUUID(String relayerId);
}
