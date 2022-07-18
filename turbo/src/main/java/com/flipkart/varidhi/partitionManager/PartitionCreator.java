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

package com.flipkart.varidhi.partitionManager;

import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.repository.ApplicationPartitionRepository;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.OutboundPartitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by manmeet.singh on 07/03/16.
 */
public class PartitionCreator {
    private int noOfExtraPartitions;
    private long partitionSize;
    private ApplicationPartitionRepository applicationPartitionRepository;
    private OutboundPartitionRepository outboundPartitionRepository;
    private final PartitionMode partitionMode;
    private final TurboReadMode turboReadMode;
    private static final Logger logger = LoggerFactory.getLogger(PartitionCreator.class);

    public PartitionCreator(int noOfExtraPartitions, long partitionSize,
                            ApplicationPartitionRepository applicationPartitionRepository,
                            OutboundPartitionRepository outboundPartitionRepository,TurboReadMode turboReadMode, PartitionMode partitionMode) {
        this.noOfExtraPartitions = noOfExtraPartitions;
        this.partitionSize = partitionSize;
        this.applicationPartitionRepository = applicationPartitionRepository;
        this.outboundPartitionRepository = outboundPartitionRepository;
        this.turboReadMode = turboReadMode;
        this.partitionMode = partitionMode;
    }

    public void createPartitions() {
        long lastMessageId = outboundPartitionRepository.getLastMessageId();
        if (lastMessageId != -1L) {
            long lastPartitionEndId = outboundPartitionRepository.getPartitionMaxId(ExchangeTableNameProvider.TableType.MESSAGE) - 1;
            long lastNonEmptyPartitionEndId =
                (lastMessageId / partitionSize + 1) * partitionSize - 1;
            int noOfEmptyPartitions =
                (int) ((lastPartitionEndId - lastNonEmptyPartitionEndId) / partitionSize);
            if (noOfEmptyPartitions >= noOfExtraPartitions) {
                return;
            }
            int noOfPartitionsNeeded = noOfExtraPartitions - noOfEmptyPartitions;
            logger.info("No of partitions needed:" + (noOfPartitionsNeeded-1));
            for (int i = 1; i < noOfPartitionsNeeded; i++) {
                lastPartitionEndId = lastPartitionEndId + partitionSize;
                createNewPartition(applicationPartitionRepository, outboundPartitionRepository,
                    lastPartitionEndId, partitionSize);
            }
        }
    }

    private void createNewPartition(ApplicationPartitionRepository applicationPartitionRepository,
        OutboundPartitionRepository outboundPartitionRepository, long lastPartitionEndId,
        long partitionSize) {
        // Order of Partition creation should be Message_Metdata,skipped_ids,Messages
        // as we are always checking the Messages Table to create the number of Partitions
        // so it should be the last table on which Partitions should be created.
        if(this.turboReadMode != TurboReadMode.OUTBOUND_READER && this.partitionMode != PartitionMode.OUTBOUND)
            applicationPartitionRepository.createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA, lastPartitionEndId, partitionSize);
        outboundPartitionRepository.createNewPartition(ExchangeTableNameProvider.TableType.SKIPPED_IDS, lastPartitionEndId, partitionSize);
        outboundPartitionRepository.createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE, lastPartitionEndId, partitionSize);

    }
}
