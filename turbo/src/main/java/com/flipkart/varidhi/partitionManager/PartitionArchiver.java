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
import com.flipkart.varidhi.core.Partition;
import com.flipkart.varidhi.core.utils.DateHelper;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.repository.ApplicationPartitionRepository;
import com.flipkart.varidhi.repository.OutboundPartitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by manmeet.singh on 23/02/16.
 */
public class PartitionArchiver implements Archiver {
    private static final Logger logger = LoggerFactory.getLogger(PartitionArchiver.class);
    private int noOfHoursToPreserve;
    private long partitionSize;
    private ApplicationPartitionRepository applicationPartitionRepository;
    private OutboundPartitionRepository outboundPartitionRepository;
    private PartitionMode partitionMode;
    private final TurboReadMode turboReadMode;

    public PartitionArchiver(int noOfHoursToPreserve, long partitionSize,
                             ApplicationPartitionRepository applicationPartitionRepository,
                             OutboundPartitionRepository outboundPartitionRepository, PartitionMode partitionMode,
                             TurboReadMode turboReadMode) {
        this.noOfHoursToPreserve = noOfHoursToPreserve;
        this.partitionSize = partitionSize;
        this.applicationPartitionRepository = applicationPartitionRepository;
        this.outboundPartitionRepository = outboundPartitionRepository;
        this.partitionMode = partitionMode;
        this.turboReadMode = turboReadMode;
    }

    @Override public void archiveAndDropShard() {
        Partition messagesPartition = outboundPartitionRepository.getMessagesPartition(1L);
        if (null != messagesPartition) {
            Date lastCreationDate = outboundPartitionRepository
                .lastCreatedDateInPartition(messagesPartition.getStartId(),
                    messagesPartition.getEndId());
            long lastMessageId = outboundPartitionRepository.getLastMessageId();
            if (lastMessageId != -1L) {
                while ((DateHelper.difference(new Date(), lastCreationDate, TimeUnit.HOURS) >= (noOfHoursToPreserve))
                        && (messagesPartition.getEndId() < lastMessageId)) {
                    if (canArchiveParition(messagesPartition.getStartId(),
                        messagesPartition.getEndId(), outboundPartitionRepository)) {
                        archiveSkippedIds(messagesPartition);
                        if(this.turboReadMode != TurboReadMode.OUTBOUND_READER && this.partitionMode!=PartitionMode.OUTBOUND)
                            archiveMetaData(messagesPartition);
                        archiveMessages(messagesPartition);
                    }
                    messagesPartition = outboundPartitionRepository
                        .getMessagesPartition(messagesPartition.getEndId() + 1);
                    lastCreationDate = outboundPartitionRepository
                        .lastCreatedDateInPartition(messagesPartition.getStartId(),
                            messagesPartition.getEndId());
                }
            }
        } else {
            logger.info("No Partitions to Archive from the Messages Table");
        }
    }

    private void archiveSkippedIds(Partition messagesPartition) {
        ArrayList<Partition> skippedIdsPartitionList = outboundPartitionRepository
            .getSkippedIdsPartitionList(messagesPartition.getStartId(),
                messagesPartition.getEndId());
        for (Partition skippedIdsPartition : skippedIdsPartitionList) {
            boolean validatePartition =
                validateAgainstMessagesPartition(messagesPartition, skippedIdsPartition);
            if (validatePartition) {
                //outboundPartitionRepository.backupSkippedIdsPartition(skippedIdsPartition);
                outboundPartitionRepository.dropSkippedIdsPartition(skippedIdsPartition);
            }
        }
    }

    private void archiveMetaData(Partition messagesPartition) {
        if (partitionMode == PartitionMode.AUTO) {
            ArrayList<Partition> applicationPartitionList = applicationPartitionRepository
                    .getMetadataPartitionList(messagesPartition.getStartId(), messagesPartition.getEndId());
            for (Partition applicationPartition : applicationPartitionList) {
                boolean validatePartition =
                        validateAgainstMessagesPartition(messagesPartition, applicationPartition);
                if (validatePartition) {
                    //applicationPartitionRepository.backupPartition(applicationPartition);
                    applicationPartitionRepository.dropPartition(applicationPartition);
                }
            }

        } else if (partitionMode == PartitionMode.OBSERVER) {
            applicationPartitionRepository.dropPartition(messagesPartition);
        }else{
            logger.error("Mode unknown for Archival");
        }
    }

    private void archiveMessages(Partition messagesPartition) {
        //outboundPartitionRepository.backupPartition(messagesPartition);
        outboundPartitionRepository.dropPartition(messagesPartition);
    }

    private boolean validateAgainstMessagesPartition(Partition messagesPartition,
        Partition thisPartition) {
        return (thisPartition.getEndId() <= messagesPartition.getEndId());
    }

    private boolean canArchiveParition(long partitionStartId, long partitionEndId,
        OutboundPartitionRepository outboundPartitionRepository) {
        return !(outboundPartitionRepository.hasPendingMessages(partitionStartId, partitionEndId)
            || outboundPartitionRepository.hasSidelinedMessages(partitionStartId, partitionEndId));
    }
}
