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
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.repository.ApplicationPartitionRepository;
import com.flipkart.varidhi.repository.OutboundPartitionRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;

import static org.mockito.Mockito.*;

//import static org.junit.jupiter.api.Assertions.*;

public class PartitionArchiverTest {


    private static long partitionSize = 10;
    private ApplicationPartitionRepository applicationPartitionRepository;
    private OutboundPartitionRepository outboundPartitionRepository;
    private PartitionMode partitionMode;
    private TurboReadMode turboReadMode;

    private PartitionArchiver partitionArchiver;

    @Before
    public void setUp() {

        applicationPartitionRepository = mock(ApplicationPartitionRepository.class);
        outboundPartitionRepository = mock(OutboundPartitionRepository.class);

        Partition samplePartition = getSamplePartition();

        when(outboundPartitionRepository.hasPendingMessages(samplePartition.getStartId(),samplePartition.getEndId())).thenReturn(false);
        when(outboundPartitionRepository.hasSidelinedMessages(samplePartition.getStartId(),samplePartition.getEndId())).thenReturn(false);


        when(outboundPartitionRepository.getSkippedIdsPartitionList(samplePartition.getStartId(), samplePartition.getEndId())).thenReturn(new ArrayList<Partition>());
        when(applicationPartitionRepository.getMetadataPartitionList(samplePartition.getStartId(), samplePartition.getEndId())).thenReturn(new ArrayList<Partition>());

        when(outboundPartitionRepository.getLastMessageId()).thenReturn(1_00_002L);
        //below mocks are done to avoid looping of this test
        when(outboundPartitionRepository.getMessagesPartition(1_00_001)).thenReturn(getNextSamplePartition());
        when(outboundPartitionRepository.lastCreatedDateInPartition(1_00_001, 2_00_000)).thenReturn(get1DayBefore());


        doNothing().when(outboundPartitionRepository).dropPartition(any());
        doNothing().when(applicationPartitionRepository).dropPartition(any());

    }


    @Test
    public void archiveAndDropShardTestForOneHour() {

        Partition samplePartition = getSamplePartition();

        // test of 1 hour
        partitionArchiver = new PartitionArchiver(1, partitionSize, applicationPartitionRepository, outboundPartitionRepository, PartitionMode.AUTO, TurboReadMode.DEFAULT);

        when(outboundPartitionRepository.getMessagesPartition(1L)).thenReturn(samplePartition);
        when(outboundPartitionRepository.lastCreatedDateInPartition(samplePartition.getStartId(), samplePartition.getEndId()))
                .thenReturn(get1HourBefore());

        partitionArchiver.archiveAndDropShard();

        verify(outboundPartitionRepository, times(1)).dropPartition(samplePartition);

    }

    @Test
    public void archiveAndDropShardTestForOneDay() {

        Partition samplePartition = getSamplePartition();

        //assert for 1-Day before test
        partitionArchiver = new PartitionArchiver(24, partitionSize, applicationPartitionRepository, outboundPartitionRepository, PartitionMode.AUTO, TurboReadMode.DEFAULT);

        when(outboundPartitionRepository.getMessagesPartition(1L)).thenReturn(samplePartition);
        when(outboundPartitionRepository.lastCreatedDateInPartition(samplePartition.getStartId(), samplePartition.getEndId()))
                .thenReturn(get1DayBefore());

        partitionArchiver.archiveAndDropShard();

        verify(outboundPartitionRepository, times(1)).dropPartition(samplePartition);

    }

    @Test
    public void NoPartitioningOfAppDbInOutboundOnlyModeTest(){

        Partition samplePartition = getSamplePartition();

        //partition Mode set to OUTBOUND
        partitionArchiver = new PartitionArchiver(24, partitionSize, applicationPartitionRepository, outboundPartitionRepository, PartitionMode.OUTBOUND, TurboReadMode.DEFAULT);

        when(outboundPartitionRepository.getMessagesPartition(1L)).thenReturn(samplePartition);
        when(outboundPartitionRepository.lastCreatedDateInPartition(samplePartition.getStartId(), samplePartition.getEndId()))
                .thenReturn(get1DayBefore());
        partitionArchiver.archiveAndDropShard();

        verify(applicationPartitionRepository, times(0)).dropPartition(samplePartition);

    }

    private Date get1HourBefore(){
        Date now = new Date();
        return  new Date(now.getYear(), now.getMonth(), now.getDate(), now.getHours()-1, now.getMinutes());
    }

    private Date get1DayBefore(){
        Date now = new Date();
        return  new Date(now.getYear(), now.getMonth(), now.getDate()-1, now.getHours(), now.getMinutes());
    }

    private Date getLastCreationDate(){
        return new Date(2021, 11, 23, 17, 30);
    }

    private Partition getSamplePartition(){
        return new Partition(1, 1_00_000, "p1");
    }
    private Partition getNextSamplePartition(){
        return new Partition(1_00_001, 2_00_000, "p2");
    }

}