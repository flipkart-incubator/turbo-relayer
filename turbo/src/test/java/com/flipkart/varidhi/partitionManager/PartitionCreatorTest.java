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
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class PartitionCreatorTest {

    private OutboundPartitionRepository outboundPartitionRepository;
    private ApplicationPartitionRepository applicationPartitionRepository;

    @Before
    public void setup(){
        outboundPartitionRepository = mock(OutboundPartitionRepository.class);
        when(outboundPartitionRepository.getPartitionMaxId(ExchangeTableNameProvider.TableType.MESSAGE)).thenReturn(95L);
        when(outboundPartitionRepository.getLastMessageId()).thenReturn(55L);
        applicationPartitionRepository = mock(ApplicationPartitionRepository.class);

    }

    @Test
    public void test() {
        PartitionCreator partitionCreator = new PartitionCreator(5, 10, applicationPartitionRepository, outboundPartitionRepository,TurboReadMode.DEFAULT, PartitionMode.AUTO);
        partitionCreator.createPartitions();
        verify(applicationPartitionRepository).createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA, 104, 10);
        verify(outboundPartitionRepository).createNewPartition(ExchangeTableNameProvider.TableType.SKIPPED_IDS, 104, 10);
        verify(outboundPartitionRepository).createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE, 104, 10);
    }

    @Test
    public void NoCreationOfPartitionInOutboundOnlyModeTest(){

        PartitionCreator partitionCreator = new PartitionCreator(5, 10, applicationPartitionRepository, outboundPartitionRepository,TurboReadMode.DEFAULT, PartitionMode.OUTBOUND);
        partitionCreator.createPartitions();
        verify(applicationPartitionRepository, times(0)).createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA, 104, 10);
        verify(outboundPartitionRepository).createNewPartition(ExchangeTableNameProvider.TableType.SKIPPED_IDS, 104, 10);
        verify(outboundPartitionRepository).createNewPartition(ExchangeTableNameProvider.TableType.MESSAGE, 104, 10);

    }


}