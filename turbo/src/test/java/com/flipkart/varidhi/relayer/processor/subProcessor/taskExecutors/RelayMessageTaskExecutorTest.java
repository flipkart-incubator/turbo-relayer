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

package com.flipkart.varidhi.relayer.processor.subProcessor.taskExecutors;

import com.flipkart.varidhi.relayer.common.GroupStatus;
import com.flipkart.varidhi.relayer.common.Pair;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.varidhi.relayer.processor.tasks.RelayMessageTask;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class RelayMessageTaskExecutorTest {
    @Test
    public void testRelayMessage() throws Exception {
        ProcessorOutboundRepository processorOutboundRepository = mock(ProcessorOutboundRepository.class);
        SubProcessorRepository processorRepository = mock(SubProcessorRepository.class);
        MessageRelayer messageRelayer = mock(MessageRelayer.class,Mockito.RETURNS_DEEP_STUBS);
        RelayMessageTaskExecutor relayMessageTaskExecutor = new RelayMessageTaskExecutor("Dude", processorRepository, processorOutboundRepository, messageRelayer){
            @Override
            protected Boolean relayMessage(RelayMessageTask relayTask) throws Exception {
                return true;
            }
        };
        RelayMessageTask relayMessage = mock(RelayMessageTask.class);
        when(relayMessage.getGroupId()).thenReturn("close_friends");
        when(processorRepository.getLastSidelinedSeqId("close_friends")).thenReturn(3L);
        when(relayMessage.getTaskId()).thenReturn(1L);
        when(relayMessage.getMessageId()).thenReturn("me");
        when(processorRepository.groupStatus("close_friends")).thenReturn(GroupStatus.SIDELINED);
        when(messageRelayer.getRelayerConfiguration().isIgnoreSideliningOf4xxFailures()).thenReturn(false);
        when(messageRelayer.getRelayerConfiguration().isIgnoreSideliningOfNon4xxFailures()).thenReturn(false);
        RelayMessageTaskExecutor spy=spy(relayMessageTaskExecutor);
        spy.execute(relayMessage);
        verify(spy).relayMessage(relayMessage);
    }


    @Test
    public void testMessageSidelining() throws Exception {
        ProcessorOutboundRepository processorOutboundRepository = mock(ProcessorOutboundRepository.class);
        SubProcessorRepository processorRepository = mock(SubProcessorRepository.class);
        MessageRelayer messageRelayer = mock(MessageRelayer.class,Mockito.RETURNS_DEEP_STUBS);
        RelayMessageTaskExecutor messageTaskExecutor = new RelayMessageTaskExecutor("Dude", processorRepository, processorOutboundRepository, messageRelayer){
            @Override
            protected Boolean sidelineMessage(RelayMessageTask relayTask, SidelineReasonCode sidelineReasonCode) {
                Assert.assertEquals(SidelineReasonCode.GROUP_SIDELINED, sidelineReasonCode);
                return true;
            }
        };
        RelayMessageTask relayMessage = mock(RelayMessageTask.class);
        when(relayMessage.getGroupId()).thenReturn("close_friends");
        when(processorRepository.getLastSidelinedSeqId("close_friends")).thenReturn(3L);
        when(relayMessage.getTaskId()).thenReturn(5L);
        when(relayMessage.getMessageId()).thenReturn("me");
        when(processorRepository.groupStatus("close_friends")).thenReturn(GroupStatus.SIDELINED);
        when(messageRelayer.getRelayerConfiguration().isIgnoreSideliningOf4xxFailures()).thenReturn(false);
        when(messageRelayer.getRelayerConfiguration().isIgnoreSideliningOfNon4xxFailures()).thenReturn(false);
        RelayMessageTaskExecutor spy=spy(messageTaskExecutor);
        spy.execute(relayMessage);
        verify(spy).sidelineMessage(relayMessage, SidelineReasonCode.GROUP_SIDELINED);
        verify(processorRepository).updateLastProccesedMessageId(Pair.of((Long)relayMessage.getTaskId(), relayMessage.getMessageId()));
    }

}

