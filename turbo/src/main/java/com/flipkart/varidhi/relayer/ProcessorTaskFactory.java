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

import com.flipkart.turbo.tasks.ProcessorTask;
import com.flipkart.varidhi.jobs.PartitionManagementJob;
import com.flipkart.varidhi.relayer.common.ControlTaskType;
import com.flipkart.varidhi.relayer.processor.tasks.*;
import com.flipkart.varidhi.relayer.reader.models.*;
import com.flipkart.varidhi.relayer.reader.outputs.OutputTaskFactory;

/*
 * *
 * Author: abhinavp
 * Date: 04-Jul-2015
 *
 */
public class ProcessorTaskFactory implements OutputTaskFactory<ProcessorTask> {
    String varidhiServer;
    private PartitionManagementJob partitionManagementJob;

    public ProcessorTaskFactory(String varidhiServer, PartitionManagementJob partitionManagementJob) {
        this.varidhiServer = varidhiServer;
        this.partitionManagementJob = partitionManagementJob;
    }

    public ProcessorTask getTask(BaseReadDomain baseReadDomain) {
        if (baseReadDomain instanceof OutboundMessage)
            return new RelayMessageTask(baseReadDomain.getId(),
                    ((OutboundMessage) baseReadDomain).getGroupId(),
                    ((OutboundMessage) baseReadDomain).getMessageId(),
                    ((OutboundMessage) baseReadDomain).getMessageData(),
                    ((OutboundMessage) baseReadDomain).getExchangeName(),
                    ((OutboundMessage) baseReadDomain).getExchangeType(),
                    ((OutboundMessage) baseReadDomain).getHttpMethod(),
                    ((OutboundMessage) baseReadDomain).getHttpUri(),
                    ((OutboundMessage) baseReadDomain).getCustomHeaders(),
                    ((OutboundMessage) baseReadDomain).getReplyTo(),
                    ((OutboundMessage) baseReadDomain).getReplyToHttpMethod(),
                    ((OutboundMessage) baseReadDomain).getReplyToHttpUri(),
                    ((OutboundMessage) baseReadDomain).getTrasactionId(),
                    ((OutboundMessage) baseReadDomain).getCorrelationId(),
                    ((OutboundMessage) baseReadDomain).getDestinationResponseStatus(),
                    ((OutboundMessage) baseReadDomain).getCreateDateTime(),
                    this.varidhiServer
            );

        else if (baseReadDomain instanceof UnsidelinedMessage)
            return new RelayUnsidelinedMessageTask(baseReadDomain.getId(),
                    ((UnsidelinedMessage) baseReadDomain).getGroupId(),
                    ((UnsidelinedMessage) baseReadDomain).getMessageId(),
                    ((UnsidelinedMessage) baseReadDomain).getMessageData(),
                    ((UnsidelinedMessage) baseReadDomain).getExchangeName(),
                    ((UnsidelinedMessage) baseReadDomain).getExchangeType(),
                    ((UnsidelinedMessage) baseReadDomain).getHttpMethod(),
                    ((UnsidelinedMessage) baseReadDomain).getHttpUri(),
                    ((UnsidelinedMessage) baseReadDomain).getCustomHeaders(),
                    ((UnsidelinedMessage) baseReadDomain).getReplyTo(),
                    ((UnsidelinedMessage) baseReadDomain).getReplyToHttpMethod(),
                    ((UnsidelinedMessage) baseReadDomain).getReplyToHttpUri(),
                    ((UnsidelinedMessage) baseReadDomain).getTrasactionId(),
                    ((UnsidelinedMessage) baseReadDomain).getCorrelationId(),
                    ((UnsidelinedMessage) baseReadDomain).getDestinationResponseStatus(),
                    ((UnsidelinedMessage) baseReadDomain).getCreateDateTime(),
                    this.varidhiServer
            );

        else if (baseReadDomain instanceof SkippedMessage)
            return new RelaySkippedMessageTask(baseReadDomain.getId(),
                    ((SkippedMessage) baseReadDomain).getAppSequenceId(),
                    ((SkippedMessage) baseReadDomain).getGroupId(),
                    ((SkippedMessage) baseReadDomain).getMessageId(),
                    ((SkippedMessage) baseReadDomain).getMessageData(),
                    ((SkippedMessage) baseReadDomain).getExchangeName(),
                    ((SkippedMessage) baseReadDomain).getExchangeType(),
                    ((SkippedMessage) baseReadDomain).getHttpMethod(),
                    ((SkippedMessage) baseReadDomain).getHttpUri(),
                    ((SkippedMessage) baseReadDomain).getCustomHeaders(),
                    ((SkippedMessage) baseReadDomain).getReplyTo(),
                    ((SkippedMessage) baseReadDomain).getReplyToHttpMethod(),
                    ((SkippedMessage) baseReadDomain).getReplyToHttpUri(),
                    ((SkippedMessage) baseReadDomain).getTrasactionId(),
                    ((SkippedMessage) baseReadDomain).getCorrelationId(),
                    ((SkippedMessage) baseReadDomain).getDestinationResponseStatus(),
                    ((SkippedMessage) baseReadDomain).getCreateDateTime(),
                    this.varidhiServer
                    );
        else if (baseReadDomain instanceof ControlTask) {
            if (((ControlTask) baseReadDomain).getTaskType() == ControlTaskType.UNSIDELINE_GROUP) {
                return new UnsidelineGroupTask(baseReadDomain.getId(),
                    ((ControlTask) baseReadDomain).getGroupId());
            } else if (((ControlTask) baseReadDomain).getTaskType() == ControlTaskType.UNSIDELINE_MESSAGE) {
                return new UnsidelineMessageTask(baseReadDomain.getId(),
                    ((ControlTask) baseReadDomain).getMessageId());
            } else if (((ControlTask) baseReadDomain).getTaskType() == ControlTaskType.UNSIDELINE_ALL_UNGROUPED) {
                return new UnsidelineUngroupedForPeriodTask(baseReadDomain.getId(),
                        ((ControlTask) baseReadDomain).getFromDate(),
                        ((ControlTask) baseReadDomain).getToDate());
            } else if (((ControlTask) baseReadDomain).getTaskType() == ControlTaskType.MANAGE_PARTITION) {
                return new PartitionManagementTask(baseReadDomain.getId(),
                        ((ControlTask) baseReadDomain).getGroupId(),
                        partitionManagementJob);
            } else {
                return null;
            }
        }
        else if (baseReadDomain instanceof ReRelayMessage){
            return new ReRelayMessageTask(baseReadDomain.getId(),
                    ((ReRelayMessage) baseReadDomain).getAppSequenceId(),
                    ((ReRelayMessage) baseReadDomain).getGroupId(),
                    ((ReRelayMessage) baseReadDomain).getMessageId(),
                    ((ReRelayMessage) baseReadDomain).getMessageData(),
                    ((ReRelayMessage) baseReadDomain).getExchangeName(),
                    ((ReRelayMessage) baseReadDomain).getExchangeType(),
                    ((ReRelayMessage) baseReadDomain).getHttpMethod(),
                    ((ReRelayMessage) baseReadDomain).getHttpUri(),
                    ((ReRelayMessage) baseReadDomain).getCustomHeaders(),
                    ((ReRelayMessage) baseReadDomain).getReplyTo(),
                    ((ReRelayMessage) baseReadDomain).getReplyToHttpMethod(),
                    ((ReRelayMessage) baseReadDomain).getReplyToHttpUri(),
                    ((ReRelayMessage) baseReadDomain).getTrasactionId(),
                    ((ReRelayMessage) baseReadDomain).getCorrelationId(),
                    ((ReRelayMessage) baseReadDomain).getDestinationResponseStatus(),
                    ((ReRelayMessage) baseReadDomain).getCreateDateTime(),
                    this.varidhiServer);
        }
        else if (baseReadDomain instanceof ForcedRelayMessage){
            return new ForceRelayMessageTask(baseReadDomain.getId(),
                    ((ForcedRelayMessage) baseReadDomain).getGroupId(),
                    ((ForcedRelayMessage) baseReadDomain).getMessageId(),
                    ((ForcedRelayMessage) baseReadDomain).getMessageData(),
                    ((ForcedRelayMessage) baseReadDomain).getExchangeName(),
                    ((ForcedRelayMessage) baseReadDomain).getExchangeType(),
                    ((ForcedRelayMessage) baseReadDomain).getHttpMethod(),
                    ((ForcedRelayMessage) baseReadDomain).getHttpUri(),
                    ((ForcedRelayMessage) baseReadDomain).getCustomHeaders(),
                    ((ForcedRelayMessage) baseReadDomain).getReplyTo(),
                    ((ForcedRelayMessage) baseReadDomain).getReplyToHttpMethod(),
                    ((ForcedRelayMessage) baseReadDomain).getReplyToHttpUri(),
                    ((ForcedRelayMessage) baseReadDomain).getTrasactionId(),
                    ((ForcedRelayMessage) baseReadDomain).getCorrelationId(),
                    ((ForcedRelayMessage) baseReadDomain).getDestinationResponseStatus(),
                    ((ForcedRelayMessage) baseReadDomain).getCreateDateTime(),
                    this.varidhiServer);
        }
        else
            return null;
    }

}
