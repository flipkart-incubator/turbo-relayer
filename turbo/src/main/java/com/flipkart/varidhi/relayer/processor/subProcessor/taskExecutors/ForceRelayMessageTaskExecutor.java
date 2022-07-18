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


import com.flipkart.varidhi.relayer.common.Exceptions.RelayHttpException;
import com.flipkart.varidhi.relayer.common.Exceptions.InsufficientRelayerParametersException;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.tasks.ForceRelayMessageTask;
import com.flipkart.varidhi.utils.LoggerUtil;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ForceRelayMessageTaskExecutor extends ProcessorTaskExecutor<ForceRelayMessageTask> {


    private ProcessorOutboundRepository messageRepository;
    private final MessageRelayer httpMessageRelayer;
    private static Logger relayingMessageLogger = LoggerFactory.getLogger("relayingMessageLogger");
    private static Logger logger = LoggerFactory.getLogger(ForceRelayMessageTask.class.getCanonicalName());


    public ForceRelayMessageTaskExecutor(String executorId,
                                         ProcessorOutboundRepository messageRepository,
                                         MessageRelayer httpMessageRelayer) {
        super(executorId);
        this.messageRepository = messageRepository;
        this.httpMessageRelayer = httpMessageRelayer;
    }

    protected Boolean relayMessage(ForceRelayMessageTask relayTask) throws InsufficientRelayerParametersException,RelayHttpException, IOException {

        Response response;
        try {
            response = httpMessageRelayer.relayMessage(relayTask);
        } catch (InsufficientRelayerParametersException ex) {
            relayingMessageLogger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Relaying","failed" , relayTask.getMessageData()));
            throw ex;
        }
        catch (Exception ex) {
            relayingMessageLogger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),
                    relayTask.getGroupId(), "ForceRelaying", "failed", relayTask.getMessageData()));
            throw new RelayHttpException(500, "RELAYER_ERROR", ex.getMessage());
        }
        if (response.getStatusCode() == 0) {
            throw new RelayHttpException(response.getStatusCode(), "REQUEST_FORMATION_ERROR", "HTTP Input is not proper!");
        }
        if (response.getStatusCode() / 100 != 2) {
            throw new RelayHttpException(response.getStatusCode(), response.getStatusText(), response.getResponseBody());
        }
        relayingMessageLogger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),
                relayTask.getGroupId(), "ForceRelaying", "Finished", relayTask.getMessageData()));
        return true;
    }

    @Override
    public void execute(ForceRelayMessageTask relayTask) {
        int statusCode = 500;
        try {
            if (relayMessage(relayTask)) {
                statusCode = 200;
            }
        } catch (RelayHttpException e) {
            relayingMessageLogger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(), relayTask.getGroupId(), "ForceRelaying", "failed", relayTask.getMessageData()));
            logger.error("Got error while relaying message ID: " + relayTask.getMessageId() + " In Force Relay Task: " + e.getResponseBody());
            statusCode = e.getStatusCode();
        } catch (Exception e) {
            logger.error("Failed to publish message ID: " + relayTask.getMessageId() + " In Force Relay Task", e);
        }
        try {
            messageRepository.updateDestinationResponseStatus(relayTask.getMessageId(), statusCode);
        } catch (Exception e) {
            logger.error("Failed to update destination response status for force relay message: " + relayTask.getMessageId(), e);
        }
    }

}

