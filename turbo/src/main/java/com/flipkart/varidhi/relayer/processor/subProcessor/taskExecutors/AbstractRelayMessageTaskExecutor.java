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
import com.flipkart.varidhi.relayer.common.GroupStatus;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.processor.subProcessor.helper.MessageRelayer;
import com.flipkart.varidhi.relayer.processor.subProcessor.repository.SubProcessorRepository;
import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import com.flipkart.varidhi.utils.LoggerUtil;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;



/*
 * *
 * Author: abhinavp
 * Date: 03-Aug-2015
 *
 */
public abstract class AbstractRelayMessageTaskExecutor<T extends BaseRelayMessageTask>
    extends ProcessorTaskExecutor<T> {
    public static final int maxSidelineRetry = 5;
    public static final int maxRelayRetry = 5;
    SubProcessorRepository processorRepository;
    ProcessorOutboundRepository messageRepository;
    private final MessageRelayer httpMessageRelayer;
    private static Logger logger = LoggerFactory.getLogger(AbstractRelayMessageTaskExecutor.class.getCanonicalName());
    private static Logger relayingMessageLogger = LoggerFactory.getLogger("relayingMessageLogger");

    protected AbstractRelayMessageTaskExecutor(String executorId,
        SubProcessorRepository processorRepository, ProcessorOutboundRepository messageRepository, MessageRelayer httpMessageRelayer) {
        super(executorId);
        this.processorRepository = processorRepository;
        this.messageRepository = messageRepository;
        this.httpMessageRelayer = httpMessageRelayer;
    }

    // Depending on the group status this function puts other messages of same group to Sideline table.
    protected Boolean putMessageInSidelineQueue(T relayTask, SidelineReasonCode sidelineReasonCode,
        SidelinedMessageStatus status, GroupStatus groupStatus) {
        messageRepository
            .insertOrupdateSidelinedMessageStatus(relayTask.getMessageId(), relayTask.getGroupId(),
                sidelineReasonCode, status, null);
        processorRepository
            .updateLastSidelinedSeqId(relayTask.getGroupId(), (Long) relayTask.getTaskId(),
                groupStatus);
        //System.out.println("Putting messageData in sideline queue :"+relayTask.getMessageId());
        return true;
    }

    protected Boolean relayMessage(T relayTask) throws Exception {

        relayingMessageLogger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                ,"Relaying", "started" , relayTask.getMessageData()));
        Response response;
        try {
            response = httpMessageRelayer.relayMessage(relayTask);

            //getting the create date and time of the message to be relayed
            Timestamp createDateTime = relayTask.getCreateDateTime();
            if (createDateTime != null) {
                long createDateTimeInMills = createDateTime.getTime();
                Date currentDateTime = new Date();
                long currentTimeInMills = currentDateTime.getTime();
                //Calculating the total lag from the time message was created till the current time
                long lagTimeInMills = currentTimeInMills - createDateTimeInMills;
                //Saving the contents to subprocessor Repository after the message has been sent and there is no error in the response
                processorRepository.setMessageLagTime(lagTimeInMills);
            }

        } catch (InsufficientRelayerParametersException ex) {
            relayingMessageLogger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Relaying","failed" , relayTask.getMessageData()));
            throw ex;
        }
        catch (Exception ex) {
            relayingMessageLogger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Relaying","failed" , relayTask.getMessageData()));
            throw new RelayHttpException(500, "RELAYER_ERROR",
                    ex.getMessage());
        }
        if (response.getStatusCode() / 100 != 2) {
            relayingMessageLogger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Relaying","failed" , relayTask.getMessageData()));
            if (response.getStatusCode() == 0) {
                throw new RelayHttpException(response.getStatusCode(), "REQUEST_FORMATION_ERROR",
                    "HTTP Input is not proper!");
            } else
                throw new RelayHttpException(response.getStatusCode(), response.getStatusText(),
                    response.getResponseBody());
        }
        relayingMessageLogger.info(LoggerUtil.generateLogInLogSvcFormat(
                relayTask.getMessageId() , relayTask.getGroupId()
                ,"Relaying","Finished" , relayTask.getMessageData()));
        return true;
    }


    protected Boolean sidelineMessage(T relayTask, SidelineReasonCode sidelineReasonCode,
        int httpStatusCode, String details, int retries) {
        int retryCount = 0;
        int waitTime = 100;
        relayingMessageLogger.info(LoggerUtil.generateLogInLogSvcFormat(
                relayTask.getMessageId() , relayTask.getGroupId()
                ,"Sidelining","started" , relayTask.getMessageData()));


        while ((retryCount++) <= maxSidelineRetry && !messageRepository
            .sidelineMessage(relayTask.getMessageId(), relayTask.getGroupId(), sidelineReasonCode,
                httpStatusCode, details, retries)) {
            try {
                logger.warn("Unable to access the 'messageRepository' retrying: " + (maxRelayRetry - retryCount + 1) + " times" );
                //using this metric to see in grafna weather we able to access the database or not.
                processorRepository.setSidelineDbFailure(true);
                Thread.sleep(waitTime * retryCount);
            } catch (InterruptedException e) {
                logger.error("sidelineMessage:Error in Thread Sleep:" + e.getMessage(), e);
            }
        }

        if (retryCount > 5) {
            relayingMessageLogger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId(),
                    "Failed Sidelining 5 times","Stopping Relayer",
                    "Failed to Sideline message.Hence, Stopping Relayer :: System.exit"));
            System.exit(-1);
        } else {

            relayingMessageLogger.info(LoggerUtil.generateLogInLogSvcFormat(
                    relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Sidelining","Finished" , relayTask.getMessageData()));

            //if no failure then update the metric as 0.
            processorRepository.setSidelineDbFailure(false);
            logger.info("Successfully sidelined message :" + relayTask.getMessageId());
        }

        // Whenever there is a genuine message sideline, last message creates the boundary.
        // Even if id is lesser than earlier one upcoming IDs should be sidelined.

        if (relayTask.getGroupId() != null)
            processorRepository
                .updateLastSidelinedSeqId(relayTask.getGroupId(), (Long) relayTask.getTaskId(),
                    GroupStatus.SIDELINED);
        return true;
    }

    /*
    protected Boolean sidelineMessage(T relayTask, SidelineReasonCode sidelineReasonCode,
        String details, int retries) {
        return sidelineMessage(relayTask, sidelineReasonCode, 0, details, retries);
    }
    */

    protected Boolean sidelineMessage(T relayTask, SidelineReasonCode sidelineReasonCode) {
        return sidelineMessage(relayTask, sidelineReasonCode, 0, "", 0);
    }

    /*
    protected Boolean sidelineMessage(T relayTask, SidelineReasonCode sidelineReasonCode,
        String details) {
        return sidelineMessage(relayTask, sidelineReasonCode, 0, details, 0);
    }
    */


    /* Below functions are mainly responsible for Relay Tasks */

    abstract void relayUngroupedMessage(T relayTask) throws Exception;

    abstract void relayGroupedMessage(T relayTask) throws Exception;

    abstract void putMessageInSidelinedQueueWithGroupStatus(T relayTask);

    abstract void relayLastUndsidelinedMessageForGroup(T relayTask) throws Exception;

    abstract void sidelineMessageOnRelayFailure(T relayTask, SidelineReasonCode sidelineReasonCode,
        int statusCode, String message, int retries);

    @Override public void execute(T relayTask) {
        String groupId = relayTask.getGroupId();
        boolean sidelineIgnore4XX = this.httpMessageRelayer.getRelayerConfiguration().isIgnoreSideliningOf4xxFailures();
        boolean sidelineIgnore = this.httpMessageRelayer.getRelayerConfiguration().isIgnoreSideliningOfNon4xxFailures();

        try {
            if (groupId == null || groupId.trim().isEmpty()) // Sequencing not required
            {

                logger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Relaying", "starting" , relayTask.getMessageData()));
                relayUngroupedMessage(relayTask);
                logger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Relaying", "Successful" , relayTask.getMessageData()));
            } else  //Sequencing required.
            {
                Long lastSidelineSeqId =
                    processorRepository.getLastSidelinedSeqId(groupId) == null ?
                        -1 :
                        processorRepository.getLastSidelinedSeqId(groupId);
                Long currentTaskId = (Long) relayTask.getTaskId();
                GroupStatus status = processorRepository.groupStatus(groupId);
                logger.info("Comparing last sideline message - GroupId : " + groupId + " TaskId : "
                    + currentTaskId + ", LastSidelineSeqId : " + lastSidelineSeqId);

                if (status == null || currentTaskId < lastSidelineSeqId) {
                    logger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Relaying", "Starting" , relayTask.getMessageData()));
                    relayGroupedMessage(relayTask);
                    logger.info(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Relaying", "Successful" , relayTask.getMessageData()));
                } else if (currentTaskId > lastSidelineSeqId) {
                    logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Sidelining", "Starting" , relayTask.getMessageData()));
                    putMessageInSidelinedQueueWithGroupStatus(relayTask);
                    logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Sidelining", "Successful" , relayTask.getMessageData()));;

                } else // Last sidelined is same as current seq Id . This case should never happen.
                {
                    logger.warn("Relaying last message of group :" + relayTask.getGroupId()
                        + " from sideline table.");
                    relayLastUndsidelinedMessageForGroup(relayTask);
                    logger.warn("Relay successful last message of group :" + relayTask.getGroupId()
                        + " from sideline table.");
                }
            }

        } catch (RelayHttpException e) {
            logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                    ,"Sidelining", "Starting" , relayTask.getMessageData()));
            try {
                if(e.getStatusCode() / 100 == 4 && sidelineIgnore4XX){
                    logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Sidelining", "ignored" , relayTask.getMessageData()));
                    return;
                }
                if(e.getStatusCode() / 100 != 4 && sidelineIgnore ){
                    logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Sidelining", "ignored" , relayTask.getMessageData()));
                    return;
                }
                sidelineMessageOnRelayFailure(relayTask, SidelineReasonCode.HTTP_ERROR,
                    e.getStatusCode(), e.getResponseBody(), maxRelayRetry);
                logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Sidelining", "Sucessful" , relayTask.getMessageData()));
            } catch (Exception e2) {
                //HardStop of relayer.
                logger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Sidelining", "Failed" , relayTask.getMessageData()));
            }
        } catch (InsufficientRelayerParametersException ex) {
            logger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId() , relayTask.getGroupId()
                    ,"Relayer","stopped" , relayTask.getMessageData()));
            logger.error("Stopping relayer :: System.exit ",ex);
            System.exit(-1);

        } catch (Exception e) {
            logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                    ,"Sidelining", "Starting" , relayTask.getMessageData()));
            try {
                if(sidelineIgnore){
                    logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                            ,"Sidelining", "ignored" , relayTask.getMessageData()));
                    return;
                }
                sidelineMessageOnRelayFailure(relayTask, SidelineReasonCode.RELAYER_ERROR, 0,
                    e.getMessage(), 0);
                logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Sidelining", "Successful" , relayTask.getMessageData()));
            } catch (Exception e2) {
                //Hard Stop of relayer.
                logger.error(LoggerUtil.generateLogInLogSvcFormat(relayTask.getMessageId(),relayTask.getGroupId()
                        ,"Sidelining", "Failed" , relayTask.getMessageData()));
            }
        }

    }
}
