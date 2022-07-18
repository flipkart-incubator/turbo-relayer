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

import com.flipkart.varidhi.core.utils.DateHelper;
import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.common.ControlTaskType;
import com.flipkart.varidhi.relayer.common.ProcessedMessageLagTime;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.ControlTask;
import com.flipkart.varidhi.relayer.reader.models.Message;
import com.flipkart.varidhi.relayer.schemavalidator.models.ColumnDetails;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchema;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaCharset;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider.TableType;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.hibernate.*;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;

/*
 * *
 * Author: abhinavp
 * Date: 29-Jul-2015
 *
 */
public class OutboundRepositoryImpl implements OutboundRepository {
    private static final Logger logger = LoggerFactory.getLogger(OutboundRepositoryImpl.class);
    SessionFactory outboundSessionFactory;
    ExchangeTableNameProvider exchangeTableNameProvider;

    public OutboundRepositoryImpl(SessionFactory outboundSessionFactory,
        ExchangeTableNameProvider exchangeTableNameProvider) {
        this.outboundSessionFactory = outboundSessionFactory;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
    }

    /* ReaderOutboundRepository functions start */


    private Message convertMessageDbObject(Object[] object){
        /*
            Order of entries in object
         * 0  -> Message Sequence Id
         * 1  -> Group Id
         * 2  -> Unique Message Id
         * 3  -> Message Content
         * 4  -> exchange name
         * 5  -> exchange type
         * 6  -> sending application id
         * 7  -> http method (PUT/POST)
         * 8  -> http uri
         * 9  -> custom headers
         * 10 -> replyTo on successful response
         * 11 -> replyTo http method
         * 12 -> replyTo http uri
         * 13 -> transaction id
         * 14 -> correlation id
         * 15 -> expected response status
         * 16 -> creation date time
         */

        Message message = new Message();
        message.setId(Long.parseLong((object[0].toString())));
        message.setGroupId(object[1] != null ? object[1].toString() : null);
        message.setMessageId(object[2] != null ? object[2].toString() : null);
        message.setMessageData(object[3] != null ? object[3].toString() : null);
        message.setExchangeName(object[4] != null ? object[4].toString() : null);
        message.setExchangeType(object[5] != null ? object[5].toString() : null);
        message.setAppId(object[6] != null ? object[6].toString() : null);
        message.setHttpMethod(object[7] != null ? object[7].toString() : null);
        message.setHttpUri(object[8] != null ? object[8].toString() : null);
        message.setCustomHeaders(object[9] != null ? object[9].toString() : null);
        message.setReplyTo(object[10] != null ? object[10].toString() : null);
        message.setReplyToHttpMethod(object[11] != null ? object[11].toString() : null);
        message.setReplyToHttpUri(object[12] != null ? object[12].toString() : null);
        message.setTrasactionId(object[13] != null ? object[13].toString() : null);
        message.setCorrelationId(object[14] != null ? object[14].toString() : null);
        message.setDestinationResponseStatus(object[15] != null ? object[15].toString() : null);
        message.setCreateDateTime(object[16] != null ? (Timestamp) object[16] : null);
        return message;
    }

    private Map<String, Message> readMessagesFromIds(Session session, List<String> messageIds) {
        Map<String, Message> messages = new LinkedHashMap<>();

        if (messageIds == null || messageIds.size() == 0)
            return messages;

        //writting Prepared Statements to handle sql injection
        Query query = session.createSQLQuery(
            "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                        +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, transaction_id, "
                        +  "correlation_id, destination_response_status, created_at from " +
                        exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                        + " where message_id in ( :messageIds )  order by id ");
        //createSQLQuery is already include prepared statement .So we only need to pass the parameters required for the ids (we cannot pass the name of the table as a parameter)
        query.setParameterList("messageIds", messageIds == null || messageIds.size() == 0 ? Arrays.asList("") : messageIds );

        List<Object[]> objects = query.list();

        for (Object[] object : objects) {
            Message message = convertMessageDbObject(object);
            messages.put(object[2].toString(), message); //Key is messageID
        }
        return messages;
    }

    @Override public Map<String, Message> readMessages(List<String> messageIds) {
        Session session = null;
        Map<String, Message> messages = new HashMap<>();
        try {
            session = getSession();
            messages = readMessagesFromIds(session, messageIds);
        } catch (Exception e) {
            logger.error("Error in readMessages:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messages;
    }

    @Override
    public List<Message> readMessagesUsingSequenceIds(List<Long> messageSequenceIds){
        Session session = null;
        List<Message> messages = new ArrayList<>();
        try {
            session = getSession();
            if (messageSequenceIds == null || messageSequenceIds.size() == 0)
                return messages;

            Query query = session.createSQLQuery(
                    "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                            +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, transaction_id, "
                            +  "correlation_id, destination_response_status, created_at from " +
                            exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                            + " where id in ( :messageSequenceIds ) AND id BETWEEN :minId AND :maxId order by id ");
            query.setParameterList("messageSequenceIds", messageSequenceIds );
            query.setParameter("minId",messageSequenceIds.get(0));
            query.setParameter("maxId",messageSequenceIds.get(messageSequenceIds.size()-1));
            List<Object[]> objects = query.list();

            for (Object[] object : objects) {
                Message message = convertMessageDbObject(object);
                messages.add(message);
            }
        } catch (Exception e) {
            logger.error("Error in readMessagesUsingSequenceIds:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messages;
    }

    @Override
    public List<Message> readMessagesUsingSequenceIds(List<Long> messageSequenceIds, int count){
        Session session = null;
        List<Message> messages = new ArrayList<>();
        try {
            session = getSession();
            if (messageSequenceIds == null || messageSequenceIds.size() == 0)
                return messages;

            Query query = session.createSQLQuery(
                    "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                            +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, transaction_id, "
                            +  "correlation_id, destination_response_status, created_at from " +
                            exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                            + " where id in ( :messageSequenceIds ) AND id BETWEEN :minId AND :maxId order by id LIMIT :count");
            query.setParameterList("messageSequenceIds", messageSequenceIds );
            query.setParameter("minId",messageSequenceIds.get(0));
            query.setParameter("maxId",messageSequenceIds.get(messageSequenceIds.size()-1));
            query.setParameter("count",count);
            List<Object[]> objects = query.list();

            for (Object[] object : objects) {
                Message message = convertMessageDbObject(object);
                messages.add(message);
            }
        } catch (Exception e) {
            logger.error("Error in readMessagesUsingSequenceIds:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messages;
    }

    @Override
    // TODO : remove unused field
    public List<Message> readMessages(final Long start, final int batchSize, int delayedReadIntervalInSeconds){
        Session session = null;
        List<Message> messages = new ArrayList<>();
        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id, group_id, message_id, message, exchange_name, exchange_type, app_id, http_method,"
                            +  "http_uri, custom_headers, reply_to, reply_to_http_method, reply_to_http_uri, transaction_id, "
                            +  "correlation_id, destination_response_status, created_at from " +
                            exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                            + " where id BETWEEN :minId AND :maxId order by id ");
            query.setParameter("minId",start);
            query.setParameter("maxId",start+batchSize);
            List<Object[]> objects = query.list();

            for (Object[] object : objects) {
                Message message = convertMessageDbObject(object);
                messages.add(message);
            }
        } catch (Exception e) {
            logger.error("Error in readMessages:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messages;
    }

    @Override
    public AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds) {
        if (currentOffset == null) {
            throw new IllegalArgumentException("current offset cannot be null");
        }
        Session session = null;
        AppMessageMetaData msgMetaData = null;
        try {
            session = getSession();
            String queryString = "select id, created_at  from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE) + "  where id >=  :currentOffset";

            if (delayedReadIntervalInSeconds > 0){
                queryString += " and created_at < NOW() - INTERVAL "+ delayedReadIntervalInSeconds+" SECOND";
            }
            queryString += " order by id limit 1";

            Query query = session.createSQLQuery(queryString);
            query.setLong("currentOffset", currentOffset);
            Object[] obj = (Object[]) query.uniqueResult();
            /**
             * 0 -> Application Sequence Id
             * 1 -> Create Date Time
             */
            if (obj != null) {
                msgMetaData = new AppMessageMetaData();
                msgMetaData.setId(Double.valueOf(obj[0].toString()).longValue());
                msgMetaData.setCreateDateTime((Timestamp) obj[1]);
            }

        } catch (Exception e) {
            logger.error("Error in messagesExistForFurtherOffset: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return msgMetaData;
    }


    @Override
    public List<AppMessageMetaData> getMessageMetaData(List<String> messageIds) {
        Session session = null;
        AppMessageMetaData msgMetaData;
        List<AppMessageMetaData> messageMetaDataList = new ArrayList<>();
        if (messageIds == null || messageIds.size() == 0)
            return messageMetaDataList;

        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id, created_at from " + exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                            + "  where message_id in  ( :messageIds ) order by id ");
            query.setParameterList("messageIds", messageIds);
            List<Object[]> objects = query.list();
            /**
             * 0 -> Application Sequence Id
             * 1 -> Create Date Time
             */
            for (Object[] obj : objects) {
                msgMetaData = new AppMessageMetaData();
                msgMetaData.setId(Double.valueOf(obj[0].toString()).longValue());
                msgMetaData.setCreateDateTime((Timestamp) obj[1]);
                messageMetaDataList.add(msgMetaData);
            }
        } catch (Exception e) {
            logger.error(" Error in getMessageMetaData: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messageMetaDataList;
    }

    @Override
    public List<AppMessageMetaData> getMessageMetaData(Long start, int count, int delayedReadIntervalInSeconds) {
        Session session = null;
        List<AppMessageMetaData> appMessageMetaDataList = new ArrayList<>();
        try {
            session = getSession();

            String queryString = "select id, message_id from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE) + "  where id between :start  and  :numOfMessageToRead   ";

            if (delayedReadIntervalInSeconds > 0){
                queryString += " and created_at < NOW() - INTERVAL "+ delayedReadIntervalInSeconds+" SECOND";
            }
            queryString += " order by id";


            if (start == null) {
                throw new IllegalArgumentException("current offset cannot be null");
            }
            Query query = session.createSQLQuery(queryString);
            query.setLong("start", start);
            query.setLong("numOfMessageToRead", (start + count - 1));

            List<Object[]> objects = query.list();
            /**
             * 0 -> Application Sequence Id
             * 1 -> Unique Message Id
             */
            for (Object[] obj : objects) {
                AppMessageMetaData appMessageMetaData = new AppMessageMetaData();
                appMessageMetaData.setId(Double.valueOf(obj[0].toString()).longValue());
                appMessageMetaData.setMessageId(obj[1].toString());
                appMessageMetaDataList.add(appMessageMetaData);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return appMessageMetaDataList;
    }

    @Override
    // TODO : remove loop
    public HashMap<String, Long> processorsIdMap(HashMap<String, String> processorsMsgIdMap) {
        Session session = null;
        HashMap<String, Long> processIdMap = new HashMap<>();
        try {
            if (processorsMsgIdMap == null) {
                throw new IllegalArgumentException("processorsMsgIdMap cannot be null");
            }
            session = getSession();
            Query query = session.createSQLQuery("select id from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE) + " where message_id = :messageId").addScalar("id", LongType.INSTANCE);

            // map has <processorsId , msgId>
            for (Map.Entry<String, String> entry : processorsMsgIdMap.entrySet()) {
                query.setString("messageId", entry.getValue());
                List<Long> result = query.list();
                for (Long res : result) {
                    processIdMap.put(entry.getKey(), res);
                }
            }

        } catch (Exception e) {
            logger.error("Error in messagesExistForFurtherOffset: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return processIdMap;
    }

    @Override
    public AppMessageMetaData getMinMessageFromMessages(List<String> messageIds) {
        Session session = null;
        AppMessageMetaData msgMetaData = new AppMessageMetaData();
        msgMetaData.setId(0L);
        if (messageIds == null || messageIds.size() == 0)
            return msgMetaData;

        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id, created_at from " + exchangeTableNameProvider
                            .getTableName(TableType.MESSAGE) + "  where message_id in  ( :messageIds ) order by id limit 1");
            // Using two conditions  because setParameterList throws error when the list empty or the list is null
            query.setParameterList("messageIds", messageIds == null || messageIds.size() == 0 ? Arrays.asList("") : messageIds);
            Object[] obj = (Object[]) query.uniqueResult();
            /**
             * 0 -> Application Sequence Id
             * 1 -> Create Date Time
             */
            if (obj != null) {
                msgMetaData.setId(Double.valueOf(obj[0].toString()).longValue());
                msgMetaData.setCreateDateTime((Timestamp) obj[1]);
            }
        } catch (Exception e) {
            logger.error("Stopping Relayer:System.exit:: Error in getMinMessageFromMessages: " + e.getMessage(), e);
            System.exit(-1);
        } finally {
            closeSession(session);
        }
        return msgMetaData;
    }

    @Override public List<String> readUnsidelinedMessageIds(int count) {
        Session session = null;
        List<String> ids = null;
        try {

            session = getSession();

            Query query = session.createSQLQuery("select message_id from " +
                    exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                    + " where status= :status order by id limit :count");

            query.setString("status" , String.valueOf(SidelinedMessageStatus.UNSIDELINED));
            query.setInteger("count",count);

            ids = query.list();
        } catch (Exception e) {
            logger.error("Error in readUnsidelinedMessageIds:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return ids;
    }

    @Override public void updateSidelinedMessageStatus(List<String> messageIds,
        SidelinedMessageStatus status) {
        Session session = null;
        Transaction transaction = null;
        try {
            if (messageIds == null ){
                logger.warn("In updateSidelinedMessageStatus messageIds received null");
            }
            session = getSession();
            transaction = session.beginTransaction();
            //Writting prepared statements to remove sql injection
            Query query = session.createSQLQuery("update " +
                    exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                    + " set status = :status where message_id in (:idString )");
            // Using two conditions  because setParameterList throws error when the list empty or the list is null
            query.setParameterList("idString" , messageIds == null || messageIds.size() == 0 ? Arrays.asList("") : messageIds );
            if (status == null ){
                throw new IllegalArgumentException("status cannot be null");
            }
            query.setString("status" , (status).toString());
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateSidelinedMessageStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override
    public void updateDestinationResponseStatus(@NonNull String messageId, int statusCode) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();
            Query query = session.createSQLQuery("update " +
                    exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                    + " set destination_response_status = :status where message_id = :messageID");
            query.setString("messageID" , messageId );
            query.setInteger("status" , statusCode);
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateSidelinedMessageStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public List<ControlTask> readControlTasks(int count) {
        List<ControlTask> controlTasks = new ArrayList<>();
        List<Object[]> objects = null;
        Session session = null;
        try {
            session = getSession();

            //prepared Statement to remove sql injection
            Query query = session.createSQLQuery(
                    "select id, task_type, group_id, message_id, status, from_date, to_date from "
                            + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + " where status= :status order by id  limit :count");
            query.setString("status" , (ControlTaskStatus.NEW).toString());
            query.setInteger("count" , count);
            objects = query.list();

            if (objects == null || objects.size() == 0)
                return controlTasks;
        } catch (Exception e) {
            logger.error("Error in readControlTasks:" + e.getMessage(), e);
            return controlTasks;
        } finally {
            closeSession(session);
        }

        for (Object object[] : objects) {
            ControlTask controlTask = new ControlTask();
            controlTask.setId(Long.parseLong(object[0].toString()));
            controlTask.setTaskType(ControlTaskType.valueOfIgnoreCase(object[1].toString()));
            controlTask.setGroupId(object[2] == null ? null : object[2].toString());
            controlTask.setMessageId(object[3] == null ? null : object[3].toString());
            controlTask.setStatus(object[4].toString());
            try {
                controlTask.setFromDate(
                        object[5] == null ? null : DateHelper.getFormattedDate(object[5].toString()));
            } catch (ParseException e) {
                logger.error("readControlTasks:Error in parsing date: " + object[5] + " : " + e.getMessage(), e);
                continue;
            }
            try {
                controlTask.setToDate(
                        object[6] == null ? null : DateHelper.getFormattedDate(object[6].toString()));
            } catch (ParseException e) {
                logger.error("readControlTasks:Error in parsing date: " + object[5] + " : " + e.getMessage(), e);
                continue;
            }
            controlTasks.add(controlTask);
        }
        return controlTasks;

    }

    @Override public void updateControlTaskStatus(List<ControlTask> tasks,
                                        ControlTaskStatus status) {
        if (tasks == null || tasks.size() == 0)
            return;
        Session session = null;
        Transaction transaction = null;
        List <Long> taskIdList = new ArrayList<>();
        try {
            for (ControlTask controlTask :tasks){
                taskIdList.add(controlTask.getId());
            }
            session = getSession();
            transaction = session.beginTransaction();
            //prepared statement to remove sql injection
            Query query = session.createSQLQuery("update " +
                    exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS) + " set status= :status where id in( :idString )");
            if (status == null ){
                throw new IllegalArgumentException("status cannot be null");
            }
            query.setString("status" , (status).toString());
            query.setParameterList("idString", taskIdList == null || taskIdList.size() == 0 ? Arrays.asList("") : taskIdList);
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateControlTaskStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public void persistSkippedIds(List<Long> skippedIds) {
        if (null != skippedIds && !skippedIds.isEmpty()) {
            StringBuilder skippedIdBuilder = new StringBuilder();
            String skippedIdString;

            for (Long id : skippedIds) {
                skippedIdBuilder.append("(").append(id).append(",").append(id).append(",'")
                        .append(SkippedIdStatus.NEW).append("',NOW()),");
            }

            skippedIdString = skippedIdBuilder.substring(0, skippedIdBuilder.length() - 1);
            Session session = null;
            Transaction transaction = null;
            int queryRetryAttemptCount =0;
            int waitTime = 100;
            while(queryRetryAttemptCount++ < 5) {
                try {
                    session = getSession();
                    transaction = session.beginTransaction();

                    //prepared statement to prevent sql injection
                    Query insertQuery = session.createSQLQuery(
                            "insert into " + exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS)
                              + "(id,message_seq_id, status, created_at) values "+skippedIdString+ " on duplicate key UPDATE updated_at = NOW()");

                    int result = insertQuery.executeUpdate();
                    logger.info("persistSkippedIds::Count::" + result);

                    transaction.commit();
                    break;
                } catch (Exception e) {
                    if(queryRetryAttemptCount< 5) {
                        logger.info("Error in persistSkippedIds:" + e.getMessage(), e);
                        try {
                            Thread.sleep(waitTime * queryRetryAttemptCount);
                        } catch (InterruptedException interruptedEx) {
                            logger.error(
                                    "Stopping Relayer:System.exit:: Thread Interrupted while Waiting" + interruptedEx
                                            .getMessage(), interruptedEx);
                            System.exit(-1);
                        }
                        queryRetryAttemptCount++;
                    } else {
                        logger.error("Stopping Relayer:System.exit:: Error in persistSkippedIds:" + e.getMessage(), e);
                        System.exit(-1);
                    }
                    if (null != transaction) {
                        transaction.rollback();
                    }
                } finally {
                    closeSession(session);
                }
            }

        }

    }

    @Override public List<Long> readSkippedAppSequenceIds(long maxApplicationTransactionTime,
                                                Timestamp lastMsgTime) {
        Session session = null;
        List<Long> ids = null;
        try {
            session = getSession();
            //Put a time filter also. Pick only last 30 minutes messages;
            StringBuilder skippedAppSeqQB = new StringBuilder();
            skippedAppSeqQB.append("SELECT message_seq_id FROM ")
                .append(exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS))
                .append(" WHERE STATUS = '").append(SkippedIdStatus.NEW)
                .append("' AND created_at > DATE_SUB('").append(lastMsgTime).append("', INTERVAL ")
                .append(maxApplicationTransactionTime).append(" MINUTE) ORDER BY id");
            Query query = session.createSQLQuery(skippedAppSeqQB.toString())
                .addScalar("message_seq_id", LongType.INSTANCE);

            ids = query.list();
        } catch (Exception e) {
            logger.error("Error in readSkippedAppSequenceIds:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return ids;
    }

    @Override public void updateSkippedIdStatus(List<Long> ids, SkippedIdStatus status) {
        if (ids == null || ids.size() == 0)
            return;
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();
            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS)
                            + " set status= :status where id in( :ids)");
            if (status == null ){
                throw new IllegalArgumentException("status cannot be null");
            }
            query.setString("status" , (status).toString());

            // Using two conditions because the setParameterList will not accept empty list or null
            query.setParameterList("ids" , ids == null || ids.size()==0 ? Arrays.asList("") : ids );
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateSkippedIdStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }


    @Override public List <String> getLastProcessedMessageids() {
        Session session = null;
        List <String> result = new ArrayList<>();
        try {
            session = getSession();
            Query query = session.createSQLQuery( "select message_id from " +
                    exchangeTableNameProvider.getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                    + " where message_id is not null");

            result = query.list();
        } catch (Exception e) {
            logger.error("Error in getLastProcessedMessageids:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return result;
    }

    @Override
    public List<Long> getMessageSequenceIds(List<String> messageIds) {
        Session session = null;
        List<Long> messageSequenceIds = new ArrayList<>();
        if (messageIds == null || messageIds.size() == 0)
            return messageSequenceIds;

        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id from " + exchangeTableNameProvider.getTableName(TableType.MESSAGE)
                            + "  where message_id in  ( :messageIds ) order by id ");
            query.setParameterList("messageIds", messageIds);
            List<Object> objects = query.list();

            for (Object obj : objects) {
                messageSequenceIds.add(Long.valueOf(obj.toString()));
            }
        } catch (Exception e) {
            logger.error("Error in getMessageSequenceIds: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return messageSequenceIds;
    }

    @Override public int getNumberOfProccessors(){
        Session session = null;
        int count = 0;
        try {
            session = getSession();
            Query query = session.createSQLQuery( "select count(*) from " +
                    exchangeTableNameProvider.getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)).addScalar("count(*)" , IntegerType.INSTANCE);

            Integer result = (Integer) query.uniqueResult();
            count = result;
        } catch (Exception e) {
            logger.error("Error in getLastProcessedMessageids:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return count;
    }
    /* ReaderOutboundRepository functions end */



    /* ProcessorOutboundRepository Functions start */

    // TODO : check failure on update functions
    @Override public void markGroupUnsidelined(String groupId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();
            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                            + " set status= :status where group_id= :groupId ");
            query.setString("status" , String.valueOf(SidelinedMessageStatus.UNSIDELINED));
            query.setString("groupId" , groupId );
            query.executeUpdate();
            transaction.commit();

            transaction = session.beginTransaction();
            Query deleteGroupQuery = session.createSQLQuery(
                    "delete from " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_GROUPS)
                            + " where group_id= :groupId");
            deleteGroupQuery.setString("groupId" , groupId );
            deleteGroupQuery.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in markGroupUnsidelined:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }

    }

    @Override public void updateControlTaskStatus(Serializable id, ControlTaskStatus status) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + " set status= :status where id= :id");
            if (status == null ){
                throw new IllegalArgumentException("status cannot be null");
            }
            query.setString("status" , String.valueOf(status));
            if (id == null ){
                throw new IllegalArgumentException("id cannot be null");
            }
            query.setLong("id" , Long.valueOf(String.valueOf(id)));
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateControlTaskStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }

    }

    @Override public void insertOrupdateSidelinedMessageStatus(Serializable messageId,
                                                     Serializable groupId, SidelineReasonCode sidelineReasonCode, int statusCode, String details,
                                                     int retries, SidelinedMessageStatus status, Session session) {

        boolean isSessionCreated = false;
        Transaction transaction = null;
        try {
            if (null == session) {
                session = getSession();
                transaction = session.beginTransaction();
                isSessionCreated = true;
            } else {
                transaction = session.getTransaction();
            }

            Query getSidelinedMessageQuery = session.createSQLQuery(
                    "select id from " + exchangeTableNameProvider
                            .getTableName(TableType.SIDELINED_MESSAGES) + " where message_id= :messageId");
            getSidelinedMessageQuery.setString("messageId",messageId == null ? null :  (messageId).toString());
            List<String> idList = getSidelinedMessageQuery.list();

            if (idList.size() != 0) {
                Query query = session.createSQLQuery(
                        "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                                + " set status= :status , sideline_reason_code= :sidelineReasonCode,"
                                + " http_status_code=  :statusCode ,"
                                + " details= :details ,  retries= :retries "
                                + " where message_id= :messageId ");

                if (status == null || sidelineReasonCode == null){
                    throw new IllegalArgumentException("status or sidelineReasonCode cannot be null");
                }
                query.setString("status" , (status).toString() );
                query.setString("sidelineReasonCode" , (sidelineReasonCode).toString());
                query.setInteger("statusCode" , (statusCode) );
                query.setString("details" , details );
                query.setInteger( "retries" , retries );
                query.setString( "messageId" , messageId == null ? null :  (messageId).toString());

                query.executeUpdate();
            } else {
                Query query = session.createSQLQuery("insert into " + exchangeTableNameProvider
                        .getTableName(TableType.SIDELINED_MESSAGES)
                        + "(group_id, message_id, status, sideline_reason_code, http_status_code, details, retries, created_at)"
                        +
                        " values(:groupId, :messageId , :status , "
                        + " :sidelineReasonCode , :statusCode , :details , :retries"
                        + ",  NOW())");
                if (status == null || sidelineReasonCode == null){
                    throw new IllegalArgumentException("status or sidelineReasonCode cannot be null");
                }
                query.setString("groupId", groupId == null ? null : groupId.toString() );
                query.setString("status" ,  (status).toString() );
                query.setString("sidelineReasonCode" , String.valueOf(sidelineReasonCode));
                query.setInteger("statusCode" , (statusCode));
                query.setString("details" , details );
                query.setInteger( "retries" , retries );
                query.setString( "messageId" , messageId == null ? null :  (messageId).toString());
                query.executeUpdate();

            }

            if (isSessionCreated) {
                transaction.commit();
            }
        } catch (Exception e) {
            logger.error("Error in insertOrupdateSidelinedMessageStatus:" + e.getMessage(), e);
            if (isSessionCreated && null != transaction) {
                transaction.rollback();
            }
            throw e;
        } finally {
            if (isSessionCreated) {
                closeSession(session);
            }
        }

    }

    @Override public void insertOrupdateSidelinedMessageStatus(Serializable messageId,
                                                     Serializable groupId, SidelineReasonCode sidelineReasonCode, SidelinedMessageStatus status,
                                                     Session session) {
        insertOrupdateSidelinedMessageStatus(messageId, groupId, sidelineReasonCode, 0, null, 0,
            status, session);
    }

    @Override public boolean deleteSidelinedMessage(Serializable id) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();
            Query query = session.createSQLQuery("delete from " + exchangeTableNameProvider
                    .getTableName(TableType.SIDELINED_MESSAGES) + " where message_id= :id ");
            query.setString("id" , id == null ? null : (id).toString());
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in deleteSidelinedMessage:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }
        return true;
    }

    @Override
    public Boolean sidelineMessage(Serializable messageId, Serializable groupId,
                                   SidelineReasonCode sidelineReasonCode, int statusCode, String details, int retries) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            insertOrupdateSidelinedMessageStatus(messageId, groupId, sidelineReasonCode,
                statusCode, details, retries, SidelinedMessageStatus.SIDELINED, session);
            if (groupId != null) {
                Query getSidelinedGroupQuery = session.createSQLQuery(
                    "select id from " + exchangeTableNameProvider
                        .getTableName(TableType.SIDELINED_GROUPS) + " where group_id= :groupId");
                getSidelinedGroupQuery.setString("groupId" , groupId == null ? null :  ( groupId ).toString());

                List<Object> idList = getSidelinedGroupQuery.list();
                if (idList == null || idList.size() == 0) {
                    Query sidelineGroupQuery = session.createSQLQuery(
                            "insert into " + exchangeTableNameProvider
                                    .getTableName(TableType.SIDELINED_GROUPS)
                                    + "(group_id, created_at) values( :groupId  , :timeStamp )");

                    sidelineGroupQuery.setString("groupId" , groupId == null ? null : ( groupId ).toString());
                    sidelineGroupQuery.setTimestamp( "timeStamp" , new Timestamp(System.currentTimeMillis()) );
                    sidelineGroupQuery.executeUpdate();
                }
            }
            transaction.commit();
            return true;
        } catch (Exception e) {
            logger.error("Error in sidelineMessage:" + e.getMessage(), e);
            if (transaction != null)
                transaction.rollback();

            return false;
        } finally {
            closeSession(session);
        }
    }

    @Override public void updateSkippedMessageStatus(Long appSeqId, SkippedIdStatus status) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS)
                            + " set status= :status where id = :appSeqId ");
            if (status == null ){
                throw new IllegalArgumentException("status cannot be null");
            }
            query.setString("status" ,  String.valueOf( status ));
            if (appSeqId == null ){
                throw new IllegalArgumentException("appSeqId cannot be null");
            }
            query.setLong("appSeqId" , Long.valueOf(String.valueOf(appSeqId)) );
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in updateSkippedMessageStatus:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public void deleteSkippedMessage(Long appSeqId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "delete from " + exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS)
                            + " where id = :appSeqId ");
            if (appSeqId == null ){
                throw new IllegalArgumentException("appSeqId cannot be null");
            }
            query.setLong("appSeqId" , appSeqId);
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in deleteSkippedMessage:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public int deleteControlTaskEntries(Date beforeDate,int deleteBatchSize) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "DELETE FROM " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + " where status = 'DONE' and updated_at < :beforeDate ORDER BY ID LIMIT :deleteBatchSize");
            query.setDate("beforeDate",beforeDate);
            query.setInteger("deleteBatchSize",deleteBatchSize);
            int rowsDeleted = query.executeUpdate();
            transaction.commit();
            return rowsDeleted;
        } catch (Exception e) {
            logger.error("Error in deleteControlTaskEntries:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            throw e;
        } finally {
            closeSession(session);
        }
    }

    @Override public void unsidelineMessage(Serializable messageId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                            + " set status= :status  where message_id= :messageId ");
            query.setString("status" , String.valueOf(SidelinedMessageStatus.UNSIDELINED));
            query.setString("messageId" , String.valueOf(messageId) );
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in unsidelineMessagge:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }

    }

    @Override public void unsidelineAllUngroupedMessage(Date fromDate, Date toDate) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                            + " set status= :status where group_id is :groupId and updated_at>= :fromDate  and updated_at<= :toDate");
            query.setString("status" , String.valueOf(SidelinedMessageStatus.UNSIDELINED) );
            query.setString("groupId" , null );
            query.setTimestamp( "fromDate" , fromDate );
            query.setTimestamp( "toDate" , toDate );

            //System.out.println(query);
            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in unsidelineAllUngroupedMessage:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public HashMap <String , String> getLastProcessedMessages() {
        Session session = null;
        Transaction transaction = null;
        HashMap <String , String> processIdMessageIdMap = new HashMap<>();
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery("select process_id , message_id from " +
                    exchangeTableNameProvider.getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE) + " where message_id is not NULL"
            );

            List <Object[]> objects = query.list();

            for (Object[] object : objects){
                processIdMessageIdMap.put(object[0].toString() , object[1].toString());
            }
        } catch (Exception e) {
            logger.error("Error in getLastProcessedMessages:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return processIdMessageIdMap;
    }

    /* ProcessorOutboundRepository Functions end */

    /* ProcessorMetadataRepository functions start */
    @Override public void persistLastProcessedMessageId(String processorId, String messageId,
                                              Long appSequenceId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery("update " + exchangeTableNameProvider
                    .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE) + " set message_id= :messageId where process_id= :processId ");
            query.setString("messageId" , messageId );
            query.setString("processId" , processorId );

            query.executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in persistLastProcessedMessageId:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public Set<String> getRegisteredProcesses(List<String> ids) {
        Set<String> alreadyRegisteredProcesses = null;
        Session session = null;
        try {
            session = getSession();

            Query query = session.createSQLQuery(
                "select process_id from " + exchangeTableNameProvider
                    .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE) + " where process_id in ( :ids )");
            // Using two conditions because the setParameterList willl not accept empty list or null
            query.setParameterList("ids" , ids == null || ids.size()==0 ? Arrays.asList("") : ids );

            alreadyRegisteredProcesses = new HashSet<>(query.list());
        } catch (Exception e) {
            logger.error("Error in getRegisteredProcesses:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return alreadyRegisteredProcesses;
    }

    @Override public void registerNewProcess(List<String> processIds) {
        String idString = "";
        int idIndex = 0;
        for (String id : processIds) {
            if (idIndex == 0) {
                idString = idString + "('" + id + "', NOW())";
                idIndex++;
            } else
                idString = idString + "," + "('" + id + "',NOW())";

        }

        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query insertQuery = session.createSQLQuery("insert into " + exchangeTableNameProvider
                    .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                    + "(process_id, created_at) values  " + idString  );

            int result = insertQuery.executeUpdate();
            logger.info("registerNewProcess::Count::" + result);

            transaction.commit();
            //System.out.println(result);
        } catch (Exception e) {
            logger.error("Error in registerNewProcess:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public void clearOldProcessorRecords(List<String> currentProcesseIds) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query deleteQuery = session.createSQLQuery("delete from " + exchangeTableNameProvider
                    .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                    + " where process_id not in ( :ids )");
            // Using two conditions  because setParameterList throws error when the list empty or the list is null
            deleteQuery.setParameterList("ids" ,currentProcesseIds == null || currentProcesseIds.size()==0 ? Arrays.asList("") : currentProcesseIds);

            int result = deleteQuery.executeUpdate();
            logger.info("clearOldProcessorRecords::Count::"+result);

            transaction.commit();
            //System.out.println(result);
        } catch (Exception e) {
            logger.error("Error in clearOldProcessorRecords:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public List<String> getLastProcessedMessageIdsWithSystemExit() {
        Session session = null;
        List<String> messageIds = new ArrayList<>();
        try {
            session = getSession();

            Query query = session.createSQLQuery(
                "select message_id from " + exchangeTableNameProvider
                    .getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                    + " where message_id is not null");
            messageIds = query.list();
        } catch (Exception e) {
            logger.error("Stopping Relayer:System.exit:: Error in getLastProcessedMessageIdsWithSystemExit:" + e.getMessage(), e);
            System.exit(-1);
        } finally {
            closeSession(session);
        }
        return messageIds;
    }

    /* ProcessorMetadataRepository functions end */

    /* Relayer core Repository functions start*/

    @Override public Boolean createUnsidelineGroupTask(String groupId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "insert into " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + "(task_type, group_id, message_id, status, created_at) values ( :taskType , :grpId , :msgId , :status , NOW() )");

            query.setString("taskType" , String.valueOf(ControlTaskType.UNSIDELINE_GROUP));
            query.setString("grpId" , groupId );
            query.setString("msgId" , null );
            query.setString("status" , String.valueOf(ControlTaskStatus.NEW ));

            query.executeUpdate();
            transaction.commit();
            return true;
        } catch (Exception e) {
            logger.error("Error in createUnsidelineGroupTask:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }

    }

    @Override public Boolean createUnsidelineMessageTask(String messageId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "insert into " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + "(task_type, group_id, message_id, status, created_at) values ( :taskType , :grpId , :msgId , :status , NOW() )");

            query.setString("taskType" , String.valueOf(ControlTaskType.UNSIDELINE_MESSAGE));
            query.setString("grpId" , null );
            query.setString("msgId" , messageId );
            query.setString("status" , String.valueOf(ControlTaskStatus.NEW ));

            query.executeUpdate();
            transaction.commit();
            return true;
        } catch (Exception e) {
            logger.error("Error in createUnsidelineMessageTask:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }
    }

    // TODO : what if partial error occurs and remove loop if possible
    @Override public Boolean createUnsidelineMessagesBetweenDatesTask(Date fromDate, Date toDate) {
        Session session = null;
        try {
            session = getSession();
            Query distinctGroupsQuery = session.createSQLQuery(
                    "select distinct group_id from  " + exchangeTableNameProvider
                            .getTableName(TableType.SIDELINED_MESSAGES) +
                            " where updated_at>= :fromDate and updated_at<= :toDate ");

            distinctGroupsQuery.setTimestamp("fromDate" , fromDate );
            distinctGroupsQuery.setTimestamp("toDate" , toDate );

            List<String> distinctGroups = distinctGroupsQuery.list();
            for (String group : distinctGroups) {
                createUnsidelineGroupTask(group);
            }

            return true;
        } catch (Exception e) {
            logger.error("Error in createUnsidelineMessagesBetweenDatesTask:" + e.getMessage(), e);
            return false;
        } finally {
            closeSession(session);
        }
    }

    @Override public Boolean createUnsidelineAllUngroupedMessageTask(Date fromDate, Date toDate) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "insert into " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + "(task_type, group_id, message_id, status, from_date, to_date, created_at) values"
                            + "( :taskType , :grpId , :msgId , :status , :fromDate , :toDate , NOW() )");

            query.setString("taskType" , String.valueOf(ControlTaskType.UNSIDELINE_ALL_UNGROUPED) );
            query.setString("grpId" , null );
            query.setString("msgId" , null );
            query.setString("status" , String.valueOf(ControlTaskStatus.NEW) );
            query.setTimestamp("fromDate" , fromDate );
            query.setTimestamp("toDate" , toDate );

            query.executeUpdate();
            transaction.commit();
            return true;
        } catch (Exception e) {
            logger.error("Error in createUnsidelineAllUngroupedMessageTask:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }
    }

    @Override
    public Boolean createPartitionManagementTask(String relayerId) {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query query = session.createSQLQuery(
                    "insert into " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                            + " (task_type, group_id, status, created_at) values " + "( :controlTaskType  , :relayerId , :controlTaskStatus , NOW() )");

            query.setString("controlTaskType" , (ControlTaskType.MANAGE_PARTITION).toString());
            query.setString("relayerId" , relayerId);
            query.setString("controlTaskStatus" , (ControlTaskStatus.NEW).toString() );

            query.executeUpdate();
            transaction.commit();
            return true;
        } catch (Exception e) {
            logger.error("Error in createPartitionManagementTask:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }
    }

    @Override
    public String getLeaderRelayerUUID(String relayerId) {
        Session session = null;
        try {
            session = getSession();
            Query query = session.createSQLQuery(
                    "select relayer_uid from "+exchangeTableNameProvider.getTableName(TableType.LEADER_ELECTION)
                            + " where anchor = 1");
            return  (String) query.uniqueResult();
        } catch (Exception e) {
            logger.error("Error in getLeaderRelayerUUID:" + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return null;
    }

    @Override public void resetProcessingStateSkippedIds() {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query updateQuery = session.createSQLQuery(
                "update " + exchangeTableNameProvider.getTableName(TableType.SKIPPED_IDS)
                    + " set status= :statusFinal where status= :statusInitial");
            updateQuery.setString("statusFinal" , String.valueOf(SkippedIdStatus.NEW) );
            updateQuery.setString("statusInitial" , String.valueOf(SkippedIdStatus.PROCESSING));

            int result = updateQuery.executeUpdate();
            logger.info("resetProcessingStateSkippedIds::Count::"+result);

            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in resetProcessingStateSkippedIds:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public void resetProcessingStateControlTasks() {
        Session session = null;
        Transaction transaction = null;
        try {
            session = getSession();
            transaction = session.beginTransaction();

            Query updateQuery = session.createSQLQuery(
                "update " + exchangeTableNameProvider.getTableName(TableType.CONTROL_TASKS)
                    + " set status= :statusFinal where status= :statusInitial");
            updateQuery.setString("statusFinal" , String.valueOf(SkippedIdStatus.NEW) );
            updateQuery.setString("statusInitial" , String.valueOf(SkippedIdStatus.PROCESSING));

            int result = updateQuery.executeUpdate();
            logger.info("resetProcessingStateControlTasks::Count::"+result);

            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in resetProcessingStateControlTasks:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public void resetProcessingStateSidelinedMessages() {
        Session session = null;
        Transaction transaction = null;
        try {
            List<String> sidelinedGroupsIds = getSidelinedGroups();
            String idString = "";
            int idIndex = 0;
            if ( sidelinedGroupsIds == null ){
                logger.warn("In resetProcessingStateSidelinedMessages value of sidelinedGroupsIds is null");
            }

            session = getSession();
            transaction = session.beginTransaction();
            if ( sidelinedGroupsIds !=null && sidelinedGroupsIds.size() != 0) {
                Query updateUnsidelinedQuery = session.createSQLQuery(
                        "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                                + "  set status= :statusFinal where status= :statusInitial and group_id not in ( :ids ) or group_id is NULL");

                updateUnsidelinedQuery.setString("statusFinal" , String.valueOf(SidelinedMessageStatus.UNSIDELINED) );
                updateUnsidelinedQuery.setString("statusInitial" , String.valueOf(SidelinedMessageStatus.PROCESSING) );
                // Using two conditions  because setParameterList throws error when the list empty or the list is null
                updateUnsidelinedQuery.setParameterList("ids" , sidelinedGroupsIds == null || sidelinedGroupsIds.size() == 0 ? Arrays.asList("") : sidelinedGroupsIds );

                Query updateSidelinedQuery = session.createSQLQuery(
                        "update  " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                                + " set status= :statusFinal where status= :statusInitial  and group_id in ( :ids )");

                updateSidelinedQuery.setString("statusFinal" , String.valueOf(SidelinedMessageStatus.SIDELINED) );
                updateSidelinedQuery.setString("statusInitial" , String.valueOf(SidelinedMessageStatus.PROCESSING) );
                // Using two conditions  because setParameterList throws error when the list empty or the list is null
                updateSidelinedQuery.setParameterList("ids" , sidelinedGroupsIds == null || sidelinedGroupsIds.size() == 0 ? Arrays.asList("") : sidelinedGroupsIds );

                int unsidelineCount = updateUnsidelinedQuery.executeUpdate();
                int sidelineCount = updateSidelinedQuery.executeUpdate();
                logger.info(
                    "resetProcessingStateSidelinedMessages::Count::Unsidelined::" + unsidelineCount
                        + "Sidelined::" + sidelineCount);
            } else {
                Query updateUnsidelinedQuery = session.createSQLQuery(
                    "update " + exchangeTableNameProvider.getTableName(TableType.SIDELINED_MESSAGES)
                        + " set status= :statusFinal where status= :statusInitial ");

                updateUnsidelinedQuery.setString("statusFinal" , String.valueOf(SidelinedMessageStatus.UNSIDELINED) );
                updateUnsidelinedQuery.setString("statusInitial" , String.valueOf(SidelinedMessageStatus.PROCESSING) );

                int unsidelineCount = updateUnsidelinedQuery.executeUpdate();
                logger.info("resetProcessingStateSidelinedMessages::Count::Unsidelined::"
                    + unsidelineCount);
            }

            transaction.commit();
        } catch (Exception e) {
            logger.error("Error in resetProcessingStateSidelinedMessages:" + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
        } finally {
            closeSession(session);
        }
    }

    @Override public List<String> getSidelinedGroups() {
        Session session = null;
        List<String> groups;
        try {
            session = getSession();
            Query distinctGroupsQuery = session.createSQLQuery(
                "select distinct group_id from " + exchangeTableNameProvider
                    .getTableName(TableType.SIDELINED_GROUPS));
            groups = distinctGroupsQuery.list();
            return groups;
        } catch (Exception e) {
            logger.error("Error in getCurrentSidelinedMessageCount:" + e.getMessage(), e);
            return null;
        } finally {
            closeSession(session);
        }
    }

    @Override public Integer getCurrentSidelinedMessageCount() {
        Session session = null;
        Integer messageCount = null;
        try {
            session = getSession();
            Query messageCountQuery = session.createSQLQuery(
                "select count(*) from " + exchangeTableNameProvider
                    .getTableName(TableType.SIDELINED_MESSAGES) + " where status='SIDELINED'");

            messageCount = ((Number) messageCountQuery.uniqueResult()).intValue();
            return messageCount;
        } catch (Exception e) {
            logger.error("Error in getCurrentSidelinedMessageCount:" + e.getMessage(), e);
            return messageCount;
        } finally {
            closeSession(session);
        }
    }

    @Override public Integer getCurrentSidelinedGroupsCount() {
        Session session = null;
        Integer groupCount = null;
        try {
            session = getSession();
            Query groupCountQuery = session.createSQLQuery(
                "select count(*) from " + exchangeTableNameProvider
                    .getTableName(TableType.SIDELINED_GROUPS));

            groupCount = ((Number) groupCountQuery.uniqueResult()).intValue();
            return groupCount;
        } catch (Exception e) {
            logger.error("Error in getCurrentSidelinedGroupsCount:" + e.getMessage(), e);
            return groupCount;
        } finally {
            closeSession(session);
        }
    }

    @Override public Long getMaxMessageId() {
        Session session = null;
        try {
            session = getSession();
            Query maxMessageIdQuery =
                session.createSQLQuery("select max(id) from " + exchangeTableNameProvider.getTableName(TableType.MESSAGE));
            Object result = maxMessageIdQuery.uniqueResult();
            return result == null ? null : ((Number) result).longValue();
        } catch (HibernateException | NumberFormatException | NullPointerException e) {
            logger.error("Error in getMaxMessageId:" + e.getMessage(), e);
            return null;
        } finally {
            closeSession(session);
        }
    }

    @Override public Integer getPendingMessageCount() {
        Session session = null;
        try {
            session = getSession();
            String messagesTableName = exchangeTableNameProvider.getTableName(TableType.MESSAGE);
            String maxSequenceTableName =
                    exchangeTableNameProvider.getTableName(TableType.MAX_PROCESSED_MESSAGE_SEQUENCE);

            Query groupCountQuery = session.createSQLQuery(
                "select COALESCE((select max(id) from " + messagesTableName + "), 0) - " +
                    "COALESCE((select m.id from " + messagesTableName
                    + " m where m.message_id = (select ms.message_id from " + maxSequenceTableName
                    + " ms order by updated_at desc limit 1)), 0)");

            Object groupCountQueryResult = groupCountQuery.uniqueResult();
            if (null != groupCountQueryResult) {
                return ((Number) groupCountQueryResult).intValue();
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("Could not publish pending messages count:" + e.getMessage(), e);
            return null;
        } finally {
            closeSession(session);
        }
    }
    /* Relayer core Repository functions end  */

    @Override
    public ProcessedMessageLagTime getProcessedMessageMinMaxLagTime() {
        //inititalizing the variables
        Long lagTimeInMillsForMinMsg = null;
        Long lagTimeInMillsForMaxMsg = null;
        //get all the messages ids currently in the max_processed_message table
        List<String> messageIds = getLastProcessedMessageids();
        //get the date and time of the message_id with min id value
        List<Date> createDateTimeList = getMessageIdCreationTimeInOrder(messageIds);
        if (createDateTimeList != null && createDateTimeList.size() > 0) {
            Date minCreateDateTime = createDateTimeList.get(0);
            Date maxCreateDateTime = createDateTimeList.get(createDateTimeList.size() - 1);
            long currentDateTimeInMills = (new Date()).getTime();
            if (minCreateDateTime != null) {
                lagTimeInMillsForMinMsg = currentDateTimeInMills - minCreateDateTime.getTime();
            }
            if (maxCreateDateTime != null) {
                lagTimeInMillsForMaxMsg = currentDateTimeInMills - maxCreateDateTime.getTime();
            }
        }
        return new ProcessedMessageLagTime(lagTimeInMillsForMaxMsg,lagTimeInMillsForMinMsg);
    }

    /* Relayer core Repository functions end  */


     private List<Date> getMessageIdCreationTimeInOrder(List<String> messageIds) {
        Session session = null;
        List<Date> createDateTime = null ;
        try {
            session = getSession();
            // query to get the min value of id from message meta data table
            Query query = session.createSQLQuery(
                    "select created_at from " + exchangeTableNameProvider
                            .getTableName(TableType.MESSAGE) + "  where message_id in  ( :ids ) order by id");

            query.setParameterList("ids" , messageIds == null || messageIds.size() == 0 ? Arrays.asList("") : messageIds);
            createDateTime = query.list();
        } catch (Exception e) {
            logger.error("Error in getMessageIdCreationTimeInOrder: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return createDateTime;
    }

    private void closeSession(Session session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Error in Closing Session:" + e.getMessage(), e);
        }
    }

    private void closeSession(StatelessSession session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Error in Closing Session:" + e.getMessage(), e);
        }
    }

    private Session getSession() {
        return outboundSessionFactory.openSession();
    }

    private StatelessSession getStatelessSession() {
        return outboundSessionFactory.openStatelessSession();
    }

    @Override
    public boolean checkOrPerformLeaderElection(String relayerUID,int timeoutInSeconds,int expiryInterval){
        StatelessSession session = null;
        Transaction transaction = null;
        try {
            session = getStatelessSession();
            transaction = session.beginTransaction();
            transaction.setTimeout(timeoutInSeconds);
            Query query = session.createSQLQuery("insert ignore into "+ exchangeTableNameProvider
                    .getTableName(TableType.LEADER_ELECTION)+" (" +
                    "        anchor, relayer_uid, lease_expiry_time" +
                    ") values (" +
                    "        1, :current_relayer_uid, now() + interval :expiry_interval second " +
                    ") on duplicate key update" +
                    "        relayer_uid = if(lease_expiry_time < now(), values(relayer_uid), relayer_uid)," +
                    "        lease_expiry_time = if(relayer_uid = values(relayer_uid), values(lease_expiry_time), lease_expiry_time)" +
                    ";");

            query.setParameter("current_relayer_uid", relayerUID);
            query.setParameter("expiry_interval", expiryInterval);
            int result = query.executeUpdate();
            transaction.commit();
            logger.debug(query.getQueryString()+" checkOrPerformLeaderElection  result :"+result);
            return result > 1;
        } catch (Exception e) {
            logger.error("Error in checkOrPerformLeaderElection " + e.getMessage(), e);
            if (null != transaction) {
                transaction.rollback();
            }
            return false;
        } finally {
            closeSession(session);
        }
    }


    @Override public Long getMaxParitionID() {
        Session session = null;
        try {
            session = getSession();
            Query maxPartitionIdSupportedQuery = session.createSQLQuery(
                "SELECT PARTITION_DESCRIPTION FROM information_schema.partitions WHERE table_schema = database() AND table_name = '"
                    + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE) + "' order by PARTITION_ORDINAL_POSITION DESC limit 1");
            Object maxId = maxPartitionIdSupportedQuery.uniqueResult();
            if (maxId != null) {
                return Double.valueOf(maxId.toString()).longValue();
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("Error in getMaxParitionID:" + e.getMessage(), e);
            return null;
        } finally {
            closeSession(session);
        }
    }


    @Override
    public DBSchema getTableDescription(String tableName) {
        Session session = null;
        DBSchema schema = null;
        try {
            session = getSession();
            List<Object[]> objects = session.createSQLQuery(String.format("desc %s",(tableName))).list();
            if(objects == null || objects.size() == 0){
                return null;
            }
            schema = new DBSchema(tableName);
            for (Object[] object : objects) {
                ColumnDetails columnDetails = new ColumnDetails(
                        object[1].toString(),object[2].toString(),object[3].toString(),
                        object[4] != null ? object[4].toString() : null,object[5].toString());
                schema.getColumnDetails().put(object[0].toString(),columnDetails);
            }
        } catch (Exception e) {
            logger.error("Error occurred in getTableDescription while fetching schema ",e);
        } finally {
            closeSession(session);
        }
        return schema;
    }

    @Override
    public List<DBSchemaCharset> getTablesCharSet(List<String> tableNames) {
        Session session = null;
        List<DBSchemaCharset> charsetList = new ArrayList<>();
        try {
            if(CollectionUtils.isEmpty(tableNames)){
                return charsetList;
            }
            session = getSession();
            String charSetQuery = "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "where TABLE_SCHEMA!= 'INFORMATION_SCHEMA' and table_name in (:tableNames) and CHARACTER_SET_NAME is NOT NULL";
            Query query = session.createSQLQuery(charSetQuery);
            query.setParameterList("tableNames",tableNames);

            List<Object[]> objects = query.list();
            for (Object[] object : objects) {
                DBSchemaCharset charset = new DBSchemaCharset();
                charset.setTableSchema(object[0] != null ? object[0].toString() : null);
                charset.setTableName(object[1] != null ? object[1].toString() : null);
                charset.setColumnName(object[2] != null ? object[2].toString() : null);
                charset.setCharSetName(object[3] != null ? object[3].toString() : null);
                charsetList.add(charset);
            }

        } catch (Exception e) {
            logger.error("Error occurred in getTableDescription while fetching schema ",e);
        } finally {
            closeSession(session);
        }
        return charsetList;
    }

}
