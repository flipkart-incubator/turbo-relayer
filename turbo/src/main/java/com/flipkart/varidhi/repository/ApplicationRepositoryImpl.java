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

import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.schemavalidator.models.ColumnDetails;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchema;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider.TableType;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.type.LongType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 29-Jul-2015
 *
 */
public class ApplicationRepositoryImpl implements ApplicationRepository {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationRepositoryImpl.class);
    SessionFactory applicationSessionFactory;
    ExchangeTableNameProvider exchangeTableNameProvider;

    public ApplicationRepositoryImpl(SessionFactory applicationSessionFactory,
                                     ExchangeTableNameProvider exchangeTableNameProvider) {
        this.applicationSessionFactory = applicationSessionFactory;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
    }

    @Override
    public List<AppMessageMetaData> getMessageMetaData(Long start, int count) {
        return getMessageMetaData(start, count, 0);
    }

    @Override
    public List<AppMessageMetaData> getMessageMetaData(Long start, int count, int delayedReadIntervalInSeconds) {
        Session session = null;
        List<AppMessageMetaData> appMessageMetaDataList = new ArrayList<>();
        try {
            session = getSession();

            String queryString = "select id, message_id from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE_META_DATA) + "  where id between :start  and  :numOfMessageToRead   ";

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
    public Map<Long, AppMessageMetaData> getMessageMetaData(List<Long> ids, int count) {
        Session session = null;
        Map<Long, AppMessageMetaData> appMessageMetaDataMap = new HashMap<>();
        if (ids == null || ids.size() == 0) {
            logger.error("The values of ids in getMessageMetaData is null");
            return appMessageMetaDataMap;
        }
        try {
            session = getSession();
            Query query = session.createSQLQuery(
                    "select id, message_id from " + exchangeTableNameProvider
                            .getTableName(TableType.MESSAGE_META_DATA) + "  where id in  ( :ids ) order by id limit :count");
                // Using two conditions  because setParameterList throws error when the list empty or the list is null
            query.setParameterList("ids", ids);
            query.setInteger("count", count);

            List<Object[]> objects = query.list();
            /**
             * 0 -> Application Sequence Id
             * 1 -> Unique Message Id
             */
            for (Object[] obj : objects) {
                AppMessageMetaData appMessageMetaData = new AppMessageMetaData();
                Long objId = Double.valueOf(obj[0].toString()).longValue();
                appMessageMetaData.setId(objId);
                appMessageMetaData.setMessageId(obj[1].toString());
                appMessageMetaDataMap.put(objId, appMessageMetaData);
            }
        } catch (Exception e) {
            logger.error("Error in getMessageMetaData: " + e.getMessage(), e);
        } finally {
            closeSession(session);
        }
        return appMessageMetaDataMap;
    }

    @Override
    public AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset,int delayedReadIntervalInSeconds) {
        if (currentOffset == null) {
            throw new IllegalArgumentException("current offset cannot be null");
        }
        Session session = null;
        AppMessageMetaData msgMetaData = null;
        try {
            session = getSession();
            String queryString = "select id, created_at  from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE_META_DATA) + "  where id >=  :currentOffset";

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

    /* Relayer Repository Functions start here */

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
                            .getTableName(TableType.MESSAGE_META_DATA) + "  where message_id in  ( :messageIds ) order by id limit 1");
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
                    "select id, created_at from " + exchangeTableNameProvider.getTableName(TableType.MESSAGE_META_DATA)
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
            logger.error("Stopping Relayer:System.exit:: Error in getMaxMessageFromMessages: " + e.getMessage(), e);
            System.exit(-1);
        } finally {
            closeSession(session);
        }
        return messageMetaDataList;
    }

    @Override
    public HashMap<String, Long> processorsIdMap(HashMap<String, String> processorsMsgIdMap) {
        Session session = null;
        HashMap<String, Long> processIdMap = new HashMap<>();
        try {
            session = getSession();
            Query query = session.createSQLQuery("select id from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE_META_DATA) + " where message_id = :messageId").addScalar("id", LongType.INSTANCE);

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
    /* Relayer Repository Functions end here */



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
            logger.error("Error occurred while fetching schema",e);
        } finally {
            closeSession(session);
        }
        return schema;
    }

    private void closeSession(Session session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Error in closeSession: " + e.getMessage(), e);
        }
    }

    private Session getSession() {
        return applicationSessionFactory.openSession();
    }

}
