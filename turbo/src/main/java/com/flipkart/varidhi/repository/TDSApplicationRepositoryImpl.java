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
import com.flipkart.varidhi.utils.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.type.LongType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by ashudeep.sharma on 15/12/16.
 */

public class TDSApplicationRepositoryImpl implements ApplicationRepository {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationRepositoryImpl.class);
    SessionFactory applicationSessionFactory;
    ExchangeTableNameProvider exchangeTableNameProvider;

    public TDSApplicationRepositoryImpl(SessionFactory applicationSessionFactory,
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
                    .getTableName(TableType.MESSAGE_META_DATA) + "  where id between " + start
                    + " and " + (start + count - 1);
            if (delayedReadIntervalInSeconds > 0){
                queryString += " and created_at < NOW() - INTERVAL "+ delayedReadIntervalInSeconds+" SECOND";
            }
            queryString += " order by id";


            Query query = session.createSQLQuery(queryString);
            List<Object[]> objList = query.list();
            int size = objList.size();
            List<Object[]> orderedObjects = getSortedObjectsByCount(objList, size);
            for (Object[] obj : orderedObjects) {
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
        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id, message_id from " + exchangeTableNameProvider
                            .getTableName(TableType.MESSAGE_META_DATA) + "  where id in  (" + StringUtils
                            .formatIds(ids) + ") order by id limit " + count);

            List<Object[]> objects = getSortedObjectsByCount(query.list(), count);
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
    public AppMessageMetaData messagesExistForFurtherOffset(Long currentOffset, int delayedReadIntervalInSeconds) {
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

            Object[] obj = getMinValueObject(query.list());
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
                            .getTableName(TableType.MESSAGE_META_DATA) + "  where message_id in  ("
                            + StringUtils.formatIds(messageIds) + ")");

            Object[] obj = getMinValueObject(query.list());
            if (obj != null) {
                msgMetaData.setId(Double.valueOf(obj[0].toString()).longValue());
                msgMetaData.setCreateDateTime((Timestamp) obj[1]);
            }

        } catch (Exception e) {
            logger.error("Stopping Relayer:System.exit:: Error in getMinMessageFromMessages: " + e.getMessage(),
                    e);
            System.exit(-1);
        } finally {
            closeSession(session);
        }
        return msgMetaData;
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
    public List<AppMessageMetaData> getMessageMetaData(List<String> messageIds) {
        Session session = null;
        AppMessageMetaData msgMetaData;
        List<AppMessageMetaData> messageMetaDataList = new ArrayList<>();
        if (messageIds == null || messageIds.size() == 0)
            return messageMetaDataList;

        try {
            session = getSession();

            Query query = session.createSQLQuery(
                    "select id, created_at from " + exchangeTableNameProvider
                            .getTableName(TableType.MESSAGE_META_DATA) + "  where message_id in  ( :messageIds ) order by id ");
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

    private Object[] getMinValueObject(List<Object[]> idList) {
        if (idList != null && idList.size() > 0) {
            Long minId = Long.MAX_VALUE;
            Object[] minValueObj = {};
            for (Object[] object : idList) {
                Long id = Double.valueOf(object[0].toString()).longValue();
                if (minId > id) {
                    minId = id;
                    minValueObj = object;
                }
            }
            return minValueObj;
        }
        return null;
    }

    private List<Object[]> getSortedObjectsByCount(List<Object[]> idList, int count) {
        Map<Long, Object[]> map = new TreeMap<>();
        List<Object[]> objects = new ArrayList<>();
        if (idList != null && idList.size() > 0) {
            for (Object[] object : idList) {
                map.put(Double.valueOf(object[0].toString()).longValue(), object);
            }
            int numObjects = 0;
            for (Map.Entry keyVal : map.entrySet()) {
                numObjects++;
                objects.add((Object[]) keyVal.getValue());
                if (numObjects == count) {
                    break;
                }
            }
        }
        return objects;
    }

    @Override
    public HashMap<String, Long> processorsIdMap(HashMap<String, String> processorsMsgIdMap) {
        Session session = null;
        HashMap<String, Long> processIdMap = new HashMap<>();
        try {
            session = getSession();
            Query query = session.createSQLQuery("select id from " + exchangeTableNameProvider
                    .getTableName(TableType.MESSAGE_META_DATA) + " where message_id = :messageId").addScalar("id", LongType.INSTANCE);

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
