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

import com.flipkart.varidhi.config.PartitionConfiguration;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.Partition;
import com.flipkart.varidhi.core.utils.MysqlDumpHelper;
import com.flipkart.varidhi.core.utils.SessionPropertiesHelper;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.utils.Constants;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by manmeet.singh on 23/02/16.
 */
public class OutboundPartitionRepositoryImpl implements OutboundPartitionRepository {
    private static final Logger logger =
            LoggerFactory.getLogger(OutboundPartitionRepositoryImpl.class);
    private final String SKIPPED_IDS_PARTITION_KEY = "message_seq_id";
    private final String MESSAGES_PARTITION_KEY = "id";
    private final PartitionConfiguration partitionConfiguration;
    private SessionFactory outboundSessionFactory;
    private ExchangeTableNameProvider exchangeTableNameProvider;
    private TRPartitionQueriesLogger trPartitionQueriesLogger;
    private RelayerConfiguration relayerConfiguration;
    private final PartitionAlterMonitor partitionAlterMonitor;
    private boolean shouldOnlyLogQueries;

    public OutboundPartitionRepositoryImpl(SessionFactory outboundSessionFactory,
                                           ExchangeTableNameProvider exchangeTableNameProvider,
                                           PartitionConfiguration partitionConfiguration, RelayerConfiguration relayerConfiguration, PartitionAlterMonitor partitionAlterMonitor) {
        this.outboundSessionFactory = outboundSessionFactory;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.partitionConfiguration = partitionConfiguration;
        this.relayerConfiguration = relayerConfiguration;
        this.partitionAlterMonitor = partitionAlterMonitor;
    }


    @Override
    public Long getLastMessageId() {
        Session session = null;
        String messagesTableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
        try {
            session = getSession();
            String getLastMessageIdQuery = Constants.GET_LAST_MESSAGEID_QUERY;
            getLastMessageIdQuery =
                    getLastMessageIdQuery.replace(Constants.MESSAGE_TABLE, messagesTableName);
            Query query = session.createSQLQuery(getLastMessageIdQuery);
            Object result = query.uniqueResult();
            if (null != result) {
                return ((Number) result).longValue();
            } else {
                logger.info("Could not get the last message id for Table : " + messagesTableName);
                return -1L;
            }
        } catch (Exception e) {
            logger.error("Error in fetching the last message id for Table : " + messagesTableName + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }

    @Override
    public void createNewPartition(ExchangeTableNameProvider.TableType tableName, long endId, long partitionSize) {
        String partitionName = "p" + (endId + 1);
        String table = exchangeTableNameProvider.getTableName(tableName);
        long maxPartitionId = getPartitionMaxId(tableName);
        if (maxPartitionId < endId + 1) {
            create(endId, partitionName, table);
        } else {
            logger.info("Partition" + partitionName + " Already Created for Table :" + table);
        }
    }

    private void create(long endId, String partitionName, String table) {
        String createPartitionQuery = Constants.CREATE_PARTITION_QUERY;
        createPartitionQuery =
                createPartitionQuery.replaceFirst(Constants.TABLE_IDENTIFIER, table)
                        .replaceFirst(Constants.PARTITION_IDENTIFIER, partitionName)
                        .replaceFirst(Constants.PARTITION_VALUE, Long.toString(endId + 1));
        if (partitionConfiguration.isInObserverMode() || this.shouldOnlyLogQueries) {
            logger.info("Create statement executed will be : " + createPartitionQuery);
            if (this.trPartitionQueriesLogger != null) {
                trPartitionQueriesLogger.collect(table, relayerConfiguration, TRPartitionQueriesLogger.DBTYPE.TR, createPartitionQuery);
            }
            return;
        }
        partitionAlterMonitor.monitor(createPartitionQuery, table, partitionName, () -> getSession());
    }

    @Override
    public boolean hasPendingMessages(long startId, long endId) {
        Session session = null;
        String messagesTableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
        try {
            session = getSession();
            String maxSequenceTableName = exchangeTableNameProvider
                    .getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE);
            String hasPendingMessagesQuery = Constants.HAS_PENDING_MESSAGES_QUERY;
            hasPendingMessagesQuery =
                    hasPendingMessagesQuery.replace(Constants.MESSAGE_TABLE, messagesTableName)
                            .replace(Constants.MAX_SEQ_TABLE, maxSequenceTableName)
                            .replace(Constants.END_ID, Long.toString(endId));
            Query query = session.createSQLQuery(hasPendingMessagesQuery);
            return !query.list().isEmpty();
        } catch (Exception e) {
            logger.error("Could not get pending messages for Table : " + messagesTableName + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }

    @Override
    public boolean hasSidelinedMessages(long startId, long endId) {
        Session session = null;
        String sidelineTableName = exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES);
        try {
            session = getSession();
            String messagesTableName =
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
            String getSidelineMessageQuery = Constants.HAS_SIDELINE_MESSAGES_QUERY;
            getSidelineMessageQuery =
                    getSidelineMessageQuery.replaceAll(Constants.MESSAGE_TABLE, messagesTableName)
                            .replaceAll(Constants.SIDELINE_TABLE, sidelineTableName);
            getSidelineMessageQuery =
                    getSidelineMessageQuery.replace(Constants.START_ID, Long.toString(startId))
                            .replace(Constants.END_ID, Long.toString(endId));
            Query query = session.createSQLQuery(getSidelineMessageQuery);
            return !query.list().isEmpty();
        } catch (Exception e) {
            logger.error("Could not get sidelined messages for Table : " + sidelineTableName + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }

    public boolean hasSkippedMessages(long startId, long endId) {
        Session session = null;
        try {
            session = getSession();
            String skippedTableName = exchangeTableNameProvider
                    .getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS);
            String messagesTableName =
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
            Query query = session.createSQLQuery(
                    "SELECT message.id FROM " + messagesTableName + " message JOIN " + skippedTableName
                            + " skipped " +
                            "ON message.id = skipped.message_seq_id " +
                            "WHERE message.id BETWEEN " + startId + " AND " + endId + " AND " +
                            "skipped.status IN ('" + SkippedIdStatus.NEW + "', '" +
                            SkippedIdStatus.PROCESSING + "') LIMIT 1");
            return !query.list().isEmpty();
        } catch (Exception e) {
            logger.error("Could not get skipped messages: " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }

    @Override
    public void backupPartition(Partition messagesPartition) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
        MysqlDumpHelper
                .takeDump(messagesPartition.getStartId(), messagesPartition.getEndId(), tableName,
                        outboundSessionFactory, MESSAGES_PARTITION_KEY);
    }

    @Override
    public Date lastCreatedDateInPartition(long startId, long endId) {
        Session session = null;
        try {
            session = getSession();
            String messagesTableName =
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
            Query query = session.createSQLQuery(
                    "SELECT created_at from " + messagesTableName + " WHERE id between " +
                            startId + " AND " + endId + " ORDER BY id DESC LIMIT 1");
            return ((Date) query.uniqueResult());
        } catch (Exception e) {
            logger.error("Could not get last created date in partition: " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }

    @Override
    public long getPartitionMaxId(ExchangeTableNameProvider.TableType tableType) {
        Session session = null;
        Properties urlProperties = SessionPropertiesHelper.getUrlProperties(outboundSessionFactory);
        String dbName = urlProperties.getProperty("DBNAME");
        String tableName = exchangeTableNameProvider.getTableName(tableType);
        try {
            session = getSession();
            String getPartitionMaxIdQuery = Constants.GET_PARTITION_MAX_ID_QUERY;
            getPartitionMaxIdQuery =
                    getPartitionMaxIdQuery.replace(Constants.TABLE_IDENTIFIER, tableName)
                            .replace(Constants.DATABASE_IDENTIFIER, dbName);
            Query query = session.createSQLQuery(getPartitionMaxIdQuery);
            Object res = query.uniqueResult();
            if(res == null) {
                throw new RuntimeException("Could not get Max Partition Id for Table : " + tableName);
            }
            return ((BigInteger) res).longValue();
        } catch (Exception e) {
            logger.error("Could not get Max Partition Id for Table : " + tableName + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }

    }

    @Override
    public void dropPartition(Partition messagesPartition) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
        String dropPartitionQuery = Constants.DROP_PARTITION_QUERY;
        dropPartitionQuery = dropPartitionQuery.replaceFirst(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.PARTITION_IDENTIFIER, messagesPartition.getPartitionName());
        if (partitionConfiguration.isInObserverMode() || shouldOnlyLogQueries) {
            logger.info("Drop statement executed will be : " + dropPartitionQuery);
            if (trPartitionQueriesLogger != null) {
                trPartitionQueriesLogger.collect(tableName, relayerConfiguration, TRPartitionQueriesLogger.DBTYPE.TR, dropPartitionQuery);
            }
            return;
        }
        logger.info("Dropping partition '" + messagesPartition.getPartitionName() + "' for table "
                + tableName);
        partitionAlterMonitor.monitor(dropPartitionQuery, tableName, messagesPartition.getPartitionName(), () -> getSession());
    }

    @Override
    public void backupSkippedIdsPartition(Partition partition) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS);
        MysqlDumpHelper.takeDump(partition.getStartId(), partition.getEndId(), tableName,
                outboundSessionFactory, SKIPPED_IDS_PARTITION_KEY);
    }

    @Override
    public void dropSkippedIdsPartition(Partition partition) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS);
        String dropPartitionQuery = Constants.DROP_PARTITION_QUERY;
        dropPartitionQuery = dropPartitionQuery.replaceFirst(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.PARTITION_IDENTIFIER, partition.getPartitionName());

        if (partitionConfiguration.isInObserverMode() || shouldOnlyLogQueries) {
            logger.info("Drop statement executed will be : " + dropPartitionQuery);
            if (trPartitionQueriesLogger != null) {
                trPartitionQueriesLogger.collect(tableName, relayerConfiguration, TRPartitionQueriesLogger.DBTYPE.TR, dropPartitionQuery);
            }
            return;
        }
        logger.info(
                "Dropping partition '" + partition.getPartitionName() + "' for table " + tableName);
        partitionAlterMonitor.monitor(dropPartitionQuery, tableName, partition.getPartitionName(), () -> getSession());
    }

    @Override
    public Partition getMessagesPartition(long partitionStartId) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE);
        String getPartitionsQuery = Constants.GET_PARTITION_QUERY;
        getPartitionsQuery = getPartitionsQuery
                .replaceAll(Constants.PARTITION_START_IDENTIFIER, Long.toString(partitionStartId))
                .replaceAll(Constants.PARTITION_END_IDENTIFIER, Long.toString(partitionStartId))
                .replaceAll(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.OPERATOR_IDENTIFIER, ">");
        getPartitionsQuery += Constants.LIMIT_CLAUSE;
        List<Partition> partitionList =
                getPartitionList(partitionStartId, tableName, getPartitionsQuery);
        if (partitionList.size() > 0) {
            return partitionList.get(0);
        } else {
            return null;
        }
    }

    public void setTrPartitionQueriesLogger(TRPartitionQueriesLogger trPartitionQueriesLogger, boolean shouldOnlyLogQueries) {
        this.trPartitionQueriesLogger = trPartitionQueriesLogger;
        this.shouldOnlyLogQueries = shouldOnlyLogQueries;
    }

    @Override
    public ArrayList<Partition> getSkippedIdsPartitionList(long partitionStartId,
                                                           long partitionEndId) {
        String tableName =
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS);
        String getPartitionsQuery = Constants.GET_PARTITION_QUERY;
        getPartitionsQuery = getPartitionsQuery
                .replaceAll(Constants.PARTITION_START_IDENTIFIER, Long.toString(partitionStartId))
                .replaceAll(Constants.PARTITION_END_IDENTIFIER, Long.toString(partitionEndId))
                .replaceAll(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.OPERATOR_IDENTIFIER, "<=");
        return getPartitionList(partitionEndId, tableName, getPartitionsQuery);
    }

    private ArrayList<Partition> getPartitionList(long partitionId, String tableName,
                                                  String getPartitionsQuery) {
        Session session = null;
        Properties urlProperties = SessionPropertiesHelper.getUrlProperties(outboundSessionFactory);
        String dbName = urlProperties.getProperty("DBNAME");
        getPartitionsQuery = getPartitionsQuery.replaceAll(Constants.DATABASE_IDENTIFIER, dbName);
        ArrayList<Partition> partitionList = new ArrayList<>();
        try {
            session = getSession();
            Query query = session.createSQLQuery(getPartitionsQuery);

            List<Object[]> partitionMetadataList = query.list();
            for (Object[] partitionMetadata : partitionMetadataList) {
                if (null != partitionMetadata) {
                    Partition partition =
                            new Partition(((BigInteger) partitionMetadata[0]).longValue(),
                                    Long.parseLong((String) partitionMetadata[1]),
                                    (String) partitionMetadata[2]);
                    if (partition.getPartitionName() == null) {
                        throw new RuntimeException(
                                "No partition found in table: " + tableName + " for the startId: "
                                        + partitionId);
                    }
                    partitionList.add(partition);
                }
            }
        } catch (Exception e) {
            logger.error("Exception while getting partition name for Table : " + tableName + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
        return partitionList;
    }


    private Session getSession() {
        return outboundSessionFactory.openSession();
    }

    private void closeSession(Session session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Could not close Outbound Archival session: " + e.getMessage(), e);
        }
    }
}
