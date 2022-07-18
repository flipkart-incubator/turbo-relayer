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
import com.flipkart.varidhi.utils.Constants;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by manmeet.singh on 23/02/16.
 */
public class ApplicationPartitionRepositoryImpl implements ApplicationPartitionRepository {
    private static final Logger logger =
            LoggerFactory.getLogger(ApplicationPartitionRepositoryImpl.class);
    private final String MESSAGE_META_DATA_PARTITION_KEY = "id";
    private final PartitionConfiguration partitionConfiguration;
    private RelayerConfiguration relayerConfiguration;
    private final PartitionAlterMonitor partitionAlterMonitor;
    private SessionFactory applicationSessionFactory;
    private ExchangeTableNameProvider exchangeTableNameProvider;
    private TRPartitionQueriesLogger trPartitionQueriesLogger;
    private boolean shouldOnlyLogQueries;

    public ApplicationPartitionRepositoryImpl(SessionFactory applicationSessionFactory,
                                              ExchangeTableNameProvider exchangeTableNameProvider,
                                              PartitionConfiguration partitionConfiguration, RelayerConfiguration relayerConfiguration, PartitionAlterMonitor partitionAlterMonitor) {
        this.applicationSessionFactory = applicationSessionFactory;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.partitionConfiguration = partitionConfiguration;
        this.relayerConfiguration = relayerConfiguration;
        this.partitionAlterMonitor = partitionAlterMonitor;
    }

    @Override
    public void dropPartition(Partition partition) {
        String tableName = exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA);
        String dropPartitionQuery = Constants.DROP_PARTITION_QUERY;
        dropPartitionQuery = dropPartitionQuery.replaceFirst(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.PARTITION_IDENTIFIER, partition.getPartitionName());
        if (partitionConfiguration.isInObserverMode() || shouldOnlyLogQueries) {
            logger.info("Drop statement executed will be : " + dropPartitionQuery);
            if (this.trPartitionQueriesLogger != null) {
                trPartitionQueriesLogger.collect(tableName, relayerConfiguration, TRPartitionQueriesLogger.DBTYPE.APPLICATION, dropPartitionQuery);
            }
            return;
        }
        logger.info(
                "Dropping partition '" + partition.getPartitionName() + "' for table " + tableName);
        partitionAlterMonitor.monitor(dropPartitionQuery, tableName, partition.getPartitionName(), () -> getSession());

    }

    /*
    public String getPartitionName(long partitionStartId, long partitionEndId) {
        Session session = null;
        String tableName = exchangeTableNameProvider
            .getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA);
        try {
            session = getSession();
            Query query = session.createSQLQuery(
                "EXPLAIN PARTITIONS SELECT * FROM " + tableName + " WHERE id BETWEEN "
                    + partitionStartId + " AND " + partitionEndId);
            String partitionName = (String) ((Object[]) query.uniqueResult())[3];
            if (partitionName == null) {
                throw new RuntimeException(
                    "No partition found in table: " + tableName + " for the range "
                        + partitionStartId + " to " + partitionEndId);
            }
            if (partitionName.contains(",")) {
                throw new RuntimeException(
                    "Multiple partitions named : " + partitionName + " found in table: " + tableName
                        + " for the range " + partitionStartId + " to " + partitionEndId);
            }
            return partitionName;
        } catch (Exception e) {
            logger.error("Exception while getting partition name : ", e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
    }
    */

    @Override
    public void backupPartition(Partition partition) {
        String tableName = exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA);
        MysqlDumpHelper.takeDump(partition.getStartId(), partition.getEndId(), tableName,
                applicationSessionFactory, MESSAGE_META_DATA_PARTITION_KEY);

    }

    @Override
    public void createNewPartition(ExchangeTableNameProvider.TableType tableName, long endId, long partitionSize) {
        String partitionName = "p" + (endId + 1);
        String table = exchangeTableNameProvider.getTableName(tableName);
        if (partitionConfiguration.isInObserverMode()) {
            create(endId, partitionName, table);
            return;
        }
        long maxPartitionId = getPartitionMaxId();
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
                trPartitionQueriesLogger.collect(table, relayerConfiguration, TRPartitionQueriesLogger.DBTYPE.APPLICATION, createPartitionQuery);
            }
            return;
        }
        partitionAlterMonitor.monitor(createPartitionQuery, table, partitionName, () -> getSession());
    }

    private Session getSession() {
        return applicationSessionFactory.openSession();
    }

    private void closeSession(Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            logger.error("Could not close application partitionManager session: ", e);
        }
    }

    private ArrayList<Partition> getPartitionList(long partitionId, String tableName,
                                                  String getPartitionsQuery) {
        Session session = null;
        Properties urlProperties =
                SessionPropertiesHelper.getUrlProperties(applicationSessionFactory);
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
            logger.error("Exception while getting partition name : ", e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }
        return partitionList;
    }

    @Override
    public ArrayList<Partition> getMetadataPartitionList(long partitionStartId,
                                                         long partitionEndId) {
        String tableName = exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA);
        String getPartitionsQuery = Constants.GET_PARTITION_QUERY;
        getPartitionsQuery = getPartitionsQuery
                .replaceAll(Constants.PARTITION_START_IDENTIFIER, Long.toString(partitionStartId))
                .replaceAll(Constants.PARTITION_END_IDENTIFIER, Long.toString(partitionEndId))
                .replaceAll(Constants.TABLE_IDENTIFIER, tableName)
                .replaceFirst(Constants.OPERATOR_IDENTIFIER, "<=");
        return getPartitionList(partitionEndId, tableName, getPartitionsQuery);
    }

    public void setTrPartitionQueriesLogger(TRPartitionQueriesLogger trPartitionQueriesLogger, boolean shouldOnlyLogQueries) {
        this.trPartitionQueriesLogger = trPartitionQueriesLogger;
        this.shouldOnlyLogQueries = shouldOnlyLogQueries;
    }

    @Override
    public long getPartitionMaxId() {
        Session session = null;
        Properties urlProperties =
                SessionPropertiesHelper.getUrlProperties(applicationSessionFactory);
        String dbName = urlProperties.getProperty("DBNAME");
        String tableName = exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA);
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
            logger.error("Could not get Max Partition Id for Table : " + tableName, e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            closeSession(session);
        }

    }
}
