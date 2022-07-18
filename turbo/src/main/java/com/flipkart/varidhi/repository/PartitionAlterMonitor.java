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

import com.flipkart.varidhi.jobs.MonitorDeadlock;
import com.flipkart.varidhi.utils.Constants;
import org.hibernate.JDBCException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class PartitionAlterMonitor {

    private static final Logger logger =
            LoggerFactory.getLogger(PartitionAlterMonitor.class);
    private final int numRetry;
    private final Long querySleepTime;
    private Long monitorThreadSleepTime;
    private Long deadlockQueryExecutionTime;

    public PartitionAlterMonitor(Long monitorThreadSleepTime, Long deadlockQueryExecutionTime, int numRetry, Long querySleepTime) {
        this.monitorThreadSleepTime = monitorThreadSleepTime;
        this.deadlockQueryExecutionTime = deadlockQueryExecutionTime;
        this.numRetry = numRetry;
        this.querySleepTime = querySleepTime;
    }

    public void monitor(final String query, String tableName, String partitionName, Supplier<Session> sessionSupplier) {
        MonitorDeadlock monitorDeadlock = new MonitorDeadlock(query, sessionSupplier, "[Alter]: Messages",
                monitorThreadSleepTime, deadlockQueryExecutionTime);
        int queryRetryAttemptCount = 1;
        Session session = null;
        while (queryRetryAttemptCount <= numRetry) {
            try {
                monitorDeadlock.start();
                session = sessionSupplier.get();
                Query q = session.createSQLQuery(query);
                q.executeUpdate();
                break;
            } catch (JDBCException jdbcException) {
                //this case will not occur in drop partition query.
                String duplicatePartitionErrorMessageMessage =
                        Constants.DUPLICATE_PARTITION_ERROR_MESSAGE;
                if (jdbcException.getErrorCode() == 1517 && jdbcException.getMessage()
                        .equals(duplicatePartitionErrorMessageMessage
                                .replace(Constants.PARTITION_IDENTIFIER, partitionName))) {
                    logger.warn("Partition Already Exists :" + partitionName);
                    break;
                }
                if (jdbcException.getErrorCode() == Constants.KILL_QUERY_ERROR_CODE
                        && queryRetryAttemptCount < numRetry) {
                    logger.warn(
                            "[Attempt]:" + queryRetryAttemptCount + "Partition Alter Query Killed"
                                    + query + "Sleeping for " + querySleepTime
                                    + " milliseconds");
                    queryRetryAttemptCount++;
                    monitorDeadlock.stop();
                    try {
                        Thread.sleep(querySleepTime);
                    } catch (InterruptedException interruptedException) {
                        logger.error(
                                "Thread Interrupted while Waiting" + interruptedException.getMessage(), interruptedException);
                        throw new RuntimeException(interruptedException);
                    }
                } else {
                    logger.error("Error while altering partition for Table : " + tableName
                            + "exhausted Number of Attempts :" + numRetry + " : " + jdbcException.getMessage(), jdbcException);
                    throw new RuntimeException(jdbcException);
                }
            } catch (Exception e) {
                logger.error("Error while altering partition for Table : " + tableName + " : " + e.getMessage(), e);
                throw new RuntimeException(e);
            } finally {
                monitorDeadlock.stop();
                closeSession(session);
            }
        }
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
