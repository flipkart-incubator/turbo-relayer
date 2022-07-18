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

package com.flipkart.varidhi.jobs;

import com.flipkart.varidhi.utils.Constants;
import org.hibernate.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by ashudeep.sharma on 20/09/16.
 */
public class MonitorDeadlock implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MonitorDeadlock.class);
    private final String deadlockQuery;
    private Long monitorThreadSleepTime;
    private Long deadlockQueryExecutionTime;
    private final Supplier<Session> sessionSupplier;
    private final String threadInitiator;
    Thread monitorDeadlockThread = null;

    public MonitorDeadlock(final String deadLockQuery, Supplier<Session> sessionSupplier, String threadInitiator, Long monitorThreadSleepTime, Long deadlockQueryExecutionTime) {
        this.sessionSupplier = sessionSupplier;
        this.threadInitiator = threadInitiator;
        this.deadlockQuery = deadLockQuery;
        this.monitorThreadSleepTime = monitorThreadSleepTime;
        this.deadlockQueryExecutionTime = deadlockQueryExecutionTime;
    }

    public void stop() {
        if (monitorDeadlockThread != null) {
            monitorDeadlockThread.interrupt();
        }
    }

    public void run() {
        Session session = sessionSupplier.get();
        try {
            logger.info("Starting Monitor Deadlock from " + this.threadInitiator);
            while (!Thread.currentThread().isInterrupted()) {
                removeDeadlock(session);
                Thread.sleep(monitorThreadSleepTime);
            }
            logger.info("Ending Monitor Deadlock from " + this.threadInitiator);
        } catch (InterruptedException iEx) {
            logger.info("Thread requested to stop");
        } catch (Exception ex) {
            logger.error(
                    "Monitor Deadlock Job Failed for " + this.threadInitiator + " : " + ex.getMessage(),
                    ex);
        } finally {
            closeSession(session);
        }
    }

    protected void removeDeadlock(Session session) {
        final String queryState = "Waiting for table metadata lock";
        try {
            String deadlockDetectionQuery = Constants.DEADLOCK_DETECTION_QUERY;
            deadlockDetectionQuery =
                    deadlockDetectionQuery.replaceFirst(Constants.STATE_IDENTIFIER, queryState)
                            .replaceFirst(Constants.QUERY_IDENTIFIER, this.deadlockQuery);
            logger.info("[Verifying Deadlocks]- Executing Query :" + deadlockDetectionQuery);
            Query query = session.createSQLQuery(deadlockDetectionQuery);
            List<Object[]> objects = query.list();
            for (Object[] obj : objects) {
                if (null != obj[0]) {
                    long queryId = ((BigInteger) obj[0]).longValue();
                    int executionTime = (int) obj[1];
                    if (executionTime >= deadlockQueryExecutionTime) {
                        String deadlockRemovalQuery = Constants.KILL_QUERY_COMMAND;
                        deadlockRemovalQuery = deadlockRemovalQuery
                                .replaceFirst(Constants.QUERY_ID, Long.toString(queryId));
                        logger.info("[Removing Deadlock]- for Query" + this.deadlockQuery
                                + ".Executing Query :" + deadlockRemovalQuery);
                        Query killQuery = session.createSQLQuery(deadlockRemovalQuery);
                        killQuery.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in Executing Queries through Monitor Thread ", e);
            throw new RuntimeException(e);
        }
    }

    private void closeSession(Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            logger.error("Could not close session: ", e);
        }
    }

    public void start() {
        logger.info("Starting Monitor Deadlock Thread for Query :" + this.deadlockQuery);
        monitorDeadlockThread = new Thread(this,"Monitor_Deadlock_Thread");
        monitorDeadlockThread.start();
    }

}
