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

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.PartitionConfiguration;
import com.flipkart.varidhi.jobs.MonitorDeadlock;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.hibernate.SessionFactory;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
 * *
 * Author: abhinavp
 * Date: 19-Aug-2015
 *
 */
public class TestDeadlocks extends DBBaseTest{
    private static final String turboTable = "turboTable";
    private static final String tableSQL =
            "CREATE TABLE IF NOT EXISTS `" + turboTable +"` (`id` int(11) NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`)) PARTITION BY RANGE (id)(PARTITION p1 VALUES LESS THAN (1000),PARTITION p2 VALUES LESS THAN (2000));";
    public static final String CONNECTION_STRING = "jdbc:mysql://localhost:3344/app";
    public static final String USER = "test";
    public static final String PASSWORD = "test";
    private final ApplicationConfiguration applicationConfiguration;

    public TestDeadlocks() {
        applicationConfiguration = new ApplicationConfiguration();
        applicationConfiguration.setPartitionConfiguration(new PartitionConfiguration());
    }

    private static boolean checkPartitionDeadlock(String query)
            throws SQLException {

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
            connection.setAutoCommit(false);
            connection.createStatement().execute(tableSQL);

            //Locking the Table
            connection.createStatement().execute("UPDATE " + turboTable + " set id =2;");
            TestDeadlocks testDeadlocks = new TestDeadlocks();
            SessionFactory sessionFactory = testDeadlocks.getSessionFactory();
            MonitorDeadlock monitorDeadlock = new MonitorDeadlock(query, () -> sessionFactory.openSession(), "Test",
                    testDeadlocks.getApplicationConfiguration().getPartitionConfiguration().getMonitorThreadSleepTime(),
                    testDeadlocks.getApplicationConfiguration().getPartitionConfiguration().getDeadlockQueryExecutionTime());
            monitorDeadlock.start();
            Connection newConnection = null;
            try {
                newConnection = DriverManager.getConnection (CONNECTION_STRING, USER, PASSWORD);
                newConnection.createStatement().execute(query);
            } catch (SQLException sqlException) {
                if (null != newConnection) {
                    newConnection.close();
                }
                return false;
            } finally {
                monitorDeadlock.stop();
            }
            connection.rollback();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            if (null != connection) {
                connection.rollback();
                connection.close();
            }
            return false;
        } finally {
            if (!connection.isClosed()) {
                connection.close();
            }
            return true;
        }
    }

    @Test
    public void testPartitionCreationDeadlock() throws Exception {
        Assert.assertEquals(true,
                checkPartitionDeadlock("ALTER TABLE "+turboTable+" ADD PARTITION (PARTITION p3 VALUES LESS THAN (3000))"));
    }

    @Test
    public void testPartitionDropDeadlock() throws Exception {
        Assert.assertEquals(true, checkPartitionDeadlock("ALTER TABLE "+turboTable+" DROP PARTITION p2"));
    }

    public ApplicationConfiguration getApplicationConfiguration() {
        return this.applicationConfiguration;
    }

    private SessionFactory getSessionFactory() {

        Map<String, Properties> mysqlProperties = new HashMap<>();
        Properties properties = new Properties();
        properties.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");
        properties.setProperty("hibernate.connection.username", USER);
        properties.setProperty("hibernate.connection.password", PASSWORD);
        properties.setProperty("hibernate.connection.url", CONNECTION_STRING);
        mysqlProperties.put("test", properties);

        this.applicationConfiguration.setMysql(mysqlProperties);
        this.applicationConfiguration.getPartitionConfiguration().setDeadlockQueryExecutionTime(5L);
        this.applicationConfiguration.getPartitionConfiguration().setMonitorThreadSleepTime(200L);

        SessionFactory sessionFactory = null;
        for (Map.Entry<String, Properties> propertyMap : applicationConfiguration.getMysql()
                .entrySet()) {
            ServiceRegistry serviceRegistry =
                    new ServiceRegistryBuilder().applySettings(propertyMap.getValue())
                            .buildServiceRegistry();
            sessionFactory =
                    new org.hibernate.cfg.Configuration().addProperties(propertyMap.getValue())
                            .buildSessionFactory(serviceRegistry);
        }
        return sessionFactory;
    }

}
