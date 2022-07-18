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

import com.flipkart.varidhi.RelayerModule;
import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.PartitionConfiguration;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.utils.DBBaseTest;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.OutboundPartitionRepository;
import com.flipkart.varidhi.repository.OutboundPartitionRepositoryImpl;
import com.flipkart.varidhi.repository.PartitionAlterMonitor;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Created by ashudeep.sharma on 11/10/16.
 */
public class TestSidelinedMessages extends DBBaseTest {

    @Test
    public void TestHasSideLinedMessages() {
        ApplicationConfiguration applicationConfiguration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        String relayerName = applicationConfiguration.getRelayers().get(0).getName();
        String appDatabase = applicationConfiguration.getRelayers().get(0).getAppDbRef().getId();
        String hibernateConnectionURL =
                applicationConfiguration.getMysql().get("app").get("hibernate.connection.url")
                        .toString();
        String dbConnectionURL =
                hibernateConnectionURL.substring(0, hibernateConnectionURL.lastIndexOf('/') + 1)
                        + "?user=" + applicationConfiguration.getMysql().get("app").get("hibernate.connection.username").toString()
                        + "&password=" + applicationConfiguration.getMysql().get("app").get("hibernate.connection.password");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String messagesInsertQuery1 =
                "INSERT INTO " + relayerName + "_messages(id,message_id,created_at) values(10,'10','"
                        + formatter.format(DateTime.now().toDate()) + "')";
        String messagesInsertQuery2 = "INSERT INTO " + relayerName
                + "_messages(id,message_id,created_at) values(1001,'1001','" + formatter
                .format(DateTime.now().minusMinutes(40).toDate()) + "')";
        String messagesInsertQuery3 = "INSERT INTO " + relayerName
                + "_messages(id,message_id,created_at) values(2001,'2001','" + formatter
                .format(DateTime.now().minusMinutes(25).toDate()) + "')";

        String sideLinedMessageInsertQuery1 = "INSERT INTO " + relayerName
                + "_sidelined_messages(id,message_id,status,created_at) values(10,'10','SIDELINED','"
                + formatter.format(DateTime.now().toDate()) + "')";
        String sideLinedMessageInsertQuery2 = "INSERT INTO " + relayerName
                + "_sidelined_messages(id,message_id,status,created_at) values(1001,'1001','UNSIDELINED','"
                + formatter.format(DateTime.now().minusMinutes(40).toDate()) + "')";
        String sideLinedMessageInsertQuery3 = "INSERT INTO " + relayerName
                + "_sidelined_messages(id,message_id,status,created_at) values(2001,'2001','PROCESSING','"
                + formatter.format(DateTime.now().minusMinutes(25).toDate()) + "')";
        ArrayList<String> queryList = new ArrayList<>();
        queryList.add(messagesInsertQuery1);
        queryList.add(messagesInsertQuery2);
        queryList.add(messagesInsertQuery3);
        queryList.add(sideLinedMessageInsertQuery1);
        queryList.add(sideLinedMessageInsertQuery2);
        queryList.add(sideLinedMessageInsertQuery3);

        try {
            System.out.println("Proceeding with Relayer :" + relayerName);
            ExchangeTableNameProvider exchangeTableNameProvider =
                    new ExchangeTableNameProvider(relayerName);
            RelayerModule relayerModule = new RelayerModule();
            InitializeRelayer.setRelayerMetadata(relayerName, appDatabase, dbConnectionURL);
            InitializeRelayer.setupRelayer();
            InitializeRelayer.executeQueries(queryList);
            SessionFactoryContainer sessionFactoryContainer =
                    relayerModule.provideSessionFactoryContainer(applicationConfiguration);
            PartitionConfiguration partitionConfiguration = applicationConfiguration.getPartitionConfiguration();
            OutboundPartitionRepository outboundPartitionRepository =
                    new OutboundPartitionRepositoryImpl(
                            sessionFactoryContainer.getSessionFactory(appDatabase),
                            exchangeTableNameProvider, partitionConfiguration, null, new PartitionAlterMonitor(partitionConfiguration.getMonitorThreadSleepTime(),partitionConfiguration.getDeadlockQueryExecutionTime(), 3, 10000L));

            Assert.assertEquals(false, outboundPartitionRepository.hasSidelinedMessages(1L, 9L));
            Assert.assertEquals(true, outboundPartitionRepository.hasSidelinedMessages(1L, 1000L));
            Assert
                    .assertEquals(true, outboundPartitionRepository.hasSidelinedMessages(1001L, 2000L));
            Assert
                    .assertEquals(true, outboundPartitionRepository.hasSidelinedMessages(2001L, 3000L));
            Assert.assertEquals(true, outboundPartitionRepository.hasSidelinedMessages(1L, 2000L));
            Assert.assertEquals(false,
                    outboundPartitionRepository.hasSidelinedMessages(2500L, 5000L));
        } catch (SQLException sqlException) {
            Assert.fail("Failed to Initialize Relayer" + sqlException.toString());
        }
    }

    @After
    public void cleanup() throws SQLException {
        InitializeRelayer.cleanup();
    }
}
