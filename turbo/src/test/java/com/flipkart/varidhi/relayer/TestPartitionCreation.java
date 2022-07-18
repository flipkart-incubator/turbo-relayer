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
import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.partitionManager.PartitionCreator;
import com.flipkart.varidhi.relayer.reader.TurboReadMode;
import com.flipkart.varidhi.repository.*;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;

/**
 * Created by ashudeep.sharma on 21/10/16.
 */
public class TestPartitionCreation extends DBBaseTest {

    @Test
    public void TestCreatePartitions() {
        //creating readerOutboundRepository object to perform test taking the config from test.yml
        ApplicationConfiguration applicationConfiguration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        String relayerName = applicationConfiguration.getRelayers().get(0).getName();
        String appDatabase = applicationConfiguration.getRelayers().get(0).getAppDbRef().getId();
        String hibernateConnectionURL =
                applicationConfiguration.getMysql().get("app").get("hibernate.connection.url")
                        .toString();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dbConnectionURL =
                hibernateConnectionURL.substring(0, hibernateConnectionURL.lastIndexOf('/') + 1)
                        + "?user=" + applicationConfiguration.getMysql().get("app")
                        .get("hibernate.connection.username").toString()+
                        "&password="+applicationConfiguration.getMysql().get("app").get("hibernate.connection.password").toString();
        String partitionCreationQuery1 = "ALTER TABLE " + relayerName
                + "_message_meta_data ADD PARTITION (PARTITION p31 VALUES LESS THAN (31000));";
        String partitionCreationQuery2 = "ALTER TABLE " + relayerName
                + "_message_meta_data ADD PARTITION (PARTITION p32 VALUES LESS THAN (32000));";
        String partitionCreationQuery3 = "ALTER TABLE " + relayerName
                + "_skipped_ids ADD PARTITION (PARTITION p31 VALUES LESS THAN (31000));";
        String messagesInsertQuery = "INSERT INTO " + relayerName
                + "_messages(id,message_id,created_at) values(2001,'2001','" + formatter
                .format(DateTime.now().minusMinutes(25).toDate()) + "')";
        ArrayList<String> queryList = new ArrayList<>();
        queryList.add(partitionCreationQuery1);
        queryList.add(partitionCreationQuery2);
        queryList.add(partitionCreationQuery3);
        queryList.add(messagesInsertQuery);

        try {
            System.out.println("Proceeding with Relayer :" + relayerName);
            ExchangeTableNameProvider exchangeTableNameProvider =
                    new ExchangeTableNameProvider(relayerName);
            RelayerModule relayerModule = new RelayerModule();
            InitializeRelayer.setRelayerMetadata(relayerName, appDatabase, dbConnectionURL);
            InitializeRelayer.setupRelayer();
            InitializeRelayer.executeQueries(queryList);
            //applicationConfiguration.getPartitionConfiguration().setNoOfExtraPartitions(32);
            SessionFactoryContainer sessionFactoryContainer =
                    relayerModule.provideSessionFactoryContainer(applicationConfiguration);
            PartitionAlterMonitor partitionAlterMonitor = new PartitionAlterMonitor(applicationConfiguration.getPartitionConfiguration().getMonitorThreadSleepTime(),
                    applicationConfiguration.getPartitionConfiguration().getDeadlockQueryExecutionTime(), 3, 10000L);
            OutboundPartitionRepository outboundPartitionRepository =
                    new OutboundPartitionRepositoryImpl(
                            sessionFactoryContainer.getSessionFactory(appDatabase),
                            exchangeTableNameProvider, applicationConfiguration.getPartitionConfiguration(), null, partitionAlterMonitor);
            ApplicationPartitionRepository applicationPartitionRepository =
                    new ApplicationPartitionRepositoryImpl(
                            sessionFactoryContainer.getSessionFactory(appDatabase),
                            exchangeTableNameProvider, applicationConfiguration.getPartitionConfiguration(), null, partitionAlterMonitor);
            PartitionCreator partitionCreator =
                    new PartitionCreator(32, 1000,applicationPartitionRepository,
                            outboundPartitionRepository, TurboReadMode.DEFAULT, PartitionMode.AUTO);
            partitionCreator.createPartitions();

            Assert.assertEquals(34000L, applicationPartitionRepository.getPartitionMaxId());
            Assert.assertEquals(34000L, outboundPartitionRepository
                    .getPartitionMaxId(ExchangeTableNameProvider.TableType.MESSAGE));
            Assert.assertEquals(34000L, outboundPartitionRepository
                    .getPartitionMaxId(ExchangeTableNameProvider.TableType.SKIPPED_IDS));
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            Assert.fail("Failed to Initialize Relayer" + sqlException.toString());
        }
    }

    @After
    public void cleanup() throws SQLException {
        InitializeRelayer.cleanup();
    }
}
