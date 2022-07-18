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
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.utils.DBBaseTest;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.OutboundRepository;
import com.flipkart.varidhi.repository.OutboundRepositoryImpl;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashudeep.sharma on 10/10/16.
 */
public class TestSkippedIds extends DBBaseTest{
    @Test
    public void TestSkippedIDs() {
        ApplicationConfiguration applicationConfiguration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        String relayerName = applicationConfiguration.getRelayers().get(0).getName();
        String appDatabase = applicationConfiguration.getRelayers().get(0).getAppDbRef().getId();
        String hibernateConnectionURL =
                applicationConfiguration.getMysql().get("app").get("hibernate.connection.url")
                        .toString();
        String dbConnectionURL =
                hibernateConnectionURL.substring(0, hibernateConnectionURL.lastIndexOf('/') + 1)
                        + "?user=" + applicationConfiguration.getMysql().get("app")
                        .get("hibernate.connection.username").toString()
                        + "&password=" + applicationConfiguration.getMysql().get("app").get("hibernate.connection.password");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String skippedIdInsertQuery1 = "INSERT INTO " + relayerName
                + "_skipped_ids(id,message_seq_id,created_at,status) values(2,2,'" + formatter
                .format(DateTime.now().toDate()) + "','NEW')";
        String skippedIdInsertQuery2 = "INSERT INTO " + relayerName
                + "_skipped_ids(id,message_seq_id,created_at,status) values(1001,1001,'" + formatter
                .format(DateTime.now().minusMinutes(40).toDate()) + "','NEW')";
        String skippedIdInsertQuery3 = "INSERT INTO " + relayerName
                + "_skipped_ids(id,message_seq_id,created_at,status) values(2001,2001,'" + formatter
                .format(DateTime.now().minusMinutes(25).toDate()) + "','NEW')";
        ArrayList<Long> verifyMessage_Seq_IdList = new ArrayList<>();
        verifyMessage_Seq_IdList.add(2L);
        verifyMessage_Seq_IdList.add(2001L);
        ArrayList<String> queryList = new ArrayList<>();
        queryList.add(skippedIdInsertQuery1);
        queryList.add(skippedIdInsertQuery2);
        queryList.add(skippedIdInsertQuery3);
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
            OutboundRepository outboundPartitionRepository =
                    new OutboundRepositoryImpl(sessionFactoryContainer.getSessionFactory(appDatabase),
                            exchangeTableNameProvider);
            List<Long> skippedAppSeqIdList =
                    outboundPartitionRepository.readSkippedAppSequenceIds(30, new Timestamp(System.currentTimeMillis()));

            Assert.assertArrayEquals(verifyMessage_Seq_IdList.toArray(),
                    skippedAppSeqIdList.toArray());
        } catch (SQLException sqlException) {
            Assert.fail("Failed to Initialize Relayer" + sqlException.toString());
        }
    }

}
