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

import com.flipkart.varidhi.RelayerModule;
import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.core.HttpAuthenticationService;
import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.relayer.InitializeRelayer;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class ApplicationRepositoryImplTest extends DBBaseTest {
    Session session;
    ExchangeTableNameProvider exchangeTableNameProvider;
    Transaction transaction;
    ApplicationRepository applicationRepository;

    @Before
    public void setup() {

        ApplicationConfiguration configuration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        RelayerModule relayerModule = new RelayerModule();

        SessionFactoryContainer sessionFactoryContainer = relayerModule.provideSessionFactoryContainer(configuration);

        this.exchangeTableNameProvider = new ExchangeTableNameProvider(configuration.getRelayers().get(0).getName());

        HttpAuthenticationService httpAuthenticationService = new HttpAuthenticationService(configuration.getHttpAuthConfig());
        //creating all the tables in the db
        RelayerHandleContainer relayerHandleContainer = relayerModule.provideRelayerHandleContainer(sessionFactoryContainer, configuration, httpAuthenticationService);

        ReaderOutboundRepository readerOutboundRepository = new OutboundRepositoryImpl(
                sessionFactoryContainer.getSessionFactory(configuration.getRelayers().get(0).getOutboundDbRef().getId()),
                this.exchangeTableNameProvider);
        this.applicationRepository = new ApplicationRepositoryImpl(
                sessionFactoryContainer.getSessionFactory(configuration.getRelayers().get(0).getAppDbRef().getId()),
                this.exchangeTableNameProvider);

        this.session = sessionFactoryContainer.getSessionFactory(configuration.getRelayers().get(0).getAppDbRef().getId()).openSession();
    }


    @After
    public void destroyTables(){
        transaction = session.beginTransaction();
        Query deleteTablesQuery = session.createSQLQuery("DROP TABLE "
                +exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
        );
        deleteTablesQuery.executeUpdate();
        transaction.commit();
        session.disconnect();
        session.close();
    }

    @Test
    public void getMessageMetaDataTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1205, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-23 19:04:20') , "
                + " (1206, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-23 19:04:22') ,"
                + " (1207, '4cb9a0a5-107a-45e4-84d8-988c9djs7', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            List<AppMessageMetaData> resultList = applicationRepository.getMessageMetaData((long) 1205, 2);

            Assert.assertEquals(2,resultList.size());

        } catch (Exception e) {
            fail("error is in getMessageMetaDataTest " + e.getMessage());
        }
    }

    @Test
    public void getMessageMetaDataTest2() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1205, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-23 19:04:20') , "
                + " (1206, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-23 19:04:22') ,"
                + " (1207, '4cb9a0a5-107a-45e4-84d8-988c9djs7', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Map<Long, AppMessageMetaData> resultList = applicationRepository.getMessageMetaData(Arrays.asList(1206L, 1205L, 1207L), 3);

            Assert.assertEquals(resultList.size(), 3);
        } catch (Exception e) {
            fail("error is in getMessageMetaDataTest2 " + e.getMessage());
        }
    }


    @Test
    public void messagesExistForFurtherOffsetTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1208, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-23 19:04:20') , "
                + " (1206, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-23 19:04:22') ,"
                + " (1207, '4cb9a0a5-107a-45e4-84d8-988c9djs7', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            AppMessageMetaData appMessageMetaData = applicationRepository.messagesExistForFurtherOffset(1205L, 0);
            Assert.assertEquals(1206, appMessageMetaData.getId());
        } catch (Exception e) {
            fail("the  error is in messagesExistForFurtherOffsetTest" + e.getMessage());
        }
    }

    @Test
    public void getMinMessageFromMessagesTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1208, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-23 19:04:20') , "
                + " (1206, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-23 19:04:22') ,"
                + " (1207, '4cb9a0a5-107a-45e4-84d8-988c9djs7', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            AppMessageMetaData appMessageMetaData = applicationRepository.getMinMessageFromMessages(Arrays.asList("4cb9a0a5-107a-45e4-84d8-988c9djs5", "4cb9a0a5-107a-45e4-84d8-988c9djs6"));
            Assert.assertEquals(appMessageMetaData.getId(), 1206);
        } catch (Exception e) {
            fail("the  error is in messagesExistForFurtherOffsetTest" + e.getMessage());
        }
    }

    @Test
    public void processorsIdMapTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1, '1201', '2018-05-23 19:04:20') , "
                + " (2, '1202', '2018-05-23 19:04:22') ,"
                + " (3, '1203', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            HashMap<String, String> tempMap = new HashMap<>();
            tempMap.put("processId1", "1201");
            tempMap.put("processId2", null);
            HashMap<String, Long> result = applicationRepository.processorsIdMap(tempMap);
            Assert.assertEquals(result.size(), 1);
            result = applicationRepository.processorsIdMap(null);
            Assert.assertEquals(result.size(), 0);
        } catch (Exception e) {
            fail("the  error is in messagesExistForFurtherOffsetTest" + e.getMessage());
        }
    }

    @Test
    public void getMessageFromMessagesTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA)
                + " VALUES "
                + " (1, '1201', '2018-05-23 19:04:20') , "
                + " (2, '1202', '2018-05-23 19:04:22') ,"
                + " (3, '1203', '2018-05-23 19:04:21')");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            List<String> tempList = new ArrayList<>();
            tempList.add("1201");
            tempList.add("1203");

            List<AppMessageMetaData> result = applicationRepository.getMessageMetaData(tempList);
            Assert.assertEquals(result.size(), 2);

        } catch (Exception e) {
            fail("the  error is in messagesExistForFurtherOffsetTest" + e.getMessage());
        }
    }


}