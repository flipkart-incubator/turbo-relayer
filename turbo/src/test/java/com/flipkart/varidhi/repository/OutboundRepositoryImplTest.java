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
import com.flipkart.varidhi.relayer.RelayerOutboundRepository;
import com.flipkart.varidhi.relayer.common.ControlTaskStatus;
import com.flipkart.varidhi.relayer.common.SidelineReasonCode;
import com.flipkart.varidhi.relayer.common.SidelinedMessageStatus;
import com.flipkart.varidhi.relayer.common.SkippedIdStatus;
import com.flipkart.varidhi.relayer.processor.ProcessorOutboundRepository;
import com.flipkart.varidhi.relayer.reader.models.AppMessageMetaData;
import com.flipkart.varidhi.relayer.reader.models.ControlTask;
import com.flipkart.varidhi.relayer.reader.models.Message;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.hibernate.*;
import org.hibernate.type.LongType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.*;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OutboundRepositoryImplTest extends DBBaseTest {

    public Transaction transaction;
    public Session session;
    public ReaderOutboundRepository readerOutboundRepository;
    public ExchangeTableNameProvider exchangeTableNameProvider;
    public ProcessorOutboundRepository processorOutboundRepository;
    public OutboundRepository processorMetaDataRepository;

    public RelayerOutboundRepository relayerOutboundRepository;
    private boolean isSetupDone = false;

    @Before
    public void setup() throws Exception {
        //creating readerOutboundRepository object to perform test taking the config
        if(!this.isSetupDone) {
            setupRelayer();
            this.isSetupDone = true;
        }
    }

    private void setupRelayer() {
        ApplicationConfiguration configuration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");

        RelayerModule relayerModule = new RelayerModule();
        SessionFactoryContainer sessionFactoryContainer = relayerModule.provideSessionFactoryContainer(configuration);
        ExchangeTableNameProvider exchangeTableNameProvider = new ExchangeTableNameProvider(configuration.getRelayers().get(0).getName());

        HttpAuthenticationService httpAuthenticationService = new HttpAuthenticationService(configuration.getHttpAuthConfig());
        //creating all the tables in the db
        RelayerHandleContainer relayerHandleContainer = relayerModule.provideRelayerHandleContainer(sessionFactoryContainer, configuration, httpAuthenticationService);

        SessionFactory outboundSessionFactory = sessionFactoryContainer.getSessionFactory(configuration.getRelayers().get(0).getOutboundDbRef().getId());

        ReaderOutboundRepository readerOutboundRepository = new OutboundRepositoryImpl(outboundSessionFactory, exchangeTableNameProvider);
        ProcessorOutboundRepository processorOutboundRepository = new OutboundRepositoryImpl(outboundSessionFactory, exchangeTableNameProvider);
        OutboundRepository processorMetaDataRepository = new OutboundRepositoryImpl(outboundSessionFactory, exchangeTableNameProvider);
        RelayerOutboundRepository relayerOutboundRepository = new OutboundRepositoryImpl(outboundSessionFactory, exchangeTableNameProvider);

        this.processorMetaDataRepository = processorMetaDataRepository;
        this.processorOutboundRepository = processorOutboundRepository;
        this.session = outboundSessionFactory.openSession();
        this.readerOutboundRepository = readerOutboundRepository;
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.relayerOutboundRepository = relayerOutboundRepository;
    }

    @After
    public void destroyTables() {
        transaction = session.beginTransaction();
        Query deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES));
        deleteTablesQuery.executeUpdate();
        deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE));
        deleteTablesQuery.executeUpdate();
        deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                +  exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS));
        deleteTablesQuery.executeUpdate();
        deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                +  exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS));
        deleteTablesQuery.executeUpdate();
        deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE));
        deleteTablesQuery.executeUpdate();
        deleteTablesQuery = session.createSQLQuery("truncate TABLE "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_GROUPS));
        deleteTablesQuery.executeUpdate();
        transaction.commit();
        session.disconnect();
        session.close();
    }

    @Test
    public void readMessagesTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1205, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1206, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Map<String, Message> result = readerOutboundRepository.readMessages(Arrays.asList("4cb9a0a5-107a-45e4-84d8-988c9djs5", "4cb9a0a5-107a-45e4-84d8-988c9djs6"));
            Assert.assertEquals(result.size(), 2);

            Map<String, Message> resultWithNullList = readerOutboundRepository.readMessages(null);
            Assert.assertEquals(resultWithNullList.size(), 0);

            // readMessages should eat up the exception
            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.readMessages(null);

        } catch (Exception e) {
            fail("error is in readMessagesTest " + e.getMessage());
        }
    }

    @Test
    public void readMessagesUsingSequenceIdsTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1207, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1208, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            List< Message> result = readerOutboundRepository.readMessagesUsingSequenceIds(Arrays.asList(1207L, 1208L));
            Assert.assertEquals(result.size(), 2);

            List< Message> resultWithNullList = readerOutboundRepository.readMessagesUsingSequenceIds(null);
            Assert.assertEquals(resultWithNullList.size(), 0);

            List< Message> resultWithEmptyList = readerOutboundRepository.readMessagesUsingSequenceIds(new ArrayList<>());
            Assert.assertEquals(resultWithEmptyList.size(), 0);

            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.readMessagesUsingSequenceIds(null);


        } catch (Exception e) {
            fail("error is in readMessagesUsingSequenceIdsTest " + e.getMessage());
        }
    }


    @Test
    public void readMessagesUsingSequenceIdsWithLimitTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1209, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1210, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            List<Message> result = readerOutboundRepository.readMessagesUsingSequenceIds(Arrays.asList(1209L,1210L),1);
            Assert.assertEquals(result.size(), 1);

            List<Message> resultWithNullList = readerOutboundRepository.readMessagesUsingSequenceIds(null,1);
            Assert.assertEquals(resultWithNullList.size(), 0);

            List<Message> resultWithEmptyList = readerOutboundRepository.readMessagesUsingSequenceIds(new ArrayList<>(),1);
            Assert.assertEquals(resultWithEmptyList.size(), 0);

            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.readMessagesUsingSequenceIds(null,1);

        } catch (Exception e) {
            fail("error is in readMessagesUsingSequenceIdsWithLimitTest " + e.getMessage());
        }
    }

    @Test
    public void readMessagesWithDelayedRelayTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1211, '4cb9a0a5-107a-45e4-84d8-988c9djs5', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1212, '4cb9a0a5-107a-45e4-84d8-988c9djs6', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
    }


    @Test
    public void messagesExistForFurtherOffsetTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + " (1213,'4cb9a0a5-107a-45e4-84d8-988c9djs5', '{}', NOW() - INTERVAL 1 DAY, 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1214,'4cb9a0a5-107a-45e4-84d8-988c9djs5', '{}', NOW() + INTERVAL 1 DAY, 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) ");

        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            try {
                readerOutboundRepository.messagesExistForFurtherOffset(null,0);
                fail("IllegalArgumentException should occur");
            } catch (IllegalArgumentException e){
            }


            AppMessageMetaData metaData = readerOutboundRepository.messagesExistForFurtherOffset(1213L,1);
            Assert.assertNotNull(metaData);

            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.messagesExistForFurtherOffset(1213L,1);

        } catch (Exception e) {
            fail("error is in messagesExistForFurtherOffsetTest " + e.getMessage());
        }
    }

    @Test
    public void getMessageMetaDataTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1215, '1215', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1216, '1216', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            List<AppMessageMetaData> metaDataList = readerOutboundRepository.getMessageMetaData(Arrays.asList(new String[]{"1215", "1216"}));
            Assert.assertEquals(2, metaDataList.size());

            List< AppMessageMetaData> resultWithNullList = readerOutboundRepository.getMessageMetaData(null);
            Assert.assertEquals(0,resultWithNullList.size());

            List< AppMessageMetaData> resultWithEmptyList = readerOutboundRepository.getMessageMetaData(new ArrayList<>());
            Assert.assertEquals(0,resultWithEmptyList.size());

            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.getMessageMetaData(null);

        } catch (Exception e) {
            fail("error is in getMessageMetaDataTest " + e.getMessage());
        }
    }


    @Test
    public void getMessageMetaDataWithDelayedRelayTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1217, '1217', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1218, '1218', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            List<AppMessageMetaData> metaDataList = readerOutboundRepository.getMessageMetaData(1217L,2,1);
            Assert.assertEquals(2, metaDataList.size());

            // throw and eat up IllegalArgumentException
            readerOutboundRepository.getMessageMetaData(null,0,0);

            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.getMessageMetaData(1217L,2,0);

        } catch (Exception e) {
            fail("error is in readMessagesUsingSequenceIdsTest " + e.getMessage());
        }
    }

    @Test
    public void processorsIdMapTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1219, '1219', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1220, '1220', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            HashMap<String,String> map = new HashMap<>();
            map.put("1219","1219");
            map.put("-1","-1");
            HashMap<String ,Long> processorsIdMap = readerOutboundRepository.processorsIdMap(map);
            Assert.assertEquals(1, processorsIdMap.size());

            // throw and eat up IllegalArgumentException
            readerOutboundRepository.processorsIdMap(map);

            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.processorsIdMap(new HashMap<>());

        } catch (Exception e) {
            fail("error is in processorsIdMapTest " + e.getMessage());
        }
    }

    @Test
    public void getMinMessageFromMessagesTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1221, '1221', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1222, '1222', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            AppMessageMetaData metaDataOnNull = readerOutboundRepository.getMinMessageFromMessages(null);
            Assert.assertEquals(0L, metaDataOnNull.getId());

            AppMessageMetaData metaDataWithList = readerOutboundRepository.getMinMessageFromMessages(Arrays.asList(new String[]{"1221","1222"}));
            Assert.assertEquals(1221, metaDataWithList.getId());

            // skipping test case of exception as it does a System.exit
            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);

        } catch (Exception e) {
            fail("error is in getMinMessageFromMessagesTest " + e.getMessage());
        }
    }

    @Test
    //before running these test cases the databases (outbound) should be empty
    public void readUnsidelinedMessageIdsTest() {

        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        //using the id value as 1
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)"
                + "VALUES"
                + "(1, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'UNSIDELINED', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, '');");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            //Assuming the database does not contain any other unsidelined message and the only unsildelined message is the dummy message that is inserted above
            List<String> unsidelinedMessageIds = readerOutboundRepository.readUnsidelinedMessageIds(5);
            Assert.assertEquals(unsidelinedMessageIds.size(), 1);

            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.readUnsidelinedMessageIds(0);

        } catch (Exception e) {
            fail("failing error" + e);
        }
    }

    @Test
    public void updateSidelinedMessageStatusTestWithExpectedInp() {

        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)"
                + " VALUES"
                + " ( 1, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'UNSIDELINED', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, '');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            readerOutboundRepository.updateSidelinedMessageStatus(Arrays.asList("4cb9a0a5-107a-45e4-84d8-988c9djs6"), SidelinedMessageStatus.PROCESSING);
            //query to check the contents fo the updated data
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9djs6' "
            );
            List<String> status = query.list();
            for (String stat : status) {
                Assert.assertEquals("PROCESSING", stat);
            }

            // throw and eat up IllegalArgumentException and logging proper error message
            readerOutboundRepository.updateSidelinedMessageStatus(null,null);

        } catch (Exception e) {
            fail("error occured: " + e);
        }
    }

    @Test
    public void updateDestinationResponseStatusTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1223, '1223', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1224, '1224', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorOutboundRepository.updateDestinationResponseStatus("1223",201);
            String savedStatus = readerOutboundRepository.readMessages(Arrays.asList(new String[]{"1223"})).get("1223").getDestinationResponseStatus();
            Assert.assertEquals(Integer.parseInt(savedStatus),201);

            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.updateDestinationResponseStatus("1223",201);

        } catch (Exception e) {
            fail("error is in updateDestinationResponseStatusTest " + e.getMessage());
        }
    }

    @Test
    public void readControlTasksTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(1, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            //reading the dummy data entered who had status as NEW
            List<ControlTask> controlTaskList = readerOutboundRepository.readControlTasks(5);
            Assert.assertEquals(controlTaskList.size(), 1);
        } catch (Exception e) {
            fail("failing error" + e);
        }

    }

    @Test
    public void updateControlTaskStatusTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(1, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00')," +
                "(2, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djsk', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs7', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            //reading the dummy data entered who had status as NEW
            List<ControlTask> controlTasks = readerOutboundRepository.readControlTasks(2);
            readerOutboundRepository.updateControlTaskStatus(controlTasks, ControlTaskStatus.DONE);

            Query query = session.createSQLQuery("select status from "
                    + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs7' or message_id ='4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6' ");

            List<String> status = query.list();
            for (String stat : status) {
                Assert.assertEquals(stat, "DONE");
            }

            // eat up exceptions and null cases
            readerOutboundRepository.updateControlTaskStatus(null,null);
            readerOutboundRepository.updateControlTaskStatus(new ArrayList<>(),null);

        } catch (Exception e) {
            Assert.fail("error occured in updateControlTaskStatusTest");
        }

    }

    @Test
    public void persistSkippedIDsTest() {
        try {
            readerOutboundRepository.persistSkippedIds(Arrays.asList(1L));

            Query query = session.createSQLQuery("select id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS)
                    + " where id = 1").addScalar("id", LongType.INSTANCE);

            List<Long> result = query.list();

            for (long response : result) {
                Assert.assertEquals(response, 1);
            }
        } catch (Exception e) {
            fail("error occured in persistSkippedIDsTest" + e.getMessage());
        }

    }

    @Test
    public void readSkippedAppSequenceIdsTest() {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            List<Long> list = readerOutboundRepository.readSkippedAppSequenceIds(Long.MAX_VALUE,now);
            Assert.assertEquals(0, list.size());

            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.readSkippedAppSequenceIds(Long.MAX_VALUE,now);

        } catch (Exception e) {
            fail("error occured in readSkippedAppSequenceIdsTest" + e.getMessage());
        }
    }

    @Test
    public void updateSkippedIdStatusTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                " VALUES " +
                " (1, 1, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03')," +
                "(2, 2, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            readerOutboundRepository.updateSkippedIdStatus(Arrays.asList(1L, 2L), SkippedIdStatus.PROCESSING);
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                    " where status = 'PROCESSING' ");
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "PROCESSING");
            }

            // null cases
            readerOutboundRepository.updateSkippedIdStatus(null,null);
            readerOutboundRepository.updateSkippedIdStatus(Arrays.asList(1L, 2L),null);

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.updateSkippedIdStatus(new ArrayList<>(),SkippedIdStatus.NEW);

        } catch (Exception e) {
            fail("error occured in updateSkippedIdStatusTest" + e.getMessage());
        }
    }


    @Test
    public void getLastProcessedMessageidsTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES"
                + "(1, 'process_id_1', '1', NOW(), NOW(), 1),"
                + "(2, 'process_id_2', '2', NOW(), NOW(), 1)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Assert.assertEquals(2,readerOutboundRepository.getLastProcessedMessageids().size());

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.getLastProcessedMessageids();

        } catch (Exception e) {
            fail("error is in updateDestinationResponseStatusTest " + e.getMessage());
        }
    }


    @Test
    public void getMessageSequenceIdsTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MESSAGE)
                + " VALUES "
                + "(1225, 'message_1225', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL) "
                + " ,(1226, 'message_1226', '{\\\"clientId\\\":\\\"flipkart\\\",\\\"clientServiceRequestBundleId\\\":\\\"FA579547276\\\",\\\"serviceRequests\\\":[{\\\"clientServiceRequestId\\\":\\\"b2c_5609948301#FA579547276\\\",\\\"serviceRequestType\\\":\\\"FA_FORWARD_E2E_EKART\\\",\\\"serviceRequestVersion\\\":\\\"1.0.0\\\",\\\"tier\\\":\\\"REGULAR\\\",\\\"serviceStartDate\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"serviceCompletionDate\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"serviceRequestHold\\\":false,\\\"serviceRequestData\\\":{\\\"source\\\":{\\\"locationId\\\":\\\"blr_wfld\\\",\\\"type\\\":\\\"ekart-facility\\\"},\\\"destination\\\":{\\\"locationId\\\":\\\"CNTCT18B92C84F3694DAC9EEFB7202\\\",\\\"type\\\":\\\"customer-location\\\"},\\\"customer\\\":{\\\"customerId\\\":\\\"ACC11AC092587C54785849DB88177EB8089T\\\",\\\"type\\\":\\\"client-customer\\\"},\\\"items\\\":[{\\\"itemIdentifier\\\":{\\\"uniqueID\\\":\\\"b2c_5609948301\\\",\\\"batchID\\\":\\\"P9_13Jan_18366589\\\",\\\"ownerID\\\":\\\"19e4d415fc1f40da\\\",\\\"groupID\\\":\\\"BAGEQ8DUQHPRRD2W\\\"},\\\"quantity\\\":1,\\\"seller\\\":{\\\"sellerId\\\":\\\"19e4d415fc1f40da\\\"},\\\"itemSize\\\":\\\"small\\\",\\\"itemAttributes\\\":[{\\\"name\\\":\\\"order_item_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"579547276\\\"},{\\\"name\\\":\\\"order_id\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"OD601497111501849000\\\"},{\\\"name\\\":\\\"hand_in_hand\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"ekl.ekl_databag\\\",\\\"type\\\":\\\"json\\\",\\\"value\\\":\\\"{\\\\\\\"sla\\\\\\\":2880,\\\\\\\"shipment_movement_type\\\\\\\":\\\\\\\"inter_zone\\\\\\\",\\\\\\\"cost\\\\\\\":210,\\\\\\\"available\\\\\\\":true,\\\\\\\"slot_ref_id\\\\\\\":\\\\\\\"800036\\\\\\\",\\\\\\\"source_id\\\\\\\":\\\\\\\"421302\\\\\\\",\\\\\\\"tags\\\\\\\":{\\\\\\\"coc_code\\\\\\\":\\\\\\\"SAT/CHP\\\\\\\",\\\\\\\"route_code\\\\\\\":\\\\\\\"CNP\\\\\\\",\\\\\\\"slot_time\\\\\\\":\\\\\\\"2017-06-16T05:00:00+0530 - 2017-06-16T13:00:00+0530\\\\\\\"},\\\\\\\"source_epst\\\\\\\":\\\\\\\"2017-06-15 04:00:00\\\\\\\",\\\\\\\"source_lpst\\\\\\\":\\\\\\\"2017-06-15 14:00:00\\\\\\\",\\\\\\\"start_time\\\\\\\":\\\\\\\"2017-06-16 05:00:00\\\\\\\",\\\\\\\"end_time\\\\\\\":\\\\\\\"2017-06-16 13:00:00\\\\\\\",\\\\\\\"booking_ref_id\\\\\\\":\\\\\\\"FU_9033\\\\\\\",\\\\\\\"na_reason\\\\\\\":null,\\\\\\\"lpe_tier\\\\\\\":\\\\\\\"REGULAR\\\\\\\",\\\\\\\"attributes\\\\\\\":[\\\\\\\"dangerous\\\\\\\"],\\\\\\\"lpe_reference\\\\\\\":{},\\\\\\\"lpe_ref_id\\\\\\\":\\\\\\\"b2c_1300628\\\\\\\",\\\\\\\"order_item_unit_id\\\\\\\":\\\\\\\"6149711150184910000\\\\\\\",\\\\\\\"location_type_tag\\\\\\\":\\\\\\\"Home\\\\\\\",\\\\\\\"alternate_phone\\\\\\\":\\\\\\\"9876543210\\\\\\\",\\\\\\\"is_d_plus_i\\\\\\\":false,\\\\\\\"supply_chain_type\\\\\\\":\\\\\\\"GROCERY\\\\\\\"}\\\"}]}],\\\"deliveryVendor\\\":{\\\"vendorCode\\\":\\\"flipkartlogistics\\\",\\\"name\\\":\\\"flipkartlogistics\\\",\\\"type\\\":\\\"EKL_SLOTTED\\\"},\\\"handoverSlot\\\":{\\\"start\\\":\\\"2016-06-15T05:00:00+05:30\\\",\\\"end\\\":\\\"2017-06-15T15:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"deliverySlot\\\":{\\\"start\\\":\\\"2018-04-24T19:49:19+05:30\\\",\\\"end\\\":\\\"2018-06-16T13:00:00+05:30\\\",\\\"type\\\":\\\"soft\\\"},\\\"serviceRequestAttributes\\\":[{\\\"name\\\":\\\"compliance_forms_required\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"true\\\"},{\\\"name\\\":\\\"gift_wrap\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"sales_channel\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"AndroidApp\\\"},{\\\"name\\\":\\\"breach_cost\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"20000\\\"},{\\\"name\\\":\\\"company\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"b2b\\\"},{\\\"name\\\":\\\"is_replacement\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"deliver_after_capability\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"false\\\"},{\\\"name\\\":\\\"lpht\\\",\\\"type\\\":\\\"string\\\",\\\"value\\\":\\\"2017-06-15T14:00:00+05:30\\\"}]}}],\\\"constraints\\\":[{\\\"clientConstraintId\\\":\\\"47caa55e4d93addae2c392c786876a84\\\",\\\"constraintType\\\":\\\"ship-together\\\",\\\"clientServiceRequestReferences\\\":{\\\"b2c_5609948301#FA579547276\\\":\\\"soft\\\"}}]}', '2018-05-04 16:55:43', 'ekart_e2e_adapter_sal_requests_preprod', 'queue', 'ekart-e2e-adapter', '', 'POST', 'http://10.85.36.18:80/v1/service-request-bundle', NULL, '', '', '', '', '2018-05-30 12:21:06', '{\\\"X-User-Id\\\":523629,\\\"X-EKART-CLIENT\\\":\\\"flipkart\\\",\\\"X-EKART-DATE\\\":1524579572,\\\"Content-Type\\\":\\\"application/json\\\",\\\"X-EKART-SERVICE-REQUEST-BUNDLE-ID\\\":\\\"FA579547276\\\",\\\"X-EKART-Authorization\\\":\\\"2g6A8zGovjHyVNNxX723V+cXFMJZPvc8CQ04w3aixqc\\\\u003d\\\"}', 'TXN-4cb9a0a5-107a-45e4-84d8-988c9ccefb3d', NULL, NULL)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Assert.assertEquals(0,readerOutboundRepository.getMessageSequenceIds(null).size());
            Assert.assertEquals(0,readerOutboundRepository.getMessageSequenceIds(new ArrayList<>()).size());

            List<Long> ids = readerOutboundRepository.getMessageSequenceIds((Arrays.asList(new String[]{"message_1225"})));
            Assert.assertEquals(1,ids.size());
            Assert.assertEquals(1225L,(long)ids.get(0));

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.getMessageSequenceIds((Arrays.asList(new String[]{"message_1225"})));

        } catch (Exception e) {
            fail("error is in getLastProcessedMessageidsTest " + e.getMessage());
        }
    }


    @Test
    public void getNumberOfProccessorsTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES "
                + "(3, 'process_id_4', '1', NOW(), NOW(), 1),"
                + "(4, 'process_id_4', '2', NOW(), NOW(), 1)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Assert.assertEquals(2,readerOutboundRepository.getNumberOfProccessors());

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertEquals(0,repository.getNumberOfProccessors());

        } catch (Exception e) {
            fail("error is in getNumberOfProccessorsTest " + e.getMessage());
        }
    }

    @Test
    public void markGroupUnsidelinedTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES) +
                " VALUES "
                + " (12, '4cb9a0a5-107a-45e4-84d8-988c9djs', '4cb9a0a5-107a-45e4-84d8-988c9djs1', 'SIDELINED', '1970-01-01 12:00:00', '2018-05-30 18:31:58', NULL, NULL, 0, 'adasd' ), "
                + "(13, '4cb9a0a5-107a-45e4-84d8-988c9djs', '4cb9a0a5-107a-45e4-84d8-988c9djs2', 'SIDELINED', '1970-01-01 12:00:00', '2018-05-30 18:31:58', NULL, NULL, 0, 'adasd' )");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorOutboundRepository.markGroupUnsidelined("4cb9a0a5-107a-45e4-84d8-988c9djs");

            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs' ");

            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "UNSIDELINED");
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.markGroupUnsidelined("4cb9a0a5-107a-45e4-84d8-988c9djs");

        } catch (Exception e) {
            fail("error in  markGroupUnsidelinedTest" + e.getMessage());
        }

    }

    @Test
    public void insertOrupdateSidelinedMessageStatusTest() {
        try {
            processorOutboundRepository.insertOrupdateSidelinedMessageStatus("4cb9a0a5-107a-45e4-84d8-988c9djs1", "4cb9a0a5-107a-45e4-84d8-988c9djs"
                    , SidelineReasonCode.GROUP_SIDELINED, 200, "it working ", 2, SidelinedMessageStatus.PROCESSING, session);

            Query query = session.createSQLQuery(" select * from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES));
            List<Object[]> resultList = query.list();
            for (Object[] res : resultList) {
                Assert.assertEquals(res[1], "4cb9a0a5-107a-45e4-84d8-988c9djs");
                Assert.assertEquals(res[2], "4cb9a0a5-107a-45e4-84d8-988c9djs1");
            }


            processorOutboundRepository.insertOrupdateSidelinedMessageStatus("message_1234", "group_1234"
                    , SidelineReasonCode.GROUP_SIDELINED, SidelinedMessageStatus.PROCESSING, session);
            Query querySideline = session.createSQLQuery(" select message_id, group_id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)+
                    " where message_id in ('message_1234')");
            for (Object[] res : (List<Object[]>)querySideline.list()) {
                Assert.assertEquals( "message_1234",res[0]);
                Assert.assertEquals( "group_1234",res[1]);
            }

        } catch (Exception e) {
            fail("error in  markGroupUnsidelinedTest" + e.getMessage());
        }
    }

    @Test
    public void updateControlTaskStatusTest2() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(200, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00')," +
                "(300, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djsk', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs7', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            processorOutboundRepository.updateControlTaskStatus(200, ControlTaskStatus.DONE);
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                    + " where id = 200"
            );
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "DONE");
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.updateControlTaskStatus(200, ControlTaskStatus.DONE);

        } catch (Exception e) {
            fail("error in  updateControlTaskStatusTest2" + e.getMessage());
        }

    }

    @Test
    public void deleteSidelinedMessageTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES) +
                " VALUES "
                + " (12, '4cb9a0a5-107a-45e4-84d8-988c9djs', '4cb9a0a5-107a-45e4-84d8-988c9djs1', 'SIDELINED', '1970-01-01 12:00:00', '2018-05-30 18:31:58', NULL, NULL, 0, 'adasd' ), "
                + "(13, '4cb9a0a5-107a-45e4-84d8-988c9djs', '4cb9a0a5-107a-45e4-84d8-988c9djs2', 'SIDELINED', '1970-01-01 12:00:00', '2018-05-30 18:31:58', NULL, NULL, 0, 'adasd' )");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorOutboundRepository.deleteSidelinedMessage("4cb9a0a5-107a-45e4-84d8-988c9djs1");
            Query query = session.createSQLQuery("select * from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9djs1' "
            );
            List<Object[]> resultList = query.list();
            Assert.assertEquals(resultList.size(), 0);

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.deleteSidelinedMessage("4cb9a0a5-107a-45e4-84d8-988c9djs1");

        } catch (Exception e) {
            fail("error in  deleteSidelinedMessageTest" + e.getMessage());
        }

    }

    @Test
    public void sidelineMessageTest() {

        try {
            Boolean response = processorOutboundRepository.sidelineMessage("4cb9a0a5-107a-45e4-84d8-988c9djs1", "4cb9a0a5-107a-45e4-84d8-988c9djs",
                    SidelineReasonCode.GROUP_SIDELINED, 400, "connection not possible", 2);
            Assert.assertTrue(response);

            Query query = session.createSQLQuery("select group_id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_GROUPS) +
                    " where group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs' ");
            List<String> resultList = query.list();
            Assert.assertEquals(resultList.size(), 1);

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            boolean sidelineMessageResponse = repository.sidelineMessage("4cb9a0a5-107a-45e4-84d8-988c9djs1", "4cb9a0a5-107a-45e4-84d8-988c9djs",
                    SidelineReasonCode.GROUP_SIDELINED, 400, "connection not possible", 2);
            Assert.assertFalse(sidelineMessageResponse);

        } catch (Exception e) {
            fail("the error  is in sidelineMessageTest" + e.getMessage());
        }
    }


    @Test
    public void updateSkippedMessageStatus() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                " VALUES " +
                " (1, 1, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03')," +
                "(2, 2, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            // null cases
            processorOutboundRepository.updateSkippedMessageStatus(0L,null);
            processorOutboundRepository.updateSkippedMessageStatus(null,null);
            processorOutboundRepository.updateSkippedMessageStatus(1L, SkippedIdStatus.PROCESSING);

            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                    " where id = 1 ");
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "PROCESSING");
            }

        } catch (Exception e) {
            fail("the error  is in updateSkippedMessageStatus" + e.getMessage());
        }

    }

    @Test
    public void deleteSkippedMessageTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                " VALUES " +
                " (1, 1, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03')," +
                "(2, 2, 'NEW', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorOutboundRepository.deleteSkippedMessage(null);
            processorOutboundRepository.deleteSkippedMessage(1L);

            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                    " where id = 1 ");
            List<String> resultList = query.list();
            Assert.assertEquals(resultList.size(), 0);

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.deleteSkippedMessage(0L);

        } catch (Exception e) {
            fail("the error  is in deleteSkippedMessageTest" + e.getMessage());
        }

    }

    @Test
    public void deleteControlTaskEntriesTest() {

        Transaction transaction = session.getTransaction();
        transaction.begin();
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(2, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'DONE', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            Query querySelect = session.createSQLQuery(
                    "select *  from "
                            + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                            + " where status= :status order by id");
            querySelect.setString("status" , (ControlTaskStatus.DONE).toString());
            Assert.assertNotSame(0,((List<Object[]>)querySelect.list()).size());
            Assert.assertEquals(1,processorOutboundRepository.deleteControlTaskEntries(new Date(),10));


            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            try {
                repository.deleteControlTaskEntries(new Date(),10);
                fail("IllegalArgumentException should occur");
            } catch (Exception e){
            }

        } catch (Exception e) {
            fail("the error  is in deleteControlTaskEntriesTest" + e.getMessage());
        }
    }

    @Test
    public void unsidelineMessageTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)"
                + "VALUES"
                + "(1, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'SIDELINED', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, '');");

        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            processorOutboundRepository.unsidelineMessage("4cb9a0a5-107a-45e4-84d8-988c9djs6");

            Query query = session.createSQLQuery("select status from "
                    + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9djs6' ");
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, String.valueOf(SidelinedMessageStatus.UNSIDELINED));
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.unsidelineMessage("4cb9a0a5-107a-45e4-84d8-988c9djs6'");

        } catch (Exception e) {
            fail("the error  is in unsidelineMessaggeTest" + e.getMessage());
        }

    }

    @Test
    public void unsidelineAllUngroupedMessageTest() {
        Date fromDate = new Date();
        Date toDate = new Date();
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES) + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)" +
                " VALUES " +
                "(1, NULL, '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'SIDELINED', '" +
                DateFormatUtils.format(fromDate, "yyyy-MM-dd HH:mm:ss") +
                "','" + DateFormatUtils.format(toDate, "yyyy-MM-dd HH:mm:ss") +
                "', 0, 'GROUP_SIDELINED', 0, '');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {

            processorOutboundRepository.unsidelineAllUngroupedMessage(fromDate, toDate);
            Query query = session.createSQLQuery("select status from "
                    + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9djs6' ");
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, String.valueOf(SidelinedMessageStatus.UNSIDELINED));
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.unsidelineAllUngroupedMessage(fromDate, toDate);

        } catch (Exception e) {
            fail("the error  is in unsidelineMessaggeTest" + e.getMessage());
        }
    }

    @Test
    public void persistLastProcessedMessageIdTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES "
                + " (9, 'Processor_queue_SP_1', '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-30 11:46:55', '2018-05-30 11:46:56', 1),"
                + " (8, 'Processor_queue_SP_0', '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1)");

        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            processorMetaDataRepository.persistLastProcessedMessageId("Processor_queue_SP_0", "4cb9a0a5-107a-45e4-84d8-988c9djs7", null);
            Query query = session.createSQLQuery(" select message_id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                    + " where process_id = 'Processor_queue_SP_0' ");
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "4cb9a0a5-107a-45e4-84d8-988c9djs7");
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.persistLastProcessedMessageId("Processor_queue_SP_0", "4cb9a0a5-107a-45e4-84d8-988c9djs7", null);
        } catch (Exception e) {
            fail("the error is in persistLastProcessedMessageIdTest " + e.getMessage());
        }

    }

    @Test
    public void getRegisteredProcessesTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES "
                + " (9, 'Processor_queue_SP_1', '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-30 11:46:55', '2018-05-30 11:46:56', 1),"
                + " (8, 'Processor_queue_SP_0', '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1)");

        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            Set<String> resultSet = processorMetaDataRepository.getRegisteredProcesses(Arrays.asList("Processor_queue_SP_1", "Processor_queue_SP_0"));
            Assert.assertEquals(resultSet.size(), 2);

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            repository.getRegisteredProcesses(Arrays.asList("Processor_queue_SP_1", "Processor_queue_SP_0"));

        } catch (Exception e) {
            fail("the error is in getRegisteredProcessesTest :" + e.getMessage());
        }
    }

    @Test
    public void registerNewProcessTest() {
        try {
            processorMetaDataRepository.registerNewProcess(Arrays.asList("Processor_queue_SP_2", "Processor_queue_SP_3"));
            Query query = session.createSQLQuery("select * from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE));

            List<String> resultList = query.list();

            Assert.assertEquals(resultList.size(), 2);
        } catch (Exception e) {
            fail("the error is in registerNewProcessTest: " + e.getMessage());
        }

    }

    @Test
    public void clearOldProcessorRecordsTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES "
                + " (9, 'Processor_queue_SP_1', '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-30 11:46:55', '2018-05-30 11:46:56', 1),"
                + " (8, 'Processor_queue_SP_0', '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1) ,"
                + "(11, 'Processor_queue_SP_2', '4cb9a0a5-107a-45e4-84d8-988c9djs7', '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1)");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorMetaDataRepository.clearOldProcessorRecords(Arrays.asList("Processor_queue_SP_1", "Processor_queue_SP_0"));
            Query query = session.createSQLQuery("select * from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE));

            List<String> resultList = query.list();
            Assert.assertEquals(resultList.size(), 2);

        } catch (Exception e) {
            fail("the error is in getRegisteredProcessesTest :" + e.getMessage());
        }

    }


    @Test
    public void getLastProcessedMessageIdsWithSystemExitTest() {
        try {
            List<String> list = processorMetaDataRepository.getLastProcessedMessageIdsWithSystemExit();
            for(String s : list){
                Assert.assertNotNull(s);
            }

            // skipping test case of exception as it does a System.exit
            SessionFactory sessionFactory= mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);

        } catch (Exception e) {
            fail("error is in getMinMessageFromMessagesTest " + e.getMessage());
        }
    }

    @Test
    public void createUnsidelineGroupTaskTest() {
        try {
            processorMetaDataRepository.createUnsidelineGroupTask("4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs");
            Query query = session.createSQLQuery("select group_id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS));
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs");
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertFalse(repository.createUnsidelineGroupTask("4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs"));

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void createUnsidelineMessageTaskTest() {
        try {
            processorMetaDataRepository.createUnsidelineMessageTask("4cb9a0a5-107a-45e4-84d8-988c9djs5");
            Query query = session.createSQLQuery("select message_id from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS));
            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "4cb9a0a5-107a-45e4-84d8-988c9djs5");
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertFalse(repository.createUnsidelineMessageTask("4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs"));

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }


    @Test
    public void createUnsidelineMessagesBetweenDatesTaskTest() {
        Date fromDate = new Date();
        Date toDate = new Date();
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)"
                + " VALUES "
                + "(1, NULL, '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'SIDELINED', '"
                + DateFormatUtils.format(fromDate, "yyyy-MM-dd HH:mm:ss")
                + "','" + DateFormatUtils.format(toDate, "yyyy-MM-dd HH:mm:ss")
                + "', 0, 'GROUP_SIDELINED', 0, '');");
        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            boolean result = processorMetaDataRepository.createUnsidelineMessagesBetweenDatesTask(fromDate, toDate);
            Assert.assertEquals(result, true);
        } catch (Exception e) {
            fail("the error is in createUnsidelineMessagesBetweenDatesTaskTest: " + e.getMessage());
        }
    }

    @Test
    public void createUnsidelineAllUngroupedMessageTaskTest() {
        Date fromDate = new Date();
        Date toDate = new Date();
        try {
            processorMetaDataRepository.createUnsidelineAllUngroupedMessageTask(fromDate, toDate);

            Query query = session.createSQLQuery("select from_Date , to_date from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS));
            List<Object[]> resultList = query.list();
            for (Object[] res : resultList) {
                Assert.assertEquals(DateFormatUtils.format((Date) res[0], "yyyy-MM-dd HH:mm:ss"), DateFormatUtils.format(fromDate, "yyyy-MM-dd HH:mm:ss"));
                Assert.assertEquals(DateFormatUtils.format((Date) res[1], "yyyy-MM-dd HH:mm:ss"), DateFormatUtils.format(toDate, "yyyy-MM-dd HH:mm:ss"));
            }

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertFalse(repository.createUnsidelineAllUngroupedMessageTask(fromDate, toDate));

        } catch (Exception e) {
            fail(" the error is in createUnsidelineAllUngroupedMessageTaskTest: " + e.getMessage());
        }

    }

    @Test
    public void resetProcessingStateSkippedIdsTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO " +
                exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS) +
                " VALUES " +
                " (1, 1, 'PROCESSING', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03')," +
                "(2, 2, 'PROCESSING', 0, '2018-05-30 17:53:03', '2018-05-30 17:53:03');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorMetaDataRepository.resetProcessingStateSkippedIds();
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SKIPPED_IDS));

            List<String> resultList = query.list();

            for (String res : resultList) {
                Assert.assertEquals(res, "NEW");
            }


        } catch (Exception e) {
            fail(" the error is in resetProcessingStateSkippedIdsTest: " + e.getMessage());
        }

    }

    @Test
    public void resetProcessingStateControlTasksTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(1, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'PROCESSING', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorMetaDataRepository.resetProcessingStateControlTasks();
            Query query = session.createSQLQuery(" select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                    + " where message_id = '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6' ");

            List<String> resultList = query.list();
            for (String res : resultList) {
                Assert.assertEquals(res, "NEW");
            }

        } catch (Exception e) {
            fail("the error is in resetProcessingStateControlTasksTest :" + e.getMessage());
        }
    }

    @Test
    public void resetProcessingStateSidelinedMessagesTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        //also inserting the current time value as a group id
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_GROUPS)
                + " (`id`, `group_id`, `status`, `created_at`, `updated_at`) "
                + "VALUES "
                + " (2, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs' , NULL, '1970-01-01 12:00:00', NOW() ), "
                + " (3, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs2' , NULL, '1970-01-01 12:00:00', NOW() )");
        InsertQuery.executeUpdate();
        transaction.commit();

        transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES) + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)" +
                "VALUES "
                + "(1, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'PROCESSING', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, ''), "
                + " (2, '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs2', '4cb9a0a5-107a-45e4-84d8-988c9djs7', 'PROCESSING', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, '');");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorMetaDataRepository.resetProcessingStateSidelinedMessages();
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs' or group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs2' ");

            List<String> resultList = query.list();

            for (String res : resultList) {
                Assert.assertEquals(res, "UNSIDELINED");
            }

        } catch (Exception e) {
            fail("error is in resetProcessingStateSidelinedMessagesTest :" + e.getMessage());
        }
    }

    @Test
    public void getSidelinedGroupsTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getSidelinedGroups());

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void getCurrentSidelinedMessageCountTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getCurrentSidelinedMessageCount());

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }


    @Test
    public void getCurrentSidelinedGroupsCountTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getCurrentSidelinedGroupsCount());

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }


    @Test
    public void getMaxMessageIdTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(HibernateException.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getMaxMessageId());

            SessionFactory sessionFactoryOnException = mock(SessionFactory.class);
            when(sessionFactoryOnException.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repositoryOnException = new OutboundRepositoryImpl(sessionFactoryOnException,null);
            try {
                Assert.assertNull(repositoryOnException.getMaxMessageId());
                fail("IllegalArgumentException should occur");
            } catch (Exception e){
            }

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void getPendingMessageCountTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getPendingMessageCount());
        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void getMessageIdCreationTimeInOrderTest() {
        try {

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void checkOrPerformLeaderElectionTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertFalse(repository.checkOrPerformLeaderElection(null,0,0));

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void getMaxParitionIDTest() {
        try {

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNull(repository.getMaxParitionID());

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }

    @Test
    public void getTablesCharSetTest() {
        try {

            Assert.assertEquals(0, readerOutboundRepository.getTablesCharSet(null).size());
            Assert.assertEquals(0, readerOutboundRepository.getTablesCharSet(Arrays.asList(new String[]{})).size());

            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertNotNull(repository.getTablesCharSet(null));

        } catch (Exception e) {
            fail("error occurred " + e.getMessage());
        }
    }


    @Test
    public void resetProcessingStateSidelinedMessagesTest2() {
        transaction = session.getTransaction();
        transaction.begin();
        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES) + " (id, group_id, message_id, status, created_at, updated_at, http_status_code, sideline_reason_code, retries, details)" +
                "VALUES "
                + "(1, NULL, '4cb9a0a5-107a-45e4-84d8-988c9djs6', 'PROCESSING', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, ''), "
                + " (2, NULL, '4cb9a0a5-107a-45e4-84d8-988c9djs7', 'PROCESSING', '2018-05-28 13:48:23', '2018-05-29 19:47:26', 0, 'GROUP_SIDELINED', 0, '');");

        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            processorMetaDataRepository.resetProcessingStateSidelinedMessages();
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES)
                    + " where group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs' or group_id = '4cb9a0a5-107a-45e4-84d8-988c9djs2' ");

            List<String> resultList = query.list();

            for (String res : resultList) {
                Assert.assertEquals(res, "UNSIDELINED");
            }

        } catch (Exception e) {
            fail("error is in resetProcessingStateSidelinedMessagesTest2 :" + e.getMessage());
        }
    }


    @Test
    public void getInitailMaxSeqTableStatusTest() {
        Transaction transaction = session.getTransaction();
        transaction.begin();

        //inserting dummy data in the database for the test purpose
        Query InsertQuery = session.createSQLQuery("INSERT INTO " + exchangeTableNameProvider
                .getTableName(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE)
                + " VALUES "
                + " (9, 'Processor_queue_SP_1', '4cb9a0a5-107a-45e4-84d8-988c9djs5', '2018-05-30 11:46:55', '2018-05-30 11:46:56', 1),"
                + " (8, 'Processor_queue_SP_0', '4cb9a0a5-107a-45e4-84d8-988c9djs6', '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1) ,"
                + " (10, 'Processor_queue_SP_2', NULL, '2018-05-30 11:46:55', '2018-05-30 12:23:57', 1)");

        InsertQuery.executeUpdate();
        transaction.commit();

        try {
            HashMap<String, String> result = processorOutboundRepository.getLastProcessedMessages();
            Assert.assertEquals(result.size(), 2);
        } catch (Exception e) {
            fail("the error is in getInitailMaxSeqTableStatusTest " + e.getMessage());
        }
    }


    @Test
    public void createPartitionManagementTaskTest() {
        //creating a transaction for the database session
        Transaction transaction = session.getTransaction();
        transaction.begin();
        Query InsertQuery = session.createSQLQuery("INSERT INTO "
                + exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS)
                + "(`id`, `task_type`, `group_id`, `message_id`, `status`, `updated_at`, `created_at`, `from_date`, `to_date`)"
                + "VALUES "
                + "(1, 'UNSIDELINE_GROUP', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs', '4cb9a0a5-107a-45e4-84d8-988c9ccefb3djs6', 'NEW', '2018-05-30 00:14:08', '1970-01-01 12:00:00', '1970-01-01 12:00:00', '1970-01-01 12:00:00');");
        InsertQuery.executeUpdate();
        transaction.commit();
        try {
            relayerOutboundRepository.createPartitionManagementTask("2");
            Query query = session.createSQLQuery("select status from " +
                    exchangeTableNameProvider.getTableName(ExchangeTableNameProvider.TableType.CONTROL_TASKS));
            List<String> resultList = query.list();
            Assert.assertEquals(resultList.size(), 2);


            // eat up exceptions
            SessionFactory sessionFactory = mock(SessionFactory.class);
            when(sessionFactory.openSession()).thenThrow(Exception.class);
            OutboundRepositoryImpl repository = new OutboundRepositoryImpl(sessionFactory,null);
            Assert.assertFalse(repository.createPartitionManagementTask("2"));

        } catch (Exception e) {
            fail("error is in resetProcessingStateSidelinedMessagesTest2 :" + e.getMessage());
        }
    }

}
