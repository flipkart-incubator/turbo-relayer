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
import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.relayer.InitializeRelayer;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;


public class SchemaCreatorTest extends DBBaseTest {
    SessionFactory sessionFactory = null;

    @Before
    public void setup() {
        ApplicationConfiguration applicationConfiguration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        sessionFactory = new RelayerModule().provideSessionFactoryContainer(applicationConfiguration).getSessionFactory("outbound");
    }

    @Test
    public void test() {
        new SchemaCreator(() -> sessionFactory.openSession(), new ExchangeTableNameProvider("test"), 50, 5, PartitionMode.AUTO).ensureOutboundSchema();
        SQLQuery outbound = sessionFactory.openSession().createSQLQuery("show tables from outbound where Tables_in_outbound like '%test%'");
        List list = outbound.list();
        Assert.assertEquals(7, list.size());

    }

    @After
    public void cleanup() {
        Session session = sessionFactory.openSession();
        SQLQuery outbound = session.createSQLQuery("show tables from outbound where Tables_in_outbound like '%test%'");
        String s = "drop tables " + outbound.list().stream().map(t -> "outbound." + t).collect(Collectors.joining(","));
        session.createSQLQuery(s).executeUpdate();
        session.close();
    }

}