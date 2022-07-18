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

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.config.RepositoryConfiguration;
import com.flipkart.varidhi.core.utils.EmailNotifier;
import com.flipkart.varidhi.relayer.InitializeRelayer;
import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Properties;

import static com.flipkart.varidhi.jobs.PartitionQueryEmailer.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionQueryEmailerTest {


    private static String getString() {
        ApplicationConfiguration applicationConfiguration = mock(ApplicationConfiguration.class);
        HashMap<String, Properties> hashMap = new HashMap<>();
        Properties srms_app_properties = new Properties();
        srms_app_properties.put("hibernate.connection.url", "mysql://somewhere//1111");
        hashMap.put("srms_app", srms_app_properties);
        Properties srms_outbound_properties = new Properties();
        srms_outbound_properties.put("hibernate.connection.url", "mysql://here_there//2222");
        hashMap.put("srms_outbound", srms_outbound_properties);
        Properties orchestrator_app_properties = new Properties();
        orchestrator_app_properties.put("hibernate.connection.url", "mysql://outside//3333");
        hashMap.put("orchestrator_app", orchestrator_app_properties);
        Properties orchestrator_outbound_properties = new Properties();
        orchestrator_outbound_properties.put("hibernate.connection.url", "mysql://inside//4444");
        hashMap.put("orchestrator_outbound", orchestrator_outbound_properties);
        when(applicationConfiguration.getMysql()).thenReturn(hashMap);
        PartitionQueryEmailer partitionQueryEmailer = new PartitionQueryEmailer(new EmailNotifier(applicationConfiguration.getEmailConfiguration()), applicationConfiguration);
        RelayerConfiguration rc1 = mock(RelayerConfiguration.class);
        when(rc1.getName()).thenReturn("SMRS");

        RepositoryConfiguration appDbRef1 = mock(RepositoryConfiguration.class);
        when(rc1.getAppDbRef()).thenReturn(appDbRef1);
        when(appDbRef1.getId()).thenReturn("srms_app");

        RepositoryConfiguration outboundDbRef1 = mock(RepositoryConfiguration.class);
        when(rc1.getOutboundDbRef()).thenReturn(outboundDbRef1);
        when(outboundDbRef1.getId()).thenReturn("srms_outbound");
        RelayerConfiguration rc2 = mock(RelayerConfiguration.class);

        when(rc2.getName()).thenReturn("Orchestrator");

        RepositoryConfiguration appDbRef2 = mock(RepositoryConfiguration.class);
        when(rc2.getAppDbRef()).thenReturn(appDbRef2);
        when(appDbRef2.getId()).thenReturn("orchestrator_app");

        RepositoryConfiguration outboundDbRef2 = mock(RepositoryConfiguration.class);
        when(rc2.getOutboundDbRef()).thenReturn(outboundDbRef2);
        when(rc2.getOutboundDbRef().getId()).thenReturn("orchestrator_outbound");
        when(applicationConfiguration.getRelayers()).thenReturn(ImmutableList.of(rc1, rc2));
        return partitionQueryEmailer.getBody(ImmutableList.of(
                new PartitionEvent("A", rc1, DBTYPE.APPLICATION, "SMRS APP A"),
                new PartitionEvent("B", rc1, DBTYPE.TR, "SMRS TR B"),
                new PartitionEvent("B", rc1, DBTYPE.TR, "SMRS TR B1"),
                new PartitionEvent("P", rc2, DBTYPE.APPLICATION, "Orchestrator APP P"),
                new PartitionEvent("Q", rc2, DBTYPE.TR, "Orchestrator TR Q"),
                new PartitionEvent("Q", rc2, DBTYPE.TR, "Orchestrator TR Q1")
        ));
    }

    @Test
    public void test2() {
        PartitionQueryEmailer partitionQueryEmailer = new PartitionQueryEmailer(null, null) {
            @Override
            public void finish() {
                Assert.assertEquals(1, partitionEventSet.size());
            }
        };
        partitionQueryEmailer.collect("A", null, DBTYPE.APPLICATION, "query1");
        partitionQueryEmailer.collect("A", null, DBTYPE.APPLICATION, "query1");
        partitionQueryEmailer.finish();
    }


    @Test
    public void test3() {
        PartitionQueryEmailer partitionQueryEmailer = new PartitionQueryEmailer(null, null) {
            @Override
            public void finish() {
                Assert.assertEquals(2, partitionEventSet.size());
            }
        };
        partitionQueryEmailer.collect("A", null, DBTYPE.APPLICATION, "query1");
        partitionQueryEmailer.collect("A", null, DBTYPE.TR, "query1");
        partitionQueryEmailer.finish();
    }


    @Test
    public void test4() {
        PartitionQueryEmailer partitionQueryEmailer = new PartitionQueryEmailer(null, null) {
            @Override
            public void finish() {
                Assert.assertEquals(2, partitionEventSet.size());
            }
        };
        partitionQueryEmailer.collect("A", null, DBTYPE.APPLICATION, "query1");
        partitionQueryEmailer.collect("A", null, DBTYPE.APPLICATION, "query2");
        partitionQueryEmailer.finish();
    }


}