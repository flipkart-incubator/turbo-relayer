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

package com.flipkart.restbus.hibernate.turbo;

import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.hibernate.DBBaseTest;
import com.flipkart.restbus.hibernate.utils.TurboOutboundMessageUtils;
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import com.flipkart.restbus.turbo.config.TurboSessionProvider;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by harshit.gangal on 19/09/16.
 */
public class TurboTest extends DBBaseTest {

    @Test
    public void testTableCreation() {
        System.setProperty("SC_MQ_TURBO_CONFIG", "src/test/resources/turbo_test.yml");

        deleteTables();

        TurboOutboundMessageUtils.ensureTurboMetaDataShard(TurboSessionProvider.getSession(),
            new Shard(TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME));

        TurboOutboundMessageUtils.ensureTurboMessageShard(TurboSessionProvider.getSession(),
            new Shard(TurboOutboundMessageUtils.DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME));

        try {
            TurboOutboundMessageUtils.partitionMaxId(TurboSessionProvider.getSession(),
                TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME);
            Assert.fail("Should result in null pointer exception");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), NullPointerException.class);
        }
        try {
            TurboOutboundMessageUtils.partitionMaxId(TurboSessionProvider.getSession(),
                TurboOutboundMessageUtils.DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME);
            Assert.fail("Should result in null pointer exception");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), NullPointerException.class);
        }
    }



    public void deleteTables() {
        Session session = TurboSessionProvider.getSession();
        Transaction tx = session.beginTransaction();
        session.createSQLQuery(
            "DROP TABLE IF EXISTS " + TurboOutboundMessageUtils.DEFAULT_TURBO_META_DATA_TABLE_NAME)
            .executeUpdate();
        session.createSQLQuery("DROP TABLE IF EXISTS "
            + TurboOutboundMessageUtils.DEFAULT_TURBO_OUTBOUND_MESSAGES_TABLE_NAME).executeUpdate();
        tx.commit();
        session.close();
    }
}
