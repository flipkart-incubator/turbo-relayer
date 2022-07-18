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

package com.flipkart.restbus.hibernate;

import com.flipkart.restbus.client.shards.Shard;
import com.flipkart.restbus.hibernate.utils.OutboundMessageUtils;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.util.concurrent.TimeUnit;

import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static com.wix.mysql.distribution.Version.v5_6_36;

public class DBBaseTest {
    protected static Session session = null;
    private static final Logger logger = LoggerFactory.getLogger(DBBaseTest.class);
    private static EmbeddedMysql embeddedMysql;
    private static boolean setUpIsDone = false;
    private static boolean useNativeMysql = false;

    @BeforeClass
    public void initSession() {
        logger.info("Calling before class");
        _setupBeforeClass();
    }

    public static void _setupBeforeClass() {
        if(!setUpIsDone && !useNativeMysql) {
            System.out.println("Initializing embeddedMysql....");
            DownloadConfig downloadConfig = aDownloadConfig()
                    .withCacheDir("/var/tmp/")
                    .build();

            MysqldConfig config = MysqldConfig.aMysqldConfig(v5_6_36)
                    .withPort(3344)
                    .withTimeout(5,TimeUnit.MINUTES)
                    .withUser("test", "test")
                    .withServerVariable("max_connections", "1000")
                    .build();

            embeddedMysql = EmbeddedMysql.anEmbeddedMysql(config, downloadConfig)
                    .addSchema("app")
                    .addSchema("outbound")
                    .addSchema("outbound_shard1")
                    .start();
            setUpIsDone = true;
        }
    }

    @BeforeMethod
    public void setUp() {
        if (session == null ) {
            logger.info("Initializing Connection");
            TestConfManager.init();
            session = HibernateService.getInstance().openSession(TestConfManager.DB);
        }
        logger.info("Beginning transaction");
        session.beginTransaction();
        ensureTables(session);
    }

    protected void ensureTables(Session session){
        // createMessageMetaDataQuery
        session.createSQLQuery("CREATE TABLE IF NOT EXISTS message_meta_data (\n" +
                "  `id` bigint(20)  NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) NOT NULL,\n" +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `message_id` (`message_id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1\n").executeUpdate();

        // createMessagesQuery
        session.createSQLQuery("CREATE TABLE IF NOT EXISTS `messages` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `message_id` varchar(100) DEFAULT NULL,\n" +
                "  `message` mediumtext,\n" +
                "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  `exchange_name` varchar(100) DEFAULT NULL,\n" +
                "  `exchange_type` varchar(20) DEFAULT 'queue',\n" +
                "  `app_id` varchar(100) DEFAULT NULL,\n" +
                "  `group_id` varchar(100) DEFAULT NULL,\n" +
                "  `http_method` varchar(10) DEFAULT NULL,\n" +
                "  `http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `parent_txn_id` varchar(100) DEFAULT NULL,\n" +
                "  `reply_to` varchar(100) DEFAULT NULL,\n" +
                "  `reply_to_http_method` varchar(10) DEFAULT NULL,\n" +
                "  `reply_to_http_uri` varchar(4096) DEFAULT NULL,\n" +
                "  `context` text,\n" +
                "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  `custom_headers` text,\n" +
                "  `transaction_id` varchar(100) DEFAULT NULL,\n" +
                "  `correlation_id` varchar(100) DEFAULT NULL,\n" +
                "  `destination_response_status` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `message_id` (`message_id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1\n" +
                "/*!50100 PARTITION BY RANGE (id)\n" +
                "(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,\n" +
                " PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,\n" +
                " PARTITION p2 VALUES LESS THAN (300000) ENGINE = InnoDB) */;").executeUpdate();
        System.setProperty("SC_MQ_TURBO_CONFIG", "src/test/resources/turbo_test.yml");
        logger.info("SC_MQ_TURBO_CONFIG : "+System.getProperty("SC_MQ_TURBO_CONFIG"));
        OutboundMessageUtils.ensureShard(session, new Shard(OutboundMessageUtils.DEFAULT_OUTBOUND_MESSAGES_TABLE_NAME));
        System.setProperty("profile", "test");
    }

    @AfterMethod
    public void tearDown() {
        logger.info("Rolling back transaction");
        session.getTransaction().rollback();
        session.clear();

    }

    @AfterClass
    public static void _tearDownAfterClass() {
        logger.info("Calling After class");
        if (null != embeddedMysql && !useNativeMysql) {
            // clear schema if needed
        }
    }
}
