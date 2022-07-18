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

package com.flipkart.varidhi.utils;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URISyntaxException;

import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static com.wix.mysql.distribution.Version.v5_6_36;

/**
 * @author shah.dhruvik
 * @date 14/05/19.
 */


public class DBBaseTest {

    private static EmbeddedMysql embeddedMysql;
    private static boolean setUpIsDone = false;
    private static boolean useNativeMysql = false;

    @BeforeClass
    public static void _setupBeforeClass() {
        if(!setUpIsDone && !useNativeMysql) {
            DownloadConfig downloadConfig = aDownloadConfig()
                    .withCacheDir("/var/tmp/")
                    .build();

            MysqldConfig config = MysqldConfig.aMysqldConfig(v5_6_36)
                    .withPort(3344)
                    .withUser("test", "test")
                    .withServerVariable("max_connections", "1000")
                    .build();

            embeddedMysql = EmbeddedMysql.anEmbeddedMysql(config, downloadConfig)
                    .addSchema("app")
                    .addSchema("outbound")
                    .start();
            setUpIsDone = true;
        }
    }


    private static void reloadSchema() {
        embeddedMysql.reloadSchema("app");
        embeddedMysql.reloadSchema("outbound");
    }

    @AfterClass
    public static void _tearDownAfterClass() {
        if (null != embeddedMysql && !useNativeMysql) {
           reloadSchema();
        }
    }
}
