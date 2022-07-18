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

package com.flipkart.varidhi.core.utils;

import com.flipkart.varidhi.RelayerMainService;
import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.PartitionMode;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.SessionFactory;
import org.hibernate.internal.SessionFactoryImpl;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by manmeet.singh on 07/03/16.
 */
public class MysqlDumpHelper {
    private static final Logger logger = LoggerFactory.getLogger(MysqlDumpHelper.class);


    public static void takeDump(long startId, long endId, String tableName,
        SessionFactory sessionFactory, String partitionKey) {
        Properties sessionProperties = ((SessionFactoryImpl) sessionFactory).getProperties();
        String username = sessionProperties.getProperty("hibernate.connection.username");
        String password = sessionProperties.getProperty("hibernate.connection.password");

        Properties urlProperties = SessionPropertiesHelper.getUrlProperties(sessionFactory);
        String hostname = urlProperties.getProperty("HOST");
        //        String hostname = sessionProperties.getProperty("slave_hostname");
        String dbName = urlProperties.getProperty("DBNAME");

        String dumpFileName =
            tableName + "_" + startId + "-" + endId + "_" + new LocalDateTime().toString() + ".sql";
        String dumpFilePath = getArchivalPath() + "/" + dumpFileName;

        String dumpStatement = "mysqldump -u" + username;
        if (password != null && !password.isEmpty()) {
            dumpStatement = dumpStatement + " -p" + password;
        }
        dumpStatement = dumpStatement +
            " -h" + hostname +
            " " + dbName +
            " " + tableName +
            " --where=\"(" + partitionKey + " between " + startId + " AND " + endId
            + ")\" --result-file=" +
            dumpFilePath + " --extended-insert --quick --single-transaction";
        String zipStatement = "gzip " + dumpFilePath;

        String rsyncStatement = "rsync -avh " + dumpFilePath + ".gz " + getArchivalDestination();
        String deleteFileStatement = "rm " + dumpFilePath + ".gz";
        logger
            .info("taking dump for table: " + tableName + " from id: " + startId + " to " + endId);
        executeDumpStatement(dumpStatement + "\n" + zipStatement + "\n" + rsyncStatement + "\n"
            + deleteFileStatement);
    }

    private static void executeDumpStatement(String dumpStatement) {
        if (isObserver()) {
            logger.debug("Dump statement executed will be : " + dumpStatement);
            return;
        }
        try {
            Process process = Runtime.getRuntime().exec(new String[] {"bash", "-c", dumpStatement});
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(process.getErrorStream()));
            List<String> errors = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                errors.add(line);
            }
            if (!errors.isEmpty()) {
                throw new RuntimeException(
                    "Errors while executing dump script: " + StringUtils.join(errors, ","));
            }
        } catch (IOException e) {
            logger.error("IO Exception: ", e);
            throw new RuntimeException(e);
        }
    }

    private static String getArchivalPath() {
        return RelayerMainService.getInstance(ApplicationConfiguration.class).
            getPartitionConfiguration().getArchivalPath();
    }

    private static String getArchivalDestination() {
        return RelayerMainService.getInstance(ApplicationConfiguration.class).
            getPartitionConfiguration().getArchivalDestination();
    }

    private static boolean isObserver() {
        return RelayerMainService.getInstance(ApplicationConfiguration.class).
            getPartitionConfiguration().getMode()== PartitionMode.OBSERVER;
    }
}
