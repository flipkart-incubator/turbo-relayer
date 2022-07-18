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

package com.flipkart.restbus.turbo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class TurboConfigProvider
{
    private static TurboConfig config = null;
    private static final String DEFAULT_TURBO_CONFIG_FILE_PATH = "/etc/fk-sc-mq/turbo.yml";
    private static final Logger logger = LoggerFactory.getLogger(TurboConfigProvider.class);

    public static TurboConfig getConfig()
    {
        if(config == null)
        {
            String configFilePath = System.getProperty("SC_MQ_TURBO_CONFIG");
            if (configFilePath == null || configFilePath.isEmpty())
                configFilePath = DEFAULT_TURBO_CONFIG_FILE_PATH;

            config = loadConfigFromPath(configFilePath);
       }

        return config;
    }

    /**
     * allows you to parse and load config from any location
     * @param configFilePath
     * @return
     */
    public static TurboConfig loadConfigFromPath(String configFilePath){
        if(configFilePath == null || configFilePath.trim().length() == 0) {
            throw new IllegalArgumentException("config path can't be empty/null");
        }

        InputStream document;
        TurboConfig config;
        try {
            document = new FileInputStream(new File(configFilePath));
            config = TurboConfig.parseDocument(document);
            if(config.getMysql() != null) {
                config.getMysql().setProperty("hibernate.connection.autocommit", "true");
            }
            Map<String, Properties> dbShards = config.getDbShards();
            if(dbShards != null) {
                for (Properties property : dbShards.values()) {
                    property.setProperty("hibernate.connection.autocommit","true");
                }
            }
            logger.info("Turbo Outbound Message config loaded from " + configFilePath);
        }  catch (FileNotFoundException e) {
            if (DEFAULT_TURBO_CONFIG_FILE_PATH.equals(configFilePath)) {
                logger.debug("Default turbo config at " + DEFAULT_TURBO_CONFIG_FILE_PATH + " not found.");
            } else {
                logger.error(configFilePath + " file not found.");
            }
            config = new TurboConfig();  //Return with default values.
        }
        return config;
    }


    /**
     *
     * @param turboConfig
     * This method overrides any config provided by you, make sure autocommit in set to true in the new config.
     * For example if you are using hibernate set "hibernate.connection.autocommit" to "true" while providing config.
     * @return
     */
    public static TurboConfig setConfig(TurboConfig turboConfig)
    {
        if(turboConfig == null)
            throw new IllegalArgumentException("Turbo Config can't be null");
        config = turboConfig;
        return config;
    }

}
