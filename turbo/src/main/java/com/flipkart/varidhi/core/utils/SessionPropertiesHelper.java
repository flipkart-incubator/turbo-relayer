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

import com.mysql.jdbc.NonRegisteringDriver;
import org.hibernate.SessionFactory;
import org.hibernate.internal.SessionFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by manmeet.singh on 16/03/16.
 */
public class SessionPropertiesHelper {
    private static final Logger logger = LoggerFactory.getLogger(SessionPropertiesHelper.class);

    public static Properties getUrlProperties(SessionFactory sessionFactory) {
        Properties sessionProperties = ((SessionFactoryImpl) sessionFactory).getProperties();
        String connectionUrl = sessionProperties.getProperty("hibernate.connection.url");
        Properties urlProperties;
        try {
            urlProperties = new NonRegisteringDriver().parseURL(connectionUrl, new Properties());
            logger.info("Url Properties:" + urlProperties);
        } catch (SQLException e) {
            logger.error("Error while parsing DB connection URL: ", e);
            throw new RuntimeException(e);
        }
        return urlProperties;
    }
}
