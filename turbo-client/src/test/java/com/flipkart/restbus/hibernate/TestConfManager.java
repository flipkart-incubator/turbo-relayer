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

import com.flipkart.restbus.hibernate.utils.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public class TestConfManager {

    public static final String HIBERNATE_CONF__FKLOGISTICS = "src/test/resources/hibernate.config.xml";

    public static final String DB = "DB";
    private static Logger logger = LoggerFactory.getLogger(TestConfManager.class);

    public static void init() {
        logger.info("Initializing session ");
        HibernateService hibernateService = HibernateService.getInstance();
        hibernateService.configure(DB, getAnnotatedClasses(), HIBERNATE_CONF__FKLOGISTICS);
    }

    public static List<Class<?>> getAnnotatedClasses() {
        return Helper.getAnnotatedClasses();
    }

    public static void close() {
        HibernateService.getInstance().close();
    }
}
