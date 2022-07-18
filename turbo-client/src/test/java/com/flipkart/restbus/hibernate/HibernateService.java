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

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HibernateService {
    private static HibernateService hibernateService = null;

    private Map<String, SessionFactory> sessionFactories = new HashMap<String, SessionFactory>();

    private Logger logger = null;

    private HibernateService() {
        logger = LoggerFactory.getLogger(HibernateService.class);
    }

    public static HibernateService getInstance() {
        Logger logger = LoggerFactory.getLogger(HibernateService.class);

        if (hibernateService == null) {
            logger.trace("Creating new HibernateManager instance");
            hibernateService = new HibernateService();
        } else {
            logger.trace("HibernateManager instance already exits");
        }

        return hibernateService;
    }

    public synchronized AnnotationConfiguration configure(String database, List<Class<?>> classes, String file) {
        logger.trace("Loading hibernate configurations for " + database + " from file: " + file);
        AnnotationConfiguration conf = new AnnotationConfiguration();
        conf.configure(new File(file));

        if (classes != null) {
            for (Class<?> className : classes) {
                conf.addAnnotatedClass(className);
            }
        }

        sessionFactories.put(database, conf.buildSessionFactory());

        return conf;
    }

    public SessionFactory getSessionFactory(String database) {
        return sessionFactories.get(database);
    }

    public synchronized Session getCurrentSession(String database) {
        return sessionFactories.get(database).getCurrentSession();
    }

    public synchronized Session openSession(String database) {
        return sessionFactories.get(database).openSession();
    }


    public void close() {
        for (String db : sessionFactories.keySet()) {
            SessionFactory sessionFactory = sessionFactories.get(db);
            if (sessionFactory != null && !sessionFactory.isClosed()) {
                sessionFactory.close();
            }
        }
    }

    public static void closeSession(Session session) {
        if (session != null && session.isConnected()) {
            session.close();
        }
    }
}
