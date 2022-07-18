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

package com.flipkart.varidhi.core;

import org.hibernate.SessionFactory;

import java.util.HashMap;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 31-Jul-2015
 *
 */
public class MapSessionFactoryContainer implements SessionFactoryContainer {
    private Map<String, SessionFactory> sessionFactoryMap = new HashMap<>();

    @Override public SessionFactory getSessionFactory(String namespace) {
        return sessionFactoryMap.get(namespace);
    }

    public void addSessionFactory(String namespace, SessionFactory sessionFactory) {
        sessionFactoryMap.put(namespace, sessionFactory);
    }

    public void removeSessionFactoryEntry(String namespace) {
        sessionFactoryMap.remove(namespace);
    }
}
