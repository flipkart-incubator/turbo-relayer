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


/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */


import com.flipkart.restbus.turbo.exceptions.InvalidInvocationException;
import com.flipkart.restbus.turbo.utils.TurboHelper;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.List;

public class TurboSessionProvider
{
    private static SessionFactory sessionFactory;

    private static final HashMap<String,SessionFactory> dbShardToSessionFactory = new HashMap<String, SessionFactory>();

    public static Session getSession()
    {
        if(sessionFactory==null) {
            initSessionFactory();
        }
        return sessionFactory.openSession();

    }

    public static Session getSession(String dbShard) throws ConfigurationException
    {
        if(dbShardToSessionFactory.get(dbShard) == null){
            initSessionFactory(dbShard);
        }
        return dbShardToSessionFactory.get(dbShard).openSession();

    }


    private static synchronized void initSessionFactory() {

        if(TurboConfigProvider.getConfig().getMysql()==null)
            throw new InvalidInvocationException("Default MySQL config isn't provided");

        if(sessionFactory==null)
        {
            AnnotationConfiguration conf = new AnnotationConfiguration();//.addProperties(TurboConfigProvider.getConfig().getMysql()).buildSessionFactory();
            List<Class<?>> annotatedClasses = TurboHelper.getAnnotatedClasses();
            for(Class<?> clazz : annotatedClasses)
            {
                conf.addAnnotatedClass(clazz) ;
            }
            sessionFactory = conf.addProperties(TurboConfigProvider.getConfig().getMysql()).buildSessionFactory();
        }
    }

    private static synchronized void initSessionFactory(String dbShard) throws ConfigurationException{
        if(dbShardToSessionFactory.get(dbShard) == null) {
            AnnotationConfiguration conf = new AnnotationConfiguration();
            List<Class<?>> annotatedClasses = TurboHelper.getAnnotatedClasses();
            for(Class<?> clazz : annotatedClasses)
            {
                conf.addAnnotatedClass(clazz) ;
            }
            if(TurboConfigProvider.getConfig().getDbShard(dbShard) == null ){
                throw new ConfigurationException("Db Shard config isn't provided/invalid"+dbShard);
            }
            dbShardToSessionFactory.put(dbShard,
                    conf.addProperties(TurboConfigProvider.getConfig().getDbShard(dbShard)).buildSessionFactory());
        }
    }

    public static void closeSession(Session session)
    {
        try
        {
            if (session != null)
                session.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


}
