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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

public final class LoggerUtil {

    private static final String MESSAGEID = "MESSAGEID:";
    private static final String GROUPID = " GROUPID:";
    private static final String ACTION = " ACTION:";
    private static final String STATUS = " STATUS:";
    private static final String PAYLOAD = " PAYLOAD:";

    public static final String APPLICATION_LOGGER = "com.flipkart.varidhi";
    public static final String RELAYING_MESSAGE_LOGGER = "relayingMessageLogger";

    public static String generateLogInLogSvcFormat(String messageId , String groupId , String action ,  String status ,String payload){
        return MESSAGEID + messageId + GROUPID + groupId + ACTION + action + STATUS + status + PAYLOAD + payload;
    }

    /**
     * only to be called at the start of the application
     * @param logLevel
     * @param logger
     */
    public static void setLogLevel(String logger, String logLevel) {
        LoggerContext lc = (LoggerContext)LoggerFactory.getILoggerFactory();
        lc.getLogger(logger).setLevel(Level.toLevel(logLevel));
    }
}
