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

/**
 * Created by ashudeep.sharma on 20/09/16.
 */
public class Constants {
    public static final long MILLISECONDS_TO_MIN_FACTOR = 60000;
    public static final String VARADHI_AUTH_TARGET_CLIENT_ID = "http://@@HOST@@:80";
    public static final String TURBO_REDIRECT_URL = "http://@@HOST@@:36005/@@PATH@@";
    public static final String URL_PATH = "@@PATH@@";
    public static final String HOST = "@@HOST@@";
    public static final String TABLE_IDENTIFIER = "@@TABLE_NAME@@";
    public static final String PARTITION_IDENTIFIER = "@@PARTITION_NAME@@";
    public static final String PARTITION_VALUE = "@@PARTITION_VALUE@@";
    public static final String DROP_PARTITION_QUERY =
        "ALTER TABLE @@TABLE_NAME@@ DROP PARTITION @@PARTITION_NAME@@";
    public static final String CREATE_PARTITION_QUERY =
        "ALTER TABLE @@TABLE_NAME@@ ADD PARTITION (PARTITION @@PARTITION_NAME@@ VALUES LESS THAN (@@PARTITION_VALUE@@))";
    public static final String STATE_IDENTIFIER = "@@STATE_IDENTIFIER@@";
    public static final String QUERY_IDENTIFIER = "@@QUERY_IDENTIFIER@@";
    public static final String DEADLOCK_DETECTION_QUERY =
        "SELECT ID,TIME FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='QUERY' AND STATE='@@STATE_IDENTIFIER@@' AND INFO = '@@QUERY_IDENTIFIER@@'";
    public static final String QUERY_ID = "@@QUERY_ID@@";
    public static final String KILL_QUERY_COMMAND = "KILL QUERY @@QUERY_ID@@";
    public static final String LIMIT_CLAUSE = " LIMIT 1";
    public static final String PARTITION_START_IDENTIFIER = "@@PARTITION_START_ID@@";
    public static final String OPERATOR_IDENTIFIER = "@@OPERATOR@@";
    public static final String DATABASE_IDENTIFIER = "@@DB_NAME@@";
    public static final String PARTITION_END_IDENTIFIER = "@@PARTITION_END_ID@@";
    public static final String GET_PARTITION_QUERY =
        "SELECT @@PARTITION_START_ID@@ , PARTITION_DESCRIPTION, PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '@@DB_NAME@@' AND TABLE_NAME = '@@TABLE_NAME@@' AND CAST(PARTITION_DESCRIPTION AS UNSIGNED) @@OPERATOR@@ @@PARTITION_END_ID@@ ORDER by CAST(PARTITION_DESCRIPTION AS UNSIGNED)";
    public static final String MESSAGE_TABLE = "@@MESSAGES_TABLE@@";
    public static final String SIDELINE_TABLE = "@@SIDELINE_TABLE@@";
    public static final String MAX_SEQ_TABLE = "@@MAX_SEQ_TABLE@@";
    public static final String START_ID = "@@START_ID@@";
    public static final String END_ID = "@@END_ID@@";
    public static final String HAS_SIDELINE_MESSAGES_QUERY =
        "SELECT message.id FROM @@MESSAGES_TABLE@@ message JOIN @@SIDELINE_TABLE@@ sideline ON message.message_id = sideline.message_id WHERE message.id BETWEEN @@START_ID@@ AND @@END_ID@@ LIMIT 1";
    public static final String HAS_PENDING_MESSAGES_QUERY =
        "SELECT message.id FROM @@MESSAGES_TABLE@@ message JOIN @@MAX_SEQ_TABLE@@ ms ON message.message_id = ms.message_id WHERE message.id < @@END_ID@@ LIMIT 1";
    public static final String GET_LAST_MESSAGEID_QUERY =
        "SELECT id FROM @@MESSAGES_TABLE@@ ORDER BY id DESC LIMIT 1";
    public static final String GET_FIRST_MESSAGEID_QUERY =
        "SELECT id FROM @@MESSAGES_TABLE@@ ORDER BY id LIMIT 1";
    public static final String GET_PARTITION_MAX_ID_QUERY =
        "SELECT MAX(CAST(partition_description AS UNSIGNED)) from INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '@@DB_NAME@@' AND TABLE_NAME = '@@TABLE_NAME@@'";
    public static final String DUPLICATE_PARTITION_ERROR_MESSAGE =
        "duplicate partition name @@PARTITION_NAME@@";
    public static final int KILL_QUERY_ERROR_CODE = 1317;
    public static final String DEFAULT_CONFIG_FILE = "config/relayer.yml";
    public static final String DEFAULT_SECRETS_CONFIG_FILE = "config/secrets";
    public static final String DEFAULT_MYSQL_PASSWORD_PATTERN = "%s/mysql.%s.password";
    public static final String HIBERNATE_CONNECTION_PASSWORD = "hibernate.connection.password";
    public static final String MOCK_HEADER = "X-MOCK-HEADER";
    public static final String VELOCITY_TEMPLATE_PATH = "templates/template.vm";
    public static final String DEFAULT_ENCODING = "ISO-8859-1";
}
