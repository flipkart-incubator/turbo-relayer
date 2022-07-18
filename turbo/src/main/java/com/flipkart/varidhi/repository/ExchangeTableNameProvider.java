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

package com.flipkart.varidhi.repository;

/*
 * *
 * Author: abhinavp
 * Date: 31-Jul-2015
 *
 */
public class ExchangeTableNameProvider {
    private final String MESSAGES = "messages";
    private final String MAX_PROCESSED_MESSAGE_SEQUENCE = "max_processed_message_sequence";
    private final String CONTROL_TASKS = "control_tasks";
    private final String SIDELINED_GROUPS = "sidelined_groups";
    private final String SIDELINED_MESSAGES = "sidelined_messages";
    private final String SKIPPED_IDS = "skipped_ids";
    private final String MESSAGE_META_DATA = "message_meta_data";
    private final String LEADER_ELECTION = "leader_election";
    String exchangeName = null;
    public ExchangeTableNameProvider(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getTableName(TableType tableType) {
        switch (tableType) {
            case MESSAGE:
                return getTableNameForExchange(MESSAGES);
            case MAX_PROCESSED_MESSAGE_SEQUENCE:
                return getTableNameForExchange(MAX_PROCESSED_MESSAGE_SEQUENCE);
            case CONTROL_TASKS:
                return getTableNameForExchange(CONTROL_TASKS);
            case SIDELINED_GROUPS:
                return getTableNameForExchange(SIDELINED_GROUPS);
            case SIDELINED_MESSAGES:
                return getTableNameForExchange(SIDELINED_MESSAGES);
            case SKIPPED_IDS:
                return getTableNameForExchange(SKIPPED_IDS);
            case MESSAGE_META_DATA:
                return getTableNameForExchange(MESSAGE_META_DATA);
            case LEADER_ELECTION:
                return getTableNameForExchange(LEADER_ELECTION);
            default:
                return null;

        }
    }

    private String getTableNameForExchange(String tableName) {
        return exchangeName == null || "".equals(exchangeName) ?
            tableName :
            exchangeName + "_" + tableName;
    }

    public enum TableType {
        MESSAGE,
        MAX_PROCESSED_MESSAGE_SEQUENCE,
        CONTROL_TASKS,
        SIDELINED_GROUPS,
        SIDELINED_MESSAGES,
        SKIPPED_IDS,
        MESSAGE_META_DATA,
        LEADER_ELECTION
    }

}
