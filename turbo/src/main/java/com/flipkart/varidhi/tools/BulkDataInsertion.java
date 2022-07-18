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

package com.flipkart.varidhi.tools;


import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * BulkDataInsertion Script can be used to insert data in bulk in TR format i.e outbound and app tables.
 * Modify insertRecords method according to your need and specify db parameters.
 * total records inserted will be batchSize*iterations*[1-20] where last param is messages per group.
 * Note : This doesn't ensure AUTO_INC across tables,so reset tables AUTO_INC if you want same ids.
 */
public class BulkDataInsertion {

    private static DataSource dataSource(ConnectionDetails connection) {
        String connectionUrl = String.format("jdbc:mysql://%s:3306/%s?user=%s&password=%s",
                connection.getIp(), connection.getDb(), connection.getUser(), connection.getPassword());
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setURL(connectionUrl);
        return mysqlDataSource;
    }

    public static void main(String[] args) {
        try {
            new BulkDataInsertion().bulkDataInsertion(args);
        } catch (Exception e){
            System.out.println("Exception occurred while insertRecords"+e);
        }
    }

    private void bulkDataInsertion(String[] args) throws Exception {
        //change these variables as per ur need
        int batchSize  = 10000;
        int iterations = 400;
        ConnectionDetails outboundConnection = new ConnectionDetails("<ip>","<oubound_db>","<user>","<pass>");
        ConnectionDetails appConnection = new ConnectionDetails("<ip>","<app_db>","<user>","<pass>");
        if(args.length == 10) {
            batchSize  = Integer.parseInt(args[0]);
            iterations = Integer.parseInt(args[1]);
            outboundConnection = new ConnectionDetails(args[2],args[3],args[4],args[5]);
            appConnection = new ConnectionDetails(args[6],args[7],args[8],args[9]);
        } else {
            System.out.println("using default parameters..");
        }
        this.insertRecords(batchSize, iterations, outboundConnection, appConnection);
    }

    private void insertRecords(int batchSize,int iterations,ConnectionDetails outboundConnection,ConnectionDetails appConnection) throws Exception {
        int sleepAfterIteration = 100;

        System.out.println("connecting dbs ..");
        final String showTablesQuery = "SHOW TABLES";
        dataSource(outboundConnection).getConnection().createStatement().executeQuery(showTablesQuery);
        dataSource(appConnection).getConnection().createStatement().executeQuery(showTablesQuery);
        System.out.println("connection successful ..");


        // bulk insertions in batch size
        // per group 1-20 messages with random group id
        for(int i=0;i<iterations;i++) {
            System.out.println("bulk insert iteration .. "+(i+1));
            beginBulkInsert(batchSize,outboundConnection,appConnection);
            Thread.sleep(sleepAfterIteration);
        }
        System.out.println("bulk insertions complete..");

    }

    private void beginBulkInsert(int batchSize, ConnectionDetails outboundConnection, ConnectionDetails appConnection) throws SQLException{
        String insertIntoMetaData = "INSERT INTO message_meta_data (message_id) VALUES ";
        String insertIntoMessages = "INSERT INTO `messages` (`message_id`, `group_id`, `message`, `created_at`, `exchange_name`, `exchange_type`, `app_id`, `http_method`, `http_uri`, `parent_txn_id`, `reply_to`, `reply_to_http_method`, `reply_to_http_uri`, `context`, `updated_at`, `custom_headers`, `transaction_id`, `correlation_id`, `destination_response_status`) VALUES ";
        String messagePayload =  " '{\"requestId\":\"a06c871a-5890-4f28-9699-45893517604e\",\"srId\":631062862779562470,\"taskId\":631063073039465300,\"status\":\"InscanSuccessAtFacility\",\"smIdToStateIdLookup\":{},\"eventMap\":{\"attributes\":[{\"id\":null,\"active\":null,\"tag\":null,\"shardKey\":null,\"referenceId\":null,\"externalEntityIdentifier\":null,\"createdBy\":null,\"updatedBy\":null,\"createdAt\":null,\"updatedAt\":null,\"name\":\"shipmentId\",\"type\":\"string\",\"value\":\"FMPP0191866127\"}]},\"trackingData\":{\"id\":null,\"active\":null,\"tag\":null,\"shardKey\":null,\"referenceId\":null,\"externalEntityIdentifier\":null,\"createdBy\":null,\"updatedBy\":null,\"createdAt\":null,\"updatedAt\":null,\"orchestratorTaskId\":\"631063073039465300\",\"status\":\"InscanSuccessAtFacility\",\"location\":\"154\",\"time\":\"2018-09-19T00:34:16+05:30\",\"originSystem\":\"MH\",\"trackingAttributes\":null,\"plannerTaskId\":null,\"user\":null,\"note\":null,\"reason\":null},\"suppressError\":false,\"user\":null,\"originSystem\":\"MH\",\"note\":null,\"reason\":null,\"name\":\"InscanSuccessAtFacility\",\"location\":\"154\",\"time\":1537297456000}', '2018-09-19 00:34:17', 'ekart.il.event.production', 'topic', 'Ekart E2E Orchestrator', 'POST', '', NULL, '', '', NULL, '', '2018-09-19 00:34:17', '{\"X_EVENT_NAME\":\"InscanSuccessAtFacility\",\"X-VARADHI-MOCK\":\"true\",\"content-type\":\"application/json\"}', 'TXN-9e388b91-14d8-4014-aef7-c9aa5f331968', NULL, NULL)";

        try {
            List<MessageGroup> list = getBulkInsertStatement(batchSize);

            StringBuilder sbMetaData = new StringBuilder();
            StringBuilder sbMessages = new StringBuilder();
            sbMetaData.append(insertIntoMetaData);
            sbMessages.append(insertIntoMessages);
            for (MessageGroup mg : list) {
                sbMetaData.append("('").append(mg.messageId).append("'),");
                sbMessages.append("('").append(mg.messageId).append("','").
                        append(mg.groupId).append("',").append(messagePayload).append(",");
            }
            sbMetaData.deleteCharAt(sbMetaData.length() - 1);
            sbMessages.deleteCharAt(sbMessages.length() - 1);


            String queryOutbound =  sbMessages.toString();
            String queryApp = sbMetaData.toString();

            Connection connectionOutbound = dataSource(outboundConnection).getConnection();
            connectionOutbound.createStatement().execute(queryOutbound);

            Connection connectionApp = dataSource(appConnection).getConnection();
            connectionApp.createStatement().execute(queryApp);

            connectionApp.close();
            connectionOutbound.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AllArgsConstructor
    @Getter
    private class ConnectionDetails{
        String ip;
        String db;
        String user;
        String password;
    }

    @AllArgsConstructor
    private class MessageGroup {
        String messageId;
        int groupId;
    }

    private List<MessageGroup> getBulkInsertStatement(int batchSize) {
        Random random = new Random();
        List<MessageGroup> list = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int groupId = Math.abs(random.nextInt());
            int numberOfMessagesInGroup = Math.abs(random.nextInt(20))+1;
            for (int j = 0; j < numberOfMessagesInGroup; j++) {
                list.add(new MessageGroup(generateRandomChars(20), groupId));
            }
        }
        return list;
    }

    private static String generateRandomChars(int length) {
        StringBuilder sb = new StringBuilder();
        String candidateChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(candidateChars.charAt(random.nextInt(candidateChars.length())));
        }
        return sb.toString();
    }

}
