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

package com.flipkart.varidhi.relayer.schemavalidator.validator;

import com.flipkart.varidhi.relayer.RelayerApplicationRepository;
import com.flipkart.varidhi.relayer.RelayerOutboundRepository;
import com.flipkart.varidhi.relayer.schemavalidator.models.ColumnDetails;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchema;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaCharset;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBSchemaDiff;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider.TableType;
import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DBSchemaValidator {
    private static final Logger logger = LoggerFactory.getLogger(DBSchemaValidator.class);
    private final ExchangeTableNameProvider exchangeTableNameProvider;
    private RelayerApplicationRepository applicationRepository;
    private RelayerOutboundRepository outboundRepository;


    public DBSchemaValidator(ExchangeTableNameProvider exchangeTableNameProvider,
                             RelayerApplicationRepository applicationRepository, RelayerOutboundRepository outboundRepository) {
        this.exchangeTableNameProvider = exchangeTableNameProvider;
        this.applicationRepository = applicationRepository;
        this.outboundRepository = outboundRepository;
    }

    public Map<String,Set<String>> validateCharSet() {
        Map<String,Set<String>> charset = new HashMap<>();
        boolean mismatchFound = false;
        try {
            logger.info("DBSchemaValidator, starting validateCharSet");
            List<String> tableNames = new ArrayList<>();
            for(TableType tableType: TableType.values()){
                tableNames.add(exchangeTableNameProvider.getTableName(tableType));
            }
            List<DBSchemaCharset> charsetList = outboundRepository.getTablesCharSet(tableNames);

            String charSet = !CollectionUtils.isEmpty(charsetList) ? charsetList.get(0).getCharSetName() : null;
            for(DBSchemaCharset dbSchemaCharset : charsetList){
                if (charSet !=null && !dbSchemaCharset.getCharSetName().equals(charSet)){
                    mismatchFound = true;
                }
                Set<String> set = charset.getOrDefault(dbSchemaCharset.getTableName(),new HashSet<>());
                set.add(dbSchemaCharset.getCharSetName());
                charset.put(dbSchemaCharset.getTableName(),set);
            }

            logger.info("DBSchemaValidator, validateCharSet complete");
        } catch (Exception e) {
            logger.error("Error occurred while doing validateCharSet");
        }
        return mismatchFound ? charset : new HashMap<>();
    }

    public List<DBSchemaDiff> validateOutboundSchema() {
        List<DBSchemaDiff> schemaDiffList = new ArrayList<>();
        try {
            logger.info("DBSchemaValidator, starting for validation outbound schema ");
            for(TableType tableType: TableType.values()){
                if(tableType == TableType.MESSAGE_META_DATA){
                    continue;
                }
                logger.info("DBSchemaValidator, validating schema for "+exchangeTableNameProvider.getTableName(tableType));
                DBSchema dbSchema = outboundRepository.getTableDescription(exchangeTableNameProvider.getTableName(tableType));
                if(dbSchema == null){
                    logger.error("DBSchemaValidator, couldn't find table "+exchangeTableNameProvider.getTableName(tableType));
                    continue;
                }
                DBSchemaDiff schemaDiff = validateSchema(getDBSchema(tableType), dbSchema);
                if(schemaDiff != null){
                    schemaDiffList.add(schemaDiff);
                }
            }
            logger.info("DBSchemaValidator, validation complete for outbound schema ");
        } catch (Exception e) {
            logger.error("Error occurred while doing schema validation");
        }
        return schemaDiffList;
    }

    public List<DBSchemaDiff> validateAppSchema() {
        Session session = null;
        List<DBSchemaDiff> schemaDiffList = new ArrayList<>();
        try {
            logger.info("DBSchemaValidator, starting for app schema ");

            TableType tableType = TableType.MESSAGE_META_DATA;
            logger.info("DBSchemaValidator, validating schema for "+exchangeTableNameProvider.getTableName(tableType));
            DBSchema dbSchema = applicationRepository.getTableDescription(exchangeTableNameProvider.getTableName(tableType));
            if(dbSchema == null){
                throw new Error("DBSchemaValidator, couldn't find table "+exchangeTableNameProvider.getTableName(tableType));
            }
            DBSchemaDiff schemaDiff = validateSchema(getDBSchema(tableType), dbSchema);
            if(schemaDiff != null){
                schemaDiffList.add(schemaDiff);
            }

            logger.info("DBSchemaValidator, validation complete for app schema ");
        } catch (Exception e) {
            logger.error("Error occurred while doing schema validation");
        } finally {
            closeSession(session);
        }
        return schemaDiffList;
    }

    private DBSchemaDiff validateSchema(DBSchema expectedSchema,DBSchema actualSchema){
        boolean validSchema = true;
        if (actualSchema.getColumnDetails().size() == 0){
            logger.error("Schema Validator, table not found : "+actualSchema.getTableName());
        }
        DBSchema expectedSchemaDiff = new DBSchema(actualSchema.getTableName());
        DBSchema actualSchemaDiff = new DBSchema(actualSchema.getTableName());
        for(Map.Entry<String,ColumnDetails> entry : expectedSchema.getColumnDetails().entrySet()){
            ColumnDetails actualColumn = actualSchema.getColumnDetails().get(entry.getKey());
            if(!entry.getValue().equals(actualColumn)){
                validSchema = false;
                expectedSchemaDiff.addColumn(entry.getKey(),entry.getValue());
                actualSchemaDiff.addColumn(entry.getKey(),actualColumn);
                logger.error("Schema Validator, column ["+entry.getKey()+"] mismatch for table : " + actualSchema.getTableName() +
                        ", EXPECTED :: "+entry.getValue()+" ACTUAL :: "+actualColumn);
            }
        }
        if(!validSchema){
            return new DBSchemaDiff(actualSchema.getTableName(),expectedSchemaDiff,actualSchemaDiff);
        }
        return null;
    }


    public DBSchema getDBSchema(ExchangeTableNameProvider.TableType tableType) {
        DBSchema schema;
        switch (tableType) {
            case MESSAGE:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.MESSAGE.toString());
                schema.addColumn("id","bigint(20)","NO","PRI",null,"auto_increment");
                schema.addColumn("message_id","varchar(100)","NO","MUL",null,"");
                schema.addColumn("message","mediumtext","YES","",null,"");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("exchange_name","varchar(100)","YES","",null,"");
                schema.addColumn("exchange_type","varchar(20)","YES","","queue","");
                schema.addColumn("app_id","varchar(100)","YES","",null,"");
                schema.addColumn("group_id","varchar(100)","YES","",null,"");
                schema.addColumn("http_method","varchar(10)","YES","",null,"");
                schema.addColumn("http_uri","varchar(4096)","YES","",null,"");
                schema.addColumn("parent_txn_id","varchar(100)","YES","",null,"");
                schema.addColumn("reply_to","varchar(100)","YES","",null,"");
                schema.addColumn("reply_to_http_method","varchar(10)","YES","",null,"");
                schema.addColumn("reply_to_http_uri","varchar(4096)","YES","",null,"");
                schema.addColumn("context","text","YES","",null,"");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","on update CURRENT_TIMESTAMP");
                schema.addColumn("custom_headers","text","YES","",null,"");
                schema.addColumn("transaction_id","varchar(100)","YES","",null,"");
                schema.addColumn("correlation_id","varchar(100)","YES","",null,"");
                schema.addColumn("destination_response_status","int(11)","YES","",null,"");
                return schema;
            case MAX_PROCESSED_MESSAGE_SEQUENCE:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.MAX_PROCESSED_MESSAGE_SEQUENCE.toString());
                schema.addColumn("id","int(11)","NO","PRI",null,"auto_increment");
                schema.addColumn("process_id","varchar(100)","NO","",null,"");
                schema.addColumn("message_id","varchar(100)","YES","",null,"");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","on update CURRENT_TIMESTAMP");
                schema.addColumn("active","tinyint(1)","YES","","1","");
                return schema;
            case CONTROL_TASKS:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.CONTROL_TASKS.toString());
                schema.addColumn("id","int(11)","NO","PRI",null,"auto_increment");
                schema.addColumn("task_type","varchar(100)","NO","",null,"");
                schema.addColumn("group_id","varchar(100)","YES","",null,"");
                schema.addColumn("message_id","varchar(100)","YES","",null,"");
                schema.addColumn("status","varchar(20)","YES","MUL",null,"");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("from_date","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("to_date","timestamp","NO","","CURRENT_TIMESTAMP","");
                return schema;
            case SIDELINED_GROUPS:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.SIDELINED_GROUPS.toString());
                schema.addColumn("id","int(11)","NO","PRI",null,"auto_increment");
                schema.addColumn("group_id","varchar(100)","YES","MUL",null,"");
                schema.addColumn("status","varchar(20)","YES","",null,"");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","on update CURRENT_TIMESTAMP");
                return schema;
            case SIDELINED_MESSAGES:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.SIDELINED_MESSAGES.toString());
                schema.addColumn("id","int(11)","NO","PRI",null,"auto_increment");
                schema.addColumn("group_id","varchar(100)","YES","MUL",null,"");
                schema.addColumn("message_id","varchar(100)","YES","MUL",null,"");
                schema.addColumn("status","varchar(20)","YES","MUL",null,"");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","on update CURRENT_TIMESTAMP");
                schema.addColumn("http_status_code","int(11)","YES","",null,"");
                schema.addColumn("sideline_reason_code","varchar(255)","YES","",null,"");
                schema.addColumn("retries","int(11)","YES","","0","");
                schema.addColumn("details","text","YES","",null,"");
                return schema;
            case SKIPPED_IDS:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.SKIPPED_IDS.toString());
                schema.addColumn("id","bigint(20)","NO","PRI",null,"auto_increment");
                schema.addColumn("message_seq_id","bigint(20)","YES","MUL",null,"");
                schema.addColumn("status","varchar(20)","YES","",null,"");
                schema.addColumn("retry_count","int(3)","YES","","0","");
                schema.addColumn("updated_at","timestamp","NO","","CURRENT_TIMESTAMP","on update CURRENT_TIMESTAMP");
                schema.addColumn("created_at","timestamp","NO","MUL","CURRENT_TIMESTAMP","");
                return schema;
            case MESSAGE_META_DATA:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.MESSAGE_META_DATA.toString());
                schema.addColumn("id","bigint(20)","NO","PRI",null,"auto_increment");
                schema.addColumn("message_id","varchar(100)","NO","MUL",null,"");
                schema.addColumn("created_at","timestamp","NO","","CURRENT_TIMESTAMP","");
                return schema;
            case LEADER_ELECTION:
                schema = new DBSchema(ExchangeTableNameProvider.TableType.LEADER_ELECTION.toString());
                schema.addColumn("anchor","tinyint(3) unsigned","NO","PRI",null,"");
                schema.addColumn("relayer_uid","varchar(255)","NO","",null,"");
                schema.addColumn("lease_expiry_time","timestamp","NO","","CURRENT_TIMESTAMP","");
                return schema;
            default:
                return null;

        }
    }

    private void closeSession(Session session) {
        try {
            if (session != null)
                session.close();
        } catch (Exception e) {
            logger.error("Error in Closing Session:" + e.getMessage(), e);
        }
    }
}
