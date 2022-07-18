
import time
import socket
import fcntl
import struct
import MySQLdb #either use MySQLdb/pymysql
import traceback
import string
import random
from locust import HttpLocust,  TaskSet, task



def get_messages(db):
                cursor = db.cursor()
                cursor.execute("SELECT * FROM messages LIMIT 10")
                rows = cursor.fetchall()
                print(rows)
                print("-----------")
                inserted_ids = []
                for row in rows:
                                inserted_ids.add(row[0])
                                for col in row:
                                                print("%s," % col)
                                print("\n")
                cursor.close()



def beginBulkInsert(batchSize, db_app, db_outbound):
                response = "data inserted"
                outbound_cursor = db_outbound.cursor()
                app_cursor = db_app.cursor()
                insertIntoMetaData = "INSERT INTO __message_meta_data (message_id) VALUES "
                insertIntoMessages = "INSERT INTO `__messages` (`message_id`, `group_id`, `message`, `created_at`, `exchange_name`, `exchange_type`, `app_id`, `http_method`, `http_uri`, `parent_txn_id`, `reply_to`, `reply_to_http_method`, `reply_to_http_uri`, `context`, `updated_at`, `custom_headers`, `transaction_id`, `correlation_id`, `destination_response_status`) VALUES "
                messagePayload =  " '{\"requestId\":\"a06c871a-5890-4f28-9699-45893517604e\",\"srId\":631062862779562470,\"taskId\":631063073039465300,\"status\":\"InscanSuccessAtFacility\",\"smIdToStateIdLookup\":{},\"eventMap\":{\"attributes\":[{\"id\":null,\"active\":null,\"tag\":null,\"shardKey\":null,\"referenceId\":null,\"externalEntityIdentifier\":null,\"createdBy\":null,\"updatedBy\":null,\"createdAt\":null,\"updatedAt\":null,\"name\":\"shipmentId\",\"type\":\"string\",\"value\":\"FMPP0191866127\"}]},\"trackingData\":{\"id\":null,\"active\":null,\"tag\":null,\"shardKey\":null,\"referenceId\":null,\"externalEntityIdentifier\":null,\"createdBy\":null,\"updatedBy\":null,\"createdAt\":null,\"updatedAt\":null,\"orchestratorTaskId\":\"631063073039465300\",\"status\":\"InscanSuccessAtFacility\",\"location\":\"154\",\"time\":\"2018-09-19T00:34:16+05:30\",\"originSystem\":\"MH\",\"trackingAttributes\":null,\"plannerTaskId\":null,\"user\":null,\"note\":null,\"reason\":null},\"suppressError\":false,\"user\":null,\"originSystem\":\"MH\",\"note\":null,\"reason\":null,\"name\":\"InscanSuccessAtFacility\",\"location\":\"154\",\"time\":1537297456000}', '2018-09-19 00:34:17', 'ekart.il.event.production', 'topic', 'Ekart E2E Orchestrator', 'POST', '', NULL, '', '', NULL, '', NOW(), '{\"X_EVENT_NAME\":\"InscanSuccessAtFacility\",\"X-VARADHI-MOCK\":\"true\",\"content-type\":\"application/json\"}', 'TXN-9e388b91-14d8-4014-aef7-c9aa5f331968', NULL, NULL)"

                try :
                                mg_list = getBulkInsertStatement(batchSize)


                                sbMetaData = "" + insertIntoMetaData
                                sbMessages = "" + insertIntoMessages
                                
                                records = 0
                                for mg in mg_list :
                                                records = records + 1
                                                sbMetaData = sbMetaData + "('" + mg.messageId + "'),"
                                                sbMessages = sbMessages + "('" + mg.messageId + "','" + str(mg.groupId) + "'," + messagePayload + ","
                                
                                sbMetaData = sbMetaData[:-1]
                                sbMessages = sbMessages[:-1]
                                queryOutbound = sbMessages
                                queryApp = sbMetaData

                                #print("---queryOutbound---")
                                #print(queryOutbound)
                                #print("---queryApp---")
                                #print(queryApp)

                                outbound_cursor.execute(queryOutbound)
                                app_cursor.execute(queryApp)
                                response = "records inserted : " + str(records)


                except:
                                response = traceback.format_exc()
                                print(response)
                                raise
                finally:
                                app_cursor.close()
                                outbound_cursor.close()
                                return response




class MessageGroup:  
                def __init__(self,messageId,groupId):  
                                self.messageId = messageId
                                self.groupId = groupId  


def getBulkInsertStatement(batchSize):
                mg_list = []
                for i in range(batchSize):
                                groupId = random.randint(1,2147483647) 
                                numberOfMessagesInGroup = random.randint(1,10)
                                for j in range(numberOfMessagesInGroup):
                                                mg_list.append(MessageGroup(generateRandomChars(20), groupId))
                return mg_list

def generateRandomChars(size=20, chars=string.ascii_uppercase + string.digits):
                return ''.join(random.choice(chars) for _ in range(size))



class BulkInsertTask(TaskSet):

        @task
        def insert_content(self):
                db_outbound = MySQLdb.connect("<ip>", "<user>", "<password>", "<outbound_db>")
                db_app = MySQLdb.connect("<ip>", "<user>", "<password>", "app_db")
                db_outbound.autocommit(True)
                db_app.autocommit(True)                
                print(beginBulkInsert(100,db_app,db_outbound))
                db_outbound.close()
                db_app.close()



class WebsiteUser(HttpLocust):
        task_set = BulkInsertTask
        min_wait = 20
        max_wait = 50
        host  = "https://127.0.0.1"
        headers = {"Content-Type" :"application/json"}