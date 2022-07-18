# Turbo Relayer

## **Background**

We use Kafka as a message bus to send the messages between the services. Let’s assume Service ‘A’ receives a request. As part of that request, Service ‘A’ does some processing, including its database update, and sends the request to Message Broker (in this case, Kafka) for Service B.

Our problem statement:

How do we maintain atomicity between database update in Service A and the event propagation to Service B?
Our challenge was to handle the failure scenarios seamlessly across the org, where we wanted to ensure that when a write to the database fails, we revert the writes to the Message Broker as well and vice versa.

Some of the secondary goals were: 

1. Ensure zero message loss while sending a message from Service A to Service B/MessageBroker.
2. Handle long-running transactions/rollback of primary entities.
3. Handle failures gracefully while pushing messages to Message Brokers.
4. Minimize Relay lag while relaying messages from the Outbox table.
5. Provide order guarantee while relaying the messages of an aggregate/group of entities.


## **Solution**

* Service A writes the primary entity to the Primary Database and the event/messages in Outbound database. Let’s call the primary entity/table which maintains the state as PRIMARY_ENTITY and the outbound entity which stores events as MESSAGES.
* There is a separate Relayer/Poller process (Turbo relayer) that reads these events from the MESSAGES and sends them to the message broker responsible for sending them to Service B.

<img width="755" alt="Screenshot 2022-05-10 at 3 32 34 PM" src="https://github.fkinternal.com/storage/user/1228/files/1e9d8200-d076-11ec-80fd-269875762716">

In this repo, we will cover the 2 components: 

1. Turbo Client Library which is responsible to write messages to Outbound database and write message metadata to Primary Database. 
2. Turbo relayer component which is responsible to read the messages from Outbound database and relay it to the another Service / Message broker. 


### **SQL Schema**

Turbo Relayer component creates all the required tables in Outbound DB auomatically when you deploy it in the container and start the process. 

Sample sql schema for message_meta_data table in Primary DB : 

```xml
CREATE TABLE `message_meta_data` (
  `id` bigint(20) NOT NULL,
  `message_id` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `message_id` (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (id)
(PARTITION p823100000 VALUES LESS THAN (823100000) ENGINE = InnoDB,
 PARTITION p823200000 VALUES LESS THAN (823200000) ENGINE = InnoDB)
```

Sample sql schema for messages table in Outbound DB is : 

```xml
CREATE TABLE `messages` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `app_id` varchar(255) DEFAULT NULL,
  `context` varchar(255) DEFAULT NULL,
  `correlation_id` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `custom_headers` mediumtext,
  `destination_response_status` int(11) DEFAULT NULL,
  `exchange_name` varchar(255) DEFAULT NULL,
  `exchange_type` varchar(255) DEFAULT NULL,
  `group_id` varchar(255) DEFAULT NULL,
  `http_method` varchar(255) DEFAULT NULL,
  `http_uri` varchar(4096) DEFAULT NULL,
  `message` mediumtext,
  `message_id` varchar(255) DEFAULT NULL,
  `reply_to` varchar(255) DEFAULT NULL,
  `reply_to_http_method` varchar(255) DEFAULT NULL,
  `reply_to_http_uri` varchar(255) DEFAULT NULL,
  `transaction_id` varchar(255) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `messages_message_id` (`message_id`),
  KEY `messages_group_id` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (id)
(PARTITION p823100000 VALUES LESS THAN (823100000) ENGINE = InnoDB,
 PARTITION p823200000 VALUES LESS THAN (823200000) ENGINE = InnoDB)
```


# Turbo Java Client
=================


#### About
Java client for Turbo relayer 

#### Dependency
```
        <dependency>
            <groupId>com.flipkart.restbus.hibernate</groupId>
            <artifactId>turbo-client</artifactId>
            <version>{turbo-client-version}</version>
        </dependency>
   
```
#### Usage
- Initialize client by providing a [config file](https://github.fkinternal.com/Flipkart/turbo-client/blob/master/hibernate-restbus-client/sample_config.yml), details are given inline.
- Config file needs to be present on `/etc/fk-sc-mq/turbo.yml`, which you can override using SYSTEM property `SC_MQ_TURBO_CONFIG="/anyLocation"`.
- After initializing config properly call .persist method as shown below.
```
    String exchangeName = "exchangeName";
    String exchangeType = "queue";
    String methodType = "POST";
    String httpUri = "http://flipkart.com";
    String replyToExchangeName = null;
    String replyToHttpMethod = "GET";
    String replyToHttpUri = "http://flipkart.com/__reply__to";
    String payload = "{ \"test\": 123 }";
    String groupId = "T1";
    Map<String, String> options = new HashMap<String, String>();
    String appId = "hibernate-restbus-client";
    OutboundMessage outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
            methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
            payload, groupId, options, appId);

    // begin transaction has to be called for this session anytime before this call
    ..
    //session is of type org.hibernate.Session
    OutboundMessageRepositoryFactory.getOutboundMessageRepository(session).persist(outboundMessage);
    ..
    // commit transaction has to be called for this session anytime before this call
    
   
    Similarly you can construct turbo specific messages using
    TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                    methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                    payload, groupId, options, appId)

```

#### Things to look out for

- Schema creation will be managed by turbo app entirely, so properly configure it on turbo app side.
- Turbo only support hibernate4.x version, for other hibernate versions you can fork out a branch and contribute the same.
- If you are overriding turbo config using `TurboConfig.setConfig(overridenTurboConfig)`, make you enable autocommit in that config also. For example if you are using hibernate set `hibernate.connection.autocommit` to `true` while providing config.


#### Contributing

1. Fork the branch.
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request


### **Contributors**

1. Dhruvik Shah
2. Umang Jain
3. Rahul Agrawal
