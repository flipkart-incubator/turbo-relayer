/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `control_tasks` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_type` varchar(100) NOT NULL,
  `group_id` varchar(100) DEFAULT NULL,
  `message_id` varchar(100) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `from_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `to_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `max_processed_message_sequence` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `process_id` varchar(100) NOT NULL,
  `message_id` varchar(100) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `active` tinyint(1) DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4505 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `messages` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `message_id` varchar(100) DEFAULT NULL,
  `message` mediumtext,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `exchange_name` varchar(100) DEFAULT NULL,
  `exchange_type` varchar(20) DEFAULT 'queue',
  `app_id` varchar(100) DEFAULT NULL,
  `group_id` varchar(100) DEFAULT NULL,
  `http_method` varchar(10) DEFAULT NULL,
  `http_uri` varchar(4096) DEFAULT NULL,
  `parent_txn_id` varchar(100) DEFAULT NULL,
  `reply_to` varchar(100) DEFAULT NULL,
  `reply_to_http_method` varchar(10) DEFAULT NULL,
  `reply_to_http_uri` varchar(4096) DEFAULT NULL,
  `context` text,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `custom_headers` text,
  `transaction_id` varchar(100) DEFAULT NULL,
  `correlation_id` varchar(100) DEFAULT NULL,
  `destination_response_status` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `message_id` (`message_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1
PARTITION BY RANGE (
PARTITION p0 VALUES LESS THAN (1000000),
PARTITION p1 VALUES LESS THAN (2000000),
PARTITION p2 VALUES LESS THAN (3000000),
PARTITION p4 VALUES LESS THAN (4000000),
PARTITION p5 VALUES LESS THAN (5000000),
PARTITION p6 VALUES LESS THAN (6000000),
PARTITION p7 VALUES LESS THAN (7000000),
PARTITION p8 VALUES LESS THAN (8000000),
PARTITION p9 VALUES LESS THAN (9000000),
PARTITION p10 VALUES LESS THAN (10000000),
PARTITION p11 VALUES LESS THAN (11000000)
);
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sidelined_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(100) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sidelined_messages` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(100) DEFAULT NULL,
  `message_id` varchar(100) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `http_status_code` int(11) DEFAULT NULL,
  `sideline_reason_code` varchar(255) DEFAULT NULL,
  `retries` int(11) DEFAULT '0',
  `details` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1163 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `skipped_ids` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `message_seq_id` int(11) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `retry_count` int(3) DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
