-- MySQL dump 10.13  Distrib 5.5.27-ndb-7.2.8, for linux2.6 (x86_64)
--
-- Host: cloud11    Database: jude
-- ------------------------------------------------------
-- Server version	5.5.27-ndb-7.2.8-cluster-gpl

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `BlockInfo`
--

DROP TABLE IF EXISTS `BlockInfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `BlockInfo` (
  `iNodeID` bigint(20) NOT NULL,
  `blockId` bigint(20) NOT NULL,
  `blockIndex` int(11) DEFAULT NULL,
  `numBytes` bigint(20) DEFAULT NULL,
  `generationStamp` bigint(20) DEFAULT NULL,
  `replication` int(11) DEFAULT NULL,
  `BlockUCState` int(11) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `primaryNodeIndex` int(11) DEFAULT NULL,
  `blockRecoveryId` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`iNodeID`,`blockId`),
  KEY `blockInfo_inodeid_idx` (`iNodeID`),
  KEY `blockInfo_blockid_idx` (`blockId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (iNodeID) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `BlockInfo`
--

LOCK TABLES `BlockInfo` WRITE;
/*!40000 ALTER TABLE `BlockInfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `BlockInfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `BlockTotal`
--

DROP TABLE IF EXISTS `BlockTotal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `BlockTotal` (
  `id` int(11) NOT NULL DEFAULT '0',
  `Total` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `BlockTotal`
--

LOCK TABLES `BlockTotal` WRITE;
/*!40000 ALTER TABLE `BlockTotal` DISABLE KEYS */;
INSERT INTO `BlockTotal` VALUES (1,0);
/*!40000 ALTER TABLE `BlockTotal` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `CorruptReplicas`
--

DROP TABLE IF EXISTS `CorruptReplicas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `CorruptReplicas` (
  `blockId` bigint(20) NOT NULL,
  `storageId` varchar(128) NOT NULL,
  PRIMARY KEY (`blockId`,`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `CorruptReplicas`
--

LOCK TABLES `CorruptReplicas` WRITE;
/*!40000 ALTER TABLE `CorruptReplicas` DISABLE KEYS */;
/*!40000 ALTER TABLE `CorruptReplicas` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Counters`
--

DROP TABLE IF EXISTS `Counters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Counters` (
  `id` int(11) NOT NULL,
  `name` varchar(20) NOT NULL,
  `value` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Counters`
--

LOCK TABLES `Counters` WRITE;
/*!40000 ALTER TABLE `Counters` DISABLE KEYS */;
INSERT INTO `Counters` VALUES (1,'Leader Election',2),(2,'Generation Stamp',0);
/*!40000 ALTER TABLE `Counters` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DatanodeInfo`
--

DROP TABLE IF EXISTS `DatanodeInfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DatanodeInfo` (
  `storageId` varchar(128) NOT NULL,
  `hostname` varchar(25) NOT NULL,
  `localPort` int(11) NOT NULL,
  `infoPort` int(11) NOT NULL,
  `ipcPort` int(11) NOT NULL,
  `status` int(11) DEFAULT '0',
  `location` varchar(25) DEFAULT NULL,
  `host` varchar(25) DEFAULT NULL,
  `numblocks` int(11) DEFAULT '0',
  `currBlocksScheduled` int(11) DEFAULT '0',
  `prevBlocksScheduled` int(11) DEFAULT '0',
  `lastBlocksScheduledRollTime` bigint(20) DEFAULT '0',
  PRIMARY KEY (`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DatanodeInfo`
--

LOCK TABLES `DatanodeInfo` WRITE;
/*!40000 ALTER TABLE `DatanodeInfo` DISABLE KEYS */;
INSERT INTO `DatanodeInfo` VALUES ('DS-1365062404-127.0.0.1-51542-1353342782191','localhost.localdomain',51542,46647,60750,0,'/default-rack','127.0.0.1',0,0,0,1353342782535);
/*!40000 ALTER TABLE `DatanodeInfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DelegationKey`
--

DROP TABLE IF EXISTS `DelegationKey`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DelegationKey` (
  `keyId` int(11) NOT NULL,
  `expiryDate` bigint(20) DEFAULT NULL,
  `keyBytes` varbinary(128) DEFAULT NULL,
  `keyType` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`keyId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DelegationKey`
--

LOCK TABLES `DelegationKey` WRITE;
/*!40000 ALTER TABLE `DelegationKey` DISABLE KEYS */;
/*!40000 ALTER TABLE `DelegationKey` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ExcessReplica`
--

DROP TABLE IF EXISTS `ExcessReplica`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ExcessReplica` (
  `blockId` bigint(20) NOT NULL,
  `storageId` varchar(128) NOT NULL,
  PRIMARY KEY (`blockId`,`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ExcessReplica`
--

LOCK TABLES `ExcessReplica` WRITE;
/*!40000 ALTER TABLE `ExcessReplica` DISABLE KEYS */;
/*!40000 ALTER TABLE `ExcessReplica` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `INodeTableSimple`
--

DROP TABLE IF EXISTS `INodeTableSimple`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `INodeTableSimple` (
  `id` bigint(20) NOT NULL,
  `name` varchar(128) NOT NULL,
  `parentid` bigint(20) NOT NULL,
  `isDir` bit(1) DEFAULT NULL,
  `replication` int(11) DEFAULT NULL,
  `modificationTime` bigint(20) DEFAULT NULL,
  `aTime` bigint(20) DEFAULT NULL,
  `permission` varbinary(128) DEFAULT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `isUnderConstruction` bit(1) DEFAULT NULL,
  `clientName` varchar(45) DEFAULT NULL,
  `clientMachine` varchar(45) DEFAULT NULL,
  `clientNode` varchar(45) DEFAULT NULL,
  `isClosedFile` bit(1) DEFAULT NULL,
  `header` bigint(20) DEFAULT NULL,
  `isDirWithQuota` bit(1) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `dscount` bigint(20) DEFAULT NULL,
  `symlink` varchar(25) DEFAULT NULL,
  `lastBlockId` bigint(20) DEFAULT '-1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_name_parentid` (`name`,`parentid`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (id) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `INodeTableSimple`
--

LOCK TABLES `INodeTableSimple` WRITE;
/*!40000 ALTER TABLE `INodeTableSimple` DISABLE KEYS */;
INSERT INTO `INodeTableSimple` VALUES (0,'',-1,'',NULL,0,0,'jude\nsupergroupí\0\0\0\0\0\0\0\0\0\0\0\0\0\0',2147483647,-1,'\0',NULL,NULL,NULL,'\0',NULL,'',1,0,NULL,-1);
/*!40000 ALTER TABLE `INodeTableSimple` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `InvalidateBlocks`
--

DROP TABLE IF EXISTS `InvalidateBlocks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `InvalidateBlocks` (
  `blockId` bigint(20) NOT NULL,
  `storageId` varchar(128) NOT NULL,
  `generationStamp` bigint(20) DEFAULT NULL,
  `numBytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`blockId`,`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `InvalidateBlocks`
--

LOCK TABLES `InvalidateBlocks` WRITE;
/*!40000 ALTER TABLE `InvalidateBlocks` DISABLE KEYS */;
/*!40000 ALTER TABLE `InvalidateBlocks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Leader`
--

DROP TABLE IF EXISTS `Leader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Leader` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hostname` varchar(25) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Leader`
--

LOCK TABLES `Leader` WRITE;
/*!40000 ALTER TABLE `Leader` DISABLE KEYS */;
/*!40000 ALTER TABLE `Leader` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Lease`
--

DROP TABLE IF EXISTS `Lease`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Lease` (
  `holder` varchar(255) NOT NULL,
  `lastUpdate` bigint(20) DEFAULT NULL,
  `holderID` int(11) DEFAULT NULL,
  PRIMARY KEY (`holder`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Lease`
--

LOCK TABLES `Lease` WRITE;
/*!40000 ALTER TABLE `Lease` DISABLE KEYS */;
/*!40000 ALTER TABLE `Lease` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `LeasePath`
--

DROP TABLE IF EXISTS `LeasePath`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `LeasePath` (
  `holderID` int(11) NOT NULL,
  `path` varchar(255) NOT NULL,
  PRIMARY KEY (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `LeasePath`
--

LOCK TABLES `LeasePath` WRITE;
/*!40000 ALTER TABLE `LeasePath` DISABLE KEYS */;
/*!40000 ALTER TABLE `LeasePath` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PendingReplicationBlock`
--

DROP TABLE IF EXISTS `PendingReplicationBlock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PendingReplicationBlock` (
  `blockId` bigint(20) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  `numReplicasInProgress` int(11) NOT NULL,
  PRIMARY KEY (`blockId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PendingReplicationBlock`
--

LOCK TABLES `PendingReplicationBlock` WRITE;
/*!40000 ALTER TABLE `PendingReplicationBlock` DISABLE KEYS */;
/*!40000 ALTER TABLE `PendingReplicationBlock` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ReplicaUc`
--

DROP TABLE IF EXISTS `ReplicaUc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ReplicaUc` (
  `blockId` bigint(20) NOT NULL,
  `expLocation` varbinary(256) DEFAULT NULL,
  `state` varbinary(256) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `id` int(11) NOT NULL,
  PRIMARY KEY (`blockId`,`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ReplicaUc`
--

LOCK TABLES `ReplicaUc` WRITE;
/*!40000 ALTER TABLE `ReplicaUc` DISABLE KEYS */;
/*!40000 ALTER TABLE `ReplicaUc` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `UnderReplicaBlocks`
--

DROP TABLE IF EXISTS `UnderReplicaBlocks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `UnderReplicaBlocks` (
  `blockId` bigint(20) NOT NULL,
  `level` int(11) NOT NULL,
  PRIMARY KEY (`blockId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `UnderReplicaBlocks`
--

LOCK TABLES `UnderReplicaBlocks` WRITE;
/*!40000 ALTER TABLE `UnderReplicaBlocks` DISABLE KEYS */;
/*!40000 ALTER TABLE `UnderReplicaBlocks` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `triplets`
--

DROP TABLE IF EXISTS `triplets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `triplets` (
  `blockId` bigint(20) NOT NULL,
  `index` int(11) NOT NULL,
  `previousBlockId` bigint(20) DEFAULT NULL,
  `nextBlockId` bigint(20) DEFAULT NULL,
  `storageId` varchar(128) NOT NULL,
  PRIMARY KEY (`blockId`,`storageId`),
  KEY `triplets_storage_idx` (`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (storageId) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `triplets`
--

LOCK TABLES `triplets` WRITE;
/*!40000 ALTER TABLE `triplets` DISABLE KEYS */;
/*!40000 ALTER TABLE `triplets` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-11-19 17:33:44
