delimiter $$



CREATE TABLE `triplets` (

  `blockId` bigint(20) NOT NULL,

  `index` int(11) NOT NULL,

  `datanodeName` varchar(128) DEFAULT NULL,

  `storageId` varchar(128) DEFAULT NULL,

  `previousBlockId` bigint(20) DEFAULT NULL,

  `nextBlockId` bigint(20) DEFAULT NULL,

  PRIMARY KEY (`blockId`,`index`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `ReplicaUc` (

  `blockId` bigint(20) NOT NULL,

  `expLocation` varbinary(256) DEFAULT NULL,

  `state` varbinary(256) DEFAULT NULL,

  `timestamp` bigint(20) DEFAULT NULL,

  `id` int(11) NOT NULL,

  PRIMARY KEY (`blockId`,`id`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `LeasePath` (

  `holderID` int(11) NOT NULL,

  `path` varchar(255) NOT NULL,

  PRIMARY KEY (`holderID`,`path`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `Lease` (

  `holder` varchar(255) NOT NULL,

  `lastUpdate` bigint(20) DEFAULT NULL,

  `holderID` int(11) DEFAULT NULL,

  PRIMARY KEY (`holder`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `INodeTableSimple` (

  `id` bigint(20) NOT NULL,

  `name` varchar(128) DEFAULT NULL,

  `parentid` bigint(20) DEFAULT NULL,

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

  `symlink` blob,

  PRIMARY KEY (`id`),

  KEY `path_lookup_idx` (`name`,`parentid`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1

/*!50100 PARTITION BY KEY (id)

PARTITIONS 1 */$$



delimiter $$



CREATE TABLE `DelegationKey` (

  `keyId` int(11) NOT NULL,

  `expiryDate` bigint(20) DEFAULT NULL,

  `keyBytes` varbinary(32) DEFAULT NULL,

  `keyType` smallint(6) DEFAULT NULL,

  PRIMARY KEY (`keyId`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `BlockTotal` (

  `Total` bigint(20) NOT NULL,

  `id` int(11) NOT NULL DEFAULT '0',

  PRIMARY KEY (`id`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$



delimiter $$



CREATE TABLE `BlockInfo` (

  `blockId` bigint(20) NOT NULL,

  `blockIndex` int(11) DEFAULT NULL,

  `iNodeID` bigint(20) DEFAULT NULL,

  `numBytes` bigint(20) DEFAULT NULL,

  `generationStamp` bigint(20) DEFAULT NULL,

  `replication` int(11) DEFAULT NULL,

  `BlockUCState` int(11) DEFAULT NULL,

  `timestamp` bigint(20) DEFAULT NULL,

  `primaryNodeIndex` int(11) DEFAULT NULL,

  `blockRecoveryId` bigint(20) DEFAULT NULL,

  PRIMARY KEY (`blockId`)

) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$




