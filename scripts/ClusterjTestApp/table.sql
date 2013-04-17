CREATE TABLE `employee` (
  `id` int(11) NOT NULL,
  `first` varchar(64) DEFAULT NULL,
  `last` varchar(64) DEFAULT NULL,
  `municipality` varchar(64) DEFAULT NULL,
  `started` varchar(64) DEFAULT NULL,
  `ended` varchar(64) DEFAULT NULL,
  `department` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 
