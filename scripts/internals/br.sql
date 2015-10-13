delimiter $$

drop table if exists `bench_blockreporting_datanodes`$$

CREATE TABLE `bench_blockreporting_datanodes` (
  `id` int(11) NOT NULL,
  `dn` int(11) NOT NULL,
  `data` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`,`dn`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100*/$$
