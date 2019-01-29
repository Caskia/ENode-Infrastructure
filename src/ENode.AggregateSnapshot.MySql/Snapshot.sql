CREATE DATABASE `EventStore`;

USE `EventStore`;

CREATE TABLE `AggregateSnapshot` (
  `AggregateRootId` varchar(36) NOT NULL,
  `AggregateRootTypeName` varchar(256) NOT NULL,
  `Version` int(11) NOT NULL,
  `CreationTime` datetime NOT NULL,
  `ModificationTime` datetime DEFAULT NULL,
  `Payload` longblob,
  PRIMARY KEY (`AggregateRootId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

