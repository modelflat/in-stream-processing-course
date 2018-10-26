#!/usr/bin/env bash
kafka-topics --zookeeper localhost:2181 --topic clickstream-log --delete
kafka-topics --zookeeper localhost:2181 --topic clickstream-log --create --partitions 4 --replication-factor 1
