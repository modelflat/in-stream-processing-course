drop keyspace if exists fraud_detector;
create keyspace fraud_detector with replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
use fraud_detector;
drop table if exists bots;
create table bots (bot_ip TEXT, reason TEXT, PRIMARY KEY (bot_ip));
drop table if exists bots_structured;
create table bots_structured (bot_ip TEXT, PRIMARY KEY (bot_ip));
