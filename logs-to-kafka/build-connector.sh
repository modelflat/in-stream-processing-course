#!/usr/bin/env bash

cd kafka-connect-logs && \
    mvn package && \
    cd .. && \
    cp kafka-connect-logs/target/kafka-connect-logs-*-standalone.jar ./kafka-connect-logs-plugin/