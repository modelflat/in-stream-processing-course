#!/usr/bin/env bash

while :
do
    ./botgen.py --bots 100 --users 50000 --freq $1 --duration $2 --file /tmp/generated/log-continuous.log
    sleep $3
done
