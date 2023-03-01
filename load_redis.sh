#!/bin/bash

if [ -z $1 ]; then
        echo "usage: ./load_redis.sh [exp_number e.g., 14]"
        exit 0
fi

pgrep redis-server | xargs kill -9
sleep 5
cp /valinordatasets/redis_dumps/$1.rdb /var/lib/redis/dump.rdb
service redis-server start
echo "Try ping Redis... "
PONG=`redis-cli ping | grep PONG`
while [ -z "$PONG" ]; do
    sleep 1
    echo "Retry Redis ping... "
    PONG=`redis-cli ping | grep PONG`
done
echo "Redis is ready!"
