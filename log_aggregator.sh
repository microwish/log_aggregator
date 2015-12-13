#!/usr/bin/env bash

# crontab
# */1 * * * * sh /data/users/data-infra/log-aggregator/log_aggregator.sh 3 >> /data/users/data-infra/log-aggregator/log_aggregator.sh.log 2>&1

ulimit -c unlimited
export LD_LIBRARY_PATH="/data/users/data-infra/kafkaclient/cpp:/usr/local/lib:$LD_LIBRARY_PATH"

num=$1

procs=$(ps -ef | grep 'log_aggregator' | grep -v 'grep')

for ((i=0; i<$num; i++))
do
    if echo "$procs" | grep -q "log_aggregator$i.conf"
    then
        continue
    else
        YmdHMs=$(date +%Y%m%d%H%M%S)
        echo "[$YmdHMs]log_aggregator#$i crashed or aborted"
    	/data/users/data-infra/log-aggregator/log_aggregator -c /data/users/data-infra/log-aggregator/log_aggregator$i.conf -b 192.168.145.201:9092,192.168.145.202:9092,192.168.145.203:9092,192.168.145.204:9092,192.168.145.205:9092,192.168.145.206:9092,192.168.145.207:9092,192.168.145.208:9092,192.168.145.209:9092,192.168.145.210:9092 -p /data/users/data-infra/log-aggregator/producer.properties -a /data/users/data-infra/log-aggregator/log_aggregator$i.offset -l /data/users/data-infra/log-aggregator/log_aggregator$i.log > /data/users/data-infra/log-aggregator/log_aggregator_stderr$i.log 2>&1 &
        sleep 7
    fi
done
