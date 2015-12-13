#!/usr/bin/env bash

# nohup sh ./cpu_monitor.sh 3 > ./cpu_monitor.sh.log 2>&1 &

num=$1
#load=$(expr 240 / $num)
load=120

while :
do
    for ((i=0; i<$num; i++))
    do
        p=$(ps -ef | grep "log_aggregator$i" | grep -v 'grep')
        l=${#p}
        if [[ $l -eq 0 ]]
        then
            YmdHMs=$(date +%Y%m%d%H%M%S)
            echo "[$YmdHMs]log_aggregator#$i not found"
            continue
        else
            cols=(${p//	/ })
            pid=${cols[1]}
            cpu_load=$(top -n 1 -b -p $pid | tail -n2 | grep 'data-in' | awk -F' ' '{print $9}')
            cpu_load2=${cpu_load%.*}
            if [[ $cpu_load2 -gt $load ]]
            then
                YmdHMS=$(date +%Y%m%d%H%M%S)
                echo "[$YmdHMS]killing log_aggregator#$i[$pid $cpu_load]..."
                #gcore $pid > /dev/null && kill $pid
                #sleep 2
                kill $pid
            #else
                #echo "log_aggregator#$i[$pid] load[$cpu_load] OK"
            fi
            #sleep 5
        fi
    done
    sleep 35
done
