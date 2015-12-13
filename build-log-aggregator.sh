#!/usr/bin/env bash

HOME_DIR=/data/users/data-infra

cd $HOME_DIR

# log-aggregator/
#   build-log-aggregator.sh
#   log_aggregator.cpp
#   log_aggregator.conf
#   log_aggregator.sh
#   producer.properties

cd log-aggregator

g++ -g -W -Wall -I/usr/local/include/librdkafka -I$HOME_DIR/kafkaclient/cpp -L$HOME_DIR/kafkaclient/cpp -o log_aggregator log_aggregator.cpp -lpykafkaclient -lpthread
#g++ -g -W -Wall -I/usr/local/include/librdkafka -I$HOME_DIR/kafkaclient/cpp -L$HOME_DIR/kafkaclient/cpp -pg -o log_aggregator log_aggregator.cpp -lpykafkaclient -lpthread
