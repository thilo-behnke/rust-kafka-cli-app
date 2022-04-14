#! /bin/bash

docker-compose down
docker-compose up -d --build --force-recreate
docker exec cli_app_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic topic --bootstrap-server localhost:9092
