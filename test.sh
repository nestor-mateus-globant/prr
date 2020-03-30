#!/usr/bin/env sh

REPOSITORY_URI=663668850653.dkr.ecr.us-east-1.amazonaws.com/sdc-airflow:latest

docker build docker-whale .
docker images
docker tag docker-whale:latest $REPOSITORY_URI
docker push $REPOSITORY_URI