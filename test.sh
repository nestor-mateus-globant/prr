#!/usr/bin/env sh




REPOSITORY_URI=663668850653.dkr.ecr.us-east-1.amazonaws.com/sdc-airflow:latest


#docker system prune -a
#docker rmi 15ede5808858
docker build -t puckel/docker-airflow .
#docker build -t docker-whale .
docker images
docker tag puckel/docker-airflow:latest $REPOSITORY_URI
docker push $REPOSITORY_URI