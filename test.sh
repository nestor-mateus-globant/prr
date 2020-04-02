#!/usr/bin/env sh




REPOSITORY_URI=663668850653.dkr.ecr.us-east-1.amazonaws.com/sdc-airflow:latest


#docker system prune -a
#docker rmi 15ede5808858
docker build --rm --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t sdc/docker-airflow .
#docker build -t sdc/docker-airflow -f Dockerfile.dev .
docker images
#docker tag sdc/docker-airflow:latest $REPOSITORY_URI
#docker push $REPOSITORY_URI