test:
	echo test
build:
	docker build -t 511674593789.dkr.ecr.us-east-2.amazonaws.com/data-engineering .
push:
	docker push 511674593789.dkr.ecr.us-east-2.amazonaws.com/data-engineering
login:
	eval $(aws --profile sandbox ecr get-login --no-include-email --region us-east-2)
start:
	ecs-cli compose --file docker-compose-CeleryExecutor.yml service up --cluster sdc-airflow-dev
stop:
	ecs-cli compose --file docker-compose-CeleryExecutor.yml service down --cluster sdc-airflow-dev
deploy-ecs:
	ecs-cli compose --file docker-compose-CeleryExecutor.yml service up --cluster airflow
start_local:
	docker-compose -f docker-compose-LocalExecutor.yml up
stop_local:
	docker-compose -f docker-compose-LocalExecutor.yml down
start_local_celery:
	docker-compose -f docker-compose-Celery-Local.yml up -d
stop_local_celery:
	docker-compose -f docker-compose-Celery-Local.yml down
start_prod_celery:
	docker-compose -f docker-compose-Celery-Prod.yml up -d
stop_prod_celery:
	docker-compose -f docker-compose-Celery-Prod.yml down