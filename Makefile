setup:
	python3 -m venv .envs/sparkifydb &&\
	source .envs/sparkifydb/bin/activate

install:
	python3 -m pip install --upgrade pip &&\
		python3 -m pip install -r requirements.txt

cluster:
	python3 manage_cluster.py create

tf-cluster:
	terraform init iac/ && terraform plan iac/ && terraform apply iac

clean:
	python3 manage_cluster.py delete

tf-clean:
	terraform destroy iac/

create:
	python3 create_tables.py

process:
	python3 etl.py

etl: create process

lint:
	pylint --disable=R,C,W1202,W0703 sql_queries.py create_tables.py etl.py

format:
	python3 -m black *.py

all: setup install lint format cluster etl

all-tf: setup install lint format tf-cluster etl
