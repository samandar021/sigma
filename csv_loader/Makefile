PYTHON ?= python
PIP ?= pip

install:
	$(PIP) install -r requirements.txt

test:
	pytest -q

test_cov:
	pytest --cov=csv_loader --cov-report=term-missing

run:
	$(PYTHON) -m csv_loader.main \
		--users data/users_big.csv \
		--products data/products_big.csv \
		--orders data/orders_big.csv \
		--init-db \
		--create-views

docker_run:
	docker-compose up --build
