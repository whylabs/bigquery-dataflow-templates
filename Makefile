NAME=
PY_SOURCE=$(shell find src/ -type f -name "*.py") 
BUCKET_NAME=whylabs-dataflow-templates
BUCKET=gs://$(BUCKET_NAME)
REGION=us-west1 # us-west2, us-central1
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp
REGION=us-central1
SHA=$(shell git rev-parse HEAD)


.PHONY: default profile_query_template upload_template 
.PHONY: example_run_direct_table example_run_template_table example_run_template_query example_run_template_offset
.PHONY: lint format test setup version_metadata help

default:help

profile_query_template_latest: NAME=profile_query_template
profile_query_template_latest: VERSION=latest
profile_query_template_latest: upload_template version_metadata ## Upload the dataflow template as the `latest` tag. 

profile_query_template: NAME=profile_query_template
profile_query_template: VERSION=$(SHA)
profile_query_template: upload_template version_metadata ## Upload the dataflow template that profiles a query


example_run_direct_table: REGION=us-central1
example_run_direct_table: TEMPLATE=profile_query_template
example_run_direct_table: SHA=latest
example_run_direct_table: ## Run the profile directly, job without templatizing it first.
	python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(NAME)"
		--input-mode=BIGQUERY_TABLE \
		--input-bigquery-table=bigquery-public-data:hacker_news.comments \
		--date-column=time_ts \
		--date-grouping-frequency=Y \
		--org-id=org-0 \
		--project=whylogs-359820 \
		--region=us-west1 \
		--output=gs://whylabs-dataflow-templates-tests/table-read/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-42 \
		--requirements_file=requirements.txt


example_run_template_table: REGION=us-central1
example_run_template_table: TEMPLATE=profile_query_template
example_run_template_table: SHA=latest
example_run_template_table: ## Run the Profile Template in table mode
	gcloud dataflow flex-template run "$(NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=BIGQUERY_TABLE \
		--parameters input-bigquery-sql='whylogs-359820.hacker_news.comments' \
		--parameters date-column=time_ts \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 300


example_run_template_query: REGION=us-central1
example_run_template_query: TEMPLATE=profile_query_template
example_run_template_query: SHA=latest
example_run_template_query: ## Run the Profile Template in query mode
	gcloud dataflow flex-template run "$(NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=BIGQUERY_SQL \
		--parameters input-bigquery-sql='select * from `whylogs-359820.btc_cash.transactions`' \
		--parameters date-column=fake_time_2 \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 300


example_run_template_offset: REGION=us-central1
example_run_template_offset: TEMPLATE=profile_query_template
example_run_template_offset: SHA=latest
example_run_template_offset: ## Run the Profile Template in offset mode
	gcloud dataflow flex-template run "$(NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=OFFSET \
		--parameters input-offset=-1 \
		--parameters logging-level=DEBUG \
		--parameters input-offset-table=whylogs-359820.btc_cash.transactions \
		--parameters date-column=fake_time_2 \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 300


upload_template: requirements.txt # Base target for other targets to use. Set the NAME, VERSION
	gcloud dataflow flex-template build $(TEMPLATE_LOCATION)/$(VERSION)/$(NAME).json \
		--sdk-language=PYTHON \
		--image-gcr-path=gcr.io/whylogs-359820/$(NAME):$(SHA) \
		--flex-template-base-image=gcr.io/dataflow-templates-base/python38-template-launcher-base \
		--env=FLEX_TEMPLATE_PYTHON_PY_FILE=ai/whylabs/templates/profile_query_template.py \
		--env=FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt \
		--py-path=src/ \
		--py-path=requirements.txt \
		--metadata-file=metadata/$(NAME)_metadata.json

version_metadata:
	echo "$(SHA)" > /tmp/version_$(SHA).sha
	gcloud storage cp /tmp/version_$(SHA).sha $(TEMPLATE_LOCATION)/$(VERSION)/version.sha

requirements.txt: pyproject.toml
	poetry export -f requirements.txt --without-hashes > requirements.txt

lint:
	poetry run mypy src/

format:
	poetry run black --check --line-length 140 src

format-fix:
	poetry run black --line-length 140 src

setup:
	poetry install

test:
	poetry run pytest

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
