NAME=
PY_SOURCE=$(shell find src/ -type f -name "*.py") 
TEMPLATE_PROJECT=whylogs-359820
BUCKET_NAME=whylabs-dataflow-templates
BUCKET=gs://$(BUCKET_NAME)
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp
REGION=us-central1
SHA=$(shell git rev-parse HEAD)
VERSION=$(SHA)
REQUIREMENTS=requirements.txt

.PHONY: default batch_bigquery_template upload_template 
.PHONY: example_run_direct_table example_run_template_table example_run_template_query example_run_template_offset
.PHONY: lint format format-fix test setup version_metadata help requirements version_string

default:help

batch_bigquery_template_latest: NAME=batch_bigquery_template
batch_bigquery_template_latest: VERSION=latest
batch_bigquery_template_latest: upload_template version_metadata ## Upload the dataflow template as the `latest` tag. 

batch_bigquery_template: NAME=batch_bigquery_template
batch_bigquery_template: upload_template version_metadata ## Upload the dataflow template that profiles a query

integ: example_run_template_table example_run_template_segmented_table

example_run_direct_table_btc: JOB_NAME=$(NAME)
example_run_direct_table_btc: TEMPLATE=batch_bigquery_template
example_run_direct_table_btc: requirements.txt ## Run the profile directly, job without templatizing it first.
	poetry run python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(JOB_NAME)" \
		--input-mode=BIGQUERY_TABLE \
		--input-bigquery-table=whylogs-359820.btc_cash.transactions \
		--date-column=block_timestamp \
		--date-grouping-frequency=Y \
		--org-id=org-0 \
		--project=whylogs-359820 \
		--region=$(REGION) \
		--logging-level=DEBUG \
		--output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--staging_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/staging \
		--temp_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/tmp \
		--tmp=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-42 \
		--num-workers 68 \
		--requirements_file=$(REQUIREMENTS)

example_run_direct_table: JOB_NAME=$(NAME)
example_run_direct_table: TEMPLATE=batch_bigquery_template
example_run_direct_table: requirements.txt ## Run the profile directly, job without templatizing it first.
	poetry run python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(JOB_NAME)" \
		--input-mode=BIGQUERY_TABLE \
		--input-bigquery-table=bigquery-public-data.hacker_news.full \
		--date-column=timestamp \
		--date-grouping-frequency=Y \
		--org-id=org-fjx9Rz \
		--project=whylogs-359820 \
		--region=$(REGION) \
		--logging-level=DEBUG \
		--output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--staging_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/staging \
		--temp_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/tmp \
		--tmp=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-11 \
		--requirements_file=$(REQUIREMENTS)

example_run_direct_segmented_table: JOB_NAME=$(NAME)
example_run_direct_segmented_table: TEMPLATE=batch_bigquery_template
example_run_direct_segmented_table: requirements.txt ## Run the profile directly, job without templatizing it first.
	poetry run python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(JOB_NAME)" \
		--input-mode=BIGQUERY_TABLE \
		--input-bigquery-table=bigquery-public-data.hacker_news.full \
		--date-column=timestamp \
		--date-grouping-frequency=Y \
		--org-id=org-fjx9Rz \
		--project=whylogs-359820 \
		--region=$(REGION) \
		--logging-level=DEBUG \
		--output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--staging_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/staging \
		--temp_location=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/tmp \
		--tmp=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-14 \
		--requirements_file=$(REQUIREMENTS) \
		--segment_columns="type, dead"

example_run_direct_query: JOB_NAME=$(NAME)
example_run_direct_query: TEMPLATE=batch_bigquery_template
example_run_direct_query: requirements.txt ## Run the profile directly, job without templatizing it first.
	poetry run python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(JOB_NAME)" \
		--input-mode=BIGQUERY_SQL \
		--input-bigquery-sql='select * from `bigquery-public-data.hacker_news.full`' \
		--date-column=timestamp \
		--date-grouping-frequency=Y \
		--org-id=org-0 \
		--project=whylogs-359820 \
		--region=$(REGION) \
		--logging-level=DEBUG \
		--output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-42 \
		--requirements_file=$(REQUIREMENTS)


example_run_direct_segmented_query: JOB_NAME=$(NAME)
example_run_direct_segmented_query: TEMPLATE=batch_bigquery_template
example_run_direct_segmented_query: requirements.txt ## Run the profile directly, job without templatizing it first.
	poetry run python src/ai/whylabs/templates/$(TEMPLATE).py \
		--job_name="$(JOB_NAME)" \
		--input-mode=BIGQUERY_SQL \
		--input-bigquery-sql='SELECT title, url, text, `by`, score, time, timestamp, type, id, parent, descendants, ranking, \
deleted, COALESCE(dead, FALSE) AS dead FROM `bigquery-public-data.hacker_news.full`' \
		--date-column=timestamp \
		--date-grouping-frequency=Y \
		--org-id=org-fjx9Rz \
		--project=whylogs-359820 \
		--region=$(REGION) \
		--logging-level=DEBUG \
		--output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-14 \
		--requirements_file=$(REQUIREMENTS) \
		--segment_columns="type, dead"


example_run_template_table: REGION=us-central1
example_run_template_table: TEMPLATE=batch_bigquery_template
example_run_template_table: JOB_NAME=$(NAME)
example_run_template_table: SHA=latest
example_run_template_table: ## Run the Profile Template in table mode
	gcloud dataflow flex-template run "$(JOB_NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=BIGQUERY_TABLE \
		--parameters input-bigquery-table=whylogs-359820:hacker_news.full \
		--parameters date-column=timestamp \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 68


example_run_template_segmented_table: REGION=us-central1
example_run_template_segmented_table: TEMPLATE=batch_bigquery_template
example_run_template_segmented_table: JOB_NAME=$(NAME)-segmented
example_run_template_segmented_table: SHA=latest
example_run_template_segmented_table: ## Run the Segmented Profile Template in table mode
	gcloud dataflow flex-template run "$(JOB_NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=BIGQUERY_TABLE \
		--parameters input-bigquery-table=whylogs-359820:hacker_news.full \
		--parameters date-column=timestamp \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/dataset_profile \
		--parameters segment_columns=type \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 68


example_run_template_query: REGION=us-central1
example_run_template_query: TEMPLATE=batch_bigquery_template
example_run_template_query: JOB_NAME=$(NAME)
example_run_template_query: SHA=latest
example_run_template_query: ## Run the Profile Template in query mode
	gcloud dataflow flex-template run "$(JOB_NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=BIGQUERY_SQL \
		--parameters input-bigquery-sql='select * from `whylogs-359820.btc_cash.transactions`' \
		--parameters date-column=fake_time_2 \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 68


example_run_template_offset: REGION=us-central1
example_run_template_offset: TEMPLATE=batch_bigquery_template
example_run_template_offset: JOB_NAME=$(NAME)-batch-template
example_run_template_offset: SHA=latest
example_run_template_offset: ## Run the Profile Template in offset mode
	gcloud dataflow flex-template run "$(JOB_NAME)" \
		--template-file-gcs-location gs://$(BUCKET_NAME)/$(TEMPLATE)/$(SHA)/$(TEMPLATE).json \
		--parameters input-mode=OFFSET \
		--parameters input-offset=-1 \
		--parameters logging-level=DEBUG \
		--parameters input-offset-table=whylogs-359820.btc_cash.transactions \
		--parameters date-column=fake_time_2 \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(JOB_NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region $(REGION) \
		--num-workers 68


upload_template: template_requirements.txt # Base target for other targets to use. Set the NAME, VERSION
	gcloud dataflow flex-template build $(TEMPLATE_LOCATION)/$(VERSION)/$(NAME).json \
		--sdk-language=PYTHON \
		--image-gcr-path=gcr.io/$(TEMPLATE_PROJECT)/$(NAME):$(SHA) \
		--flex-template-base-image=gcr.io/dataflow-templates-base/python38-template-launcher-base \
		--env=FLEX_TEMPLATE_PYTHON_PY_FILE=ai/whylabs/templates/$(NAME).py \
		--env=FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=template_requirements.txt \
		--py-path=src/ \
		--py-path=template_requirements.txt \
		--metadata-file=metadata/$(NAME)_metadata.json

version_string:  ## Update the version string
	sed -i 's/_dev_local/$(SHA)/g' ./src/ai/whylabs/templates/*.py

version_metadata:
	echo "$(SHA)" > /tmp/version_$(SHA).sha
	gcloud storage cp /tmp/version_$(SHA).sha $(TEMPLATE_LOCATION)/$(VERSION)/version.sha

requirements: requirements.txt template_requirements.txt integ_requirements.txt

requirements.txt: pyproject.toml
	poetry export -f requirements.txt > requirements.txt

template_requirements.txt: pyproject.toml
	poetry export -f requirements.txt --without dev --with beam > template_requirements.txt

integ_requirements.txt: pyproject.toml
	poetry export -f requirements.txt --without dev > integ_requirements.txt

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
