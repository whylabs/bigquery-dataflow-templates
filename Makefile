NAME=
BUCKET=gs://whylabs-dataflow-templates
REGION=us-west1 # us-west2, us-central1
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp
SHA=$(shell git rev-parse HEAD)

# TODO make sure to version the different templates, probably with a latest version_metadata

.PHONY: default profile_query_template profile_query_template_matadata help upload_template profile_query_template_latest setup upload_flex_template docker_flex_image

default:help

profile_query_template_latest: NAME=profile_query_template
profile_query_template_latest: VERSION=latest
profile_query_template_latest: upload_template version_metadata ## Upload the dataflow template as the `latest` tag. 

profile_query_template: NAME=profile_query_template
profile_query_template: VERSION=$(SHA)
profile_query_template: upload_template version_metadata ## Upload the dataflow template that profiles a query


profile_query_local_query: ## Upload the dataflow template that profiles a query
	python src/ai/whylabs/templates/profile_query_template.py \
		--input-mode=BIGQUERY_SQL \
		--input-bigquery-sql='SELECT * FROM `bigquery-public-data.hacker_news.comments` where EXTRACT(year FROM time_ts) = 2015' \
		--date-column=time_ts \
		--date-grouping-frequency=M \
		--org-id=org-0 \
		--project=whylogs-359820 \
		--region=us-central1 \
		--output=gs://whylabs-dataflow-templates-tests/table-query/profile \
		--api-key=$(WHYLABS_API_KEY) \
		--runner=DataflowRunner \
		--dataset-id=model-42 \
		--sdk_container_image=naddeoa/whylogs-dataflow-dependencies:no-beam \
		--prebuild_sdk_container_engine=cloud_build \
		--docker_registry_push_url=gcr.io/whylogs-359820/profile_query_template_worker_image

# Note, no --requirements_file. The --sdk_container_image contains the requirements
profile_query_local_table: ## Upload the dataflow template that profiles a query
	python src/ai/whylabs/templates/profile_query_template.py \
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
		--sdk_container_image=naddeoa/whylogs-dataflow-dependencies:no-beam \
		--prebuild_sdk_container_engine=cloud_build \
		--docker_registry_push_url=gcr.io/whylogs-359820/profile_query_template_worker_image


# TODO how to make this benefit from prebuild? Or does it already do that by default?
run_template: SHA=latest
run_template: ## Run the dataflow template for the given SHA. Can manually set SHA=latest as well with make run_template SHA=latest or some git sha.
	gcloud dataflow flex-template run "$(NAME)" \
		--template-file-gcs-location gs://whylabs-dataflow-templates/profile_query_template/$(SHA)/profile_query_template.json \
		--parameters input-mode=BIGQUERY_SQL \
		--parameters input-bigquery-sql='SELECT * FROM `bigquery-public-data.hacker_news.comments`' \
		--parameters date-column=time_ts \
		--parameters date-grouping-frequency=Y \
		--parameters org-id=org-0 \
		--parameters dataset-id=model-42 \
		--parameters output=gs://whylabs-dataflow-templates-tests/$(NAME)/dataset_profile \
		--parameters api-key=$(WHYLABS_API_KEY) \
		--region "us-west1" \
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

# TODO see if I can omit apache beam from here and have it still work. Might be a big time saver for startup. It should
# be present in the container base image
requirements.txt: pyproject.toml
	@# Filter out apache-beam because they pre-install that on the base images and installing it is time consuming
	poetry export -f requirements.txt --without-hashes > requirements.txt

setup:
	poetry install

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'


