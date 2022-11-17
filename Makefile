NAME=
BUCKET=gs://whylabs-dataflow-templates
REGION=us-west1 # us-west2, us-central1
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp
METADATA_FILES=$(shell find ./metadata -type f )
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
	poetry export -f requirements.txt --output requirements.txt

setup:
	poetry install

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'


