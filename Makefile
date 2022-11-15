NAME=
BUCKET=gs://whylabs-dataflow-templates
REGION=us-west1 # us-west2, us-central1
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp
METADATA_FILES=$(shell find ./metadata -type f )
SHA=$(shell git rev-parse HEAD)

# TODO make sure to version the different templates, probably with a latest version_metadata

.PHONY: default profile_query_template profile_query_template_matadata help upload_template profile_query_template_latest setup

default:help

profile_query_template_latest: NAME=profile_query_template
profile_query_template_latest: VERSION=latest
profile_query_template_latest: profile_query_template_matadata upload_template version_metadata ## Upload the dataflow template as the `latest` tag. 

profile_query_template: NAME=profile_query_template
profile_query_template: VERSION=$(SHA)
profile_query_template: profile_query_template_matadata upload_template version_metadata ## Upload the dataflow template that profiles a query

profile_query_template_matadata: NAME=profile_query_template
profile_query_template_matadata: ## Upload the metadata file for profile_query_template
	gcloud storage cp metadata/$(NAME)_metadata $(TEMPLATE_LOCATION)/$(VERSION)/$(NAME)_metadata

upload_template: 
upload_template: requirements.txt # Base target for other targets to use. Set the NAME, VERSION
	python -m ai.whylabs.templates.$(NAME) \
		--runner DataflowRunner \
		--project whylogs-359820 \
		--temp_location $(TEMPLATE_TMP_LOCATION) \
		--template_location $(TEMPLATE_LOCATION)/$(VERSION)/$(NAME) \
		--staging_location $(TEMPLATE_LOCATION)/$(VERSION)/$(NAME)_staging \
		--region $(REGION) \
		--requirements_file=requirements.txt

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


