NAME=
BUCKET=gs://whylabs-dataflow-templates
REGION=us-west1 # us-west2, us-central1
TEMPLATE_LOCATION=$(BUCKET)/$(NAME)
TEMPLATE_TMP_LOCATION=$(TEMPLATE_LOCATION)/tmp

# TODO make sure to version the different templates, probably with a latest

.PHONY: default profile_query_template profile_query_template_matadata help

default:help

profile_query_template: NAME=profile_query_template
profile_query_template: requirements.txt profile_query_template_matadata ## Upload the dataflow template that profiles a query
	python -m ai.whylabs.templates.$(NAME) \
		--runner DataflowRunner \
		--project whylogs-359820 \
		--staging_location $(BUCKET)/$(NAME)/staging \
		--temp_location $(TEMPLATE_TMP_LOCATION) \
		--template_location $(TEMPLATE_LOCATION) \
		--region $(REGION) \
		--requirements_file=requirements.txt

profile_query_template_matadata: NAME=profile_query_template
profile_query_template_matadata: ## Upload the metadata file for profile_query_template
	gsutil cp metadata/$(NAME)_metadata  $(BUCKET)/$(NAME)_metadata

requirements.txt: pyproject.toml
	poetry export -f requirements.txt --output requirements.txt

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'


