
This package contains GCP Dataflow templates that let you easily generate whylogs profiles from various data sources.

# Using

TBD, writing in progress. Reach out to us on [slack](https://whylabs.ai/slack-community) in the mean time.

# Templates

- Profile Query Template: Convert a BigQuery SQL query into a collection of whylogs dataset profiles. Ideal for regular batch jobs over a small number of days.



# Running

## Flex Template via CLI
`make run_template`

## Flex Template via Console

## Schedule Flex Template via CLI 

## Schedule Flex Template via Console

## Directly via CLI
`profile_query_local_query`



# Development
Here are the setup instructions under Ubuntu/WSL.

- Install poetry, the python dependency manager: https://python-poetry.org/docs/#installation
- Configure poetry to create virtualenv locally: `poetry config virtualenvs.in-project true`
- Install pyenv: https://github.com/pyenv/pyenv-installer
  - Optional (fish shell), install fish plugin: `omf install pyenv`
- Make sure python build dependencies are installed: `sudo apt install libedit-dev libssl-dev libffi-dev libbz2-dev liblzma-dev`
- Set local python version to 3.8: `pyenv install 3.8 && pyenv local 3.8`
- Explicitly set python version: `poetry env use $(which python)`
- Install dependencies and generate a local virtual env for IDE/tooling: `poetry install`


# Manual Testing
Sign off should include manual testing until CI is set up to trigger Dataflow jobs. This involves creating a Dataflow job using the "Custom" template optoin, and entering `whylabs-dataflow-templates/profile_query_template/latest/profile_query_template.json` for the template, replacing `SHA` with the most recent git sha. You can also get the sha from the build logs by checking out the `Upload template to GCS` step's commands.

Some sample arguments to use to launch the template:
- Output: gs://whylabs-dataflow-templates-tests/testing
- BigQuery Query: ``SELECT * FROM `bigquery-public-data.hacker_news.comments` where EXTRACT(year FROM time_ts) = 2015``
- Time Column: `time_ts`
- Org ID: `org-0`
- Model ID: `model-41i`
- date_grouping_frequency: `M`
- API Key: Generate one in WhyLabs

# TODO
- Reading from queries is apparently a lot slower than reading an entire table (of comparable size). Will need to have a separate job for backfill/historical data that uses table reads potentially.
- Minimize permissions once GCP identifies the ones that I'm using.



