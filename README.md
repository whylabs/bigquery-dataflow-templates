
This package contains GCP Dataflow templates that let you easily generate whylogs profiles from various data sources. None of these require any manual integration/code.

# Templates

- [Batch BigQuery Template][bq doc page]: Consume from a BigQuery table either directly, through a specified SQL query, or a generated SQL query based on a relative timestamp.

# Running

See [our full documentation][doc page] for guidance on running and configuring these templates in your GCP account.

# Development

Here are the setup instructions under Ubuntu/WSL.

- Install poetry, the python dependency manager: https://python-poetry.org/docs/#installation
- Configure poetry to create virtualenv locally: `poetry config virtualenvs.in-project true`
- Install pyenv: https://github.com/pyenv/pyenv-installer
  - Optional (fish shell), install fish plugin: `omf install pyenv`
- Make sure python build dependencies are installed: `sudo apt install libedit-dev libssl-dev libffi-dev libbz2-dev liblzma-dev`
  - Note: pyenv builds its python distributions locally. If you don't install these then you'll run into strange missing module errors when you run beam pipelines for things that you thought were part of the Python standard library.
- Set local python version to 3.8: `pyenv install 3.8 && pyenv local 3.8`
- Explicitly set python version: `poetry env use $(which python)`
- Install dependencies and generate a local virtual env for IDE/tooling: `poetry install`


[doc page]:https://docs.whylabs.ai/docs/integrations-bigquery-dataflow
[bq doc page]:https://docs.whylabs.ai/docs/integrations-bigquery-dataflow#batch-bigquery-template