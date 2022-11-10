
This package contains GCP Dataflow templates that let you easily generate whylogs profiles from various data sources.

# Using

TBD, writing in progress. Reach out to us on [slack](https://whylabs.ai/slack-community) in the mean time.

# Templates

- Profile Query Template: Convert a BigQuery SQL query into a collection of whylogs dataset profiles. Ideal for regular batch jobs over a small number of days.

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

