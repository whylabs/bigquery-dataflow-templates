


- install poetry
- configure poetry to create virtualenv locally `poetry config virtualenvs.in-project true`
- install fish plugin `omf install pyenv`
- install pyenv https://github.com/pyenv/pyenv-installer
- setup pyenv https://github.com/pyenv/pyenv/wiki/Common-build-problems
- make build dependencies are installed `sudo apt install libssl-dev libffi-dev`
- set local python version to 3.8 `pyenv install 3.8 && pyenv local 3.8`
- maybe explicitly set python version `poetry env use (which python)`
- install dependencies `poetry install`
