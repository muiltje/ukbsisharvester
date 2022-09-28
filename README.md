# ukbsisharvester

Please note: add fiels to metadata.py if needed (e.g. isPartOf)

# Development Workflow:

After installing pip end pipenv run the following commands:

`git clone --recurse-submodules git@github.com: . . . `

`cd to/you/project/dir/`

`pipenv install` 

`pipenv shell`

`python harverst.py`

# Pipenv 

Pipenv is used to manage the project dependencies and create a virtual environment. 

More info: https://pipenv-fork.readthedocs.io/en/latest/basics.html


# PYOAI

This project uses: https://github.com/infrae/pyoai

The package is committed added as a git sumbodule, 

To check out the submodule:

`git clone --recurse-submodules https:// ....`

To init and update the submodule:

`git submodule init`

`git submodule update`

or alternatively

` git submodules update --init`

to update the submodule with the latest version

`git pull --recurse-submodules`

