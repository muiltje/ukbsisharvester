# Ukbsisharvester

Please note: add fiels to metadata.py if needed (e.g. isPartOf)

Added fields:

    'rights':      ('textList', 'oai_dc:dc/dc:rights/text()'),

    'ispartof':    ('textList', 'oai_dc:dc/dc:isPartOf/text()')
#Pipenv 
Pipenv is used to manage the project dependencies and create a virtual environment. 
More info: https://pipenv-fork.readthedocs.io/en/latest/basics.html

#Workflow:
After installing pip end pipenv run the following commands:

`pipenv install` 

`pipenv shell`


#PYOAI
This project uses: https://github.com/infrae/pyoai

The package is committed in the lib/ folder and the  inclusion of the packages in managed by **pipenv**

Inclusion of packages: https://stackoverflow.com/questions/63442168/build-and-install-local-package-with-pip-and-pipenv