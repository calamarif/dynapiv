# dynapiv
This script is supposed to be deployed as an aws lambda function, although can be used as a stand alone script too

The purpose of this script is to dynamically pivot values where a dataset has been updated with different values (but the same schema).

Currently there are some assumptions/limitations (most of which could be changed with enhancements):
- assumes latest version of the libraryId provided will be used
- assumes source file schema doesn't change between runs
- assumes there is only one pivot in the project



- to create lambda zip file, copy python file to site-packages of the virtual environment (that you will create with the requirements.txt).
- zip -r9 your_lamda_filename.zip *
