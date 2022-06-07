----------------------------------------
>>>          Security Atlas          <<<
----------------------------------------

Code for scraping data from Scopus database, storing them in GCP Datastore, and then running the community analysis locally.

#########################################################################
To run this project some Python modules must first be installed. This can be done by running:

pip install -r requirements.txt

Then, make sure to update atlas_config.py with the correct path towards your Google Cloud key

#########################################################################

This project has been tested to run best on:

Python 3.7.5 or later on macOS
or
Python 3.7.3 or later on linux (Ubuntu)

###### Usefull commands for Ubuntu: ######

sudo apt-get install python3.7-dev

sudo update-alternatives --config python3

optional:
pip install -U Pillow