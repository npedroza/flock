# flock
## Clone the repository

`git clone https://github.com/npedroza/flock.git`

# This code runs with python3.11 or 3.12
 
## If you want to run it directly on your computer you have to install the python packages as:
`pip install -r requirements.txt`
or 
`pip3 install -r requirements.txt`

Run it as:
`python3.11 main.py`

As extra, I added a profiling using ydata\_profiling in pandas so that we can check the integrity of the dataframes.
That is run as:
`python3.11 profiling.py inputfile --output outputname.html`
in our case to check the 3 generated CSV files we run it as:

`python3.11 profiling.py data_file_20210528182554.csv --output profile1.html`
`python3.11 profiling.py data_file_20210528182844.csv --output profile2.html`
`python3.11 profiling.py data_file_20210527182730.csv --output profile3.html`


