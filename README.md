# flock
## Clone the repository

`git clone https://github.com/npedroza/flock.git`

# This code runs with python3.11 
 
## If you want to run it directly on your computer you have to install the python packages as:
`pip install -r requirements.txt`
or 
`pip3 install -r requirements.txt`

Run it as:
`python3.11 main.py`

This code will generate three outputs files named {data...}.out in the directory "files/processed".
The metadata will be in the "files/meta" and the bad fields in the "files/bad". The "files/raw" is where the initial CSV files are put before running the validations.


As extra, I added a profiling using ydata\_profiling in pandas so that we can check the integrity of the dataframes.
That is run as an example:
`python3.11 profiling.py inputfile --output outputname.html`
in our case to check the 3 generated CSV files we run it as:

`python3.11 profiling.py files/processed/data_file_20210528182554.out --output profile1.html`\
`python3.11 profiling.py files/processed/data_file_20210528182844.out --output profile2.html`\
`python3.11 profiling.py files/processed/data_file_20210527182730.out --output profile3.html`

This will generate three profiling reports in html which can be opened in chrome, safari or any other browser to visualize the report of
the data.
