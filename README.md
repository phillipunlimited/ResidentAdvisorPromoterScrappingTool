# ResidentAdvisorPromoterScrappingTool
A Resident Advisor Scrapping Tool for Promoters

This tool scrapes any city or regions' promoter page on resident advisor. Only works for promoter page. 

Anything starting with https://ra.co/promoters/[COUNTRYCODE]/[REGION/CITY] 
For example -> https://ra.co/promoters/UK/London.


Resident Advisor is an online music magazine and community platform dedicated to showcasing electronic music, artists and events across the globe.

## Installation

Install Python 3.9+ and add Python to PATH

Install requirements:
httpx, pandas, xlsxwriter, selectolax , pydash


```
pip install -r requirements.txt
```

or 

```
pip install httpx pandas xlsxwriter selectolax pydash
```


## Usage

```
usage: python raco.py [-h] [-o OUTPUT] [-t THREADS] [--no-excel]

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output file (default: output.csv)
  -t THREADS, --threads THREADS
                        Number of threads (default: 5)
  --no-excel            Do not convert to Excel (.xlsx) file. (default: False)

```

Example:

```
python ResidentAdvisorScraper.py --o output.csv
```


Note: If you are Mac user, please use `python3` instead of `python` and `pip3` instead of `pip` in the above commands.
