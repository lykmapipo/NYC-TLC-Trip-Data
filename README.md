# NYC-TLC-Trip-Data

Python scripts to download, process, and analyze [NYC Taxi and Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

> 👋 This repository's maintainer is available to hire for NYC TLC Trip data-engineering consulting projects. To get a cost estimate, send email to lallyelias87@gmail.com (for projects of any size or complexity).

## Requirements

- [Python 3.8+](https://www.python.org/)
- [pip 23.3+](https://github.com/pypa/pip)
- [joblib 1.3+](https://github.com/joblib/joblib)
- [pyarrow 14.0+](https://github.com/apache/arrow)
- [pandas 2.0+](https://github.com/pandas-dev/pandas)
- [geopandas 0.13+](https://github.com/geopandas/geopandas)
- [jupyterlab>=4.0+](https://github.com/jupyterlab/jupyterlab)

## Usage

- Clone this repository
```sh
git clone https://github.com/lykmapipo/NYC-TLC-Trip-Data.git
cd NYC-TLC-Trip-Data
```

- Install all dependencies

```sh
pip install -r requirements.txt
```

- Set environment variables
```sh
export AWS_ACCESS_KEY_ID="<YOUR_AWS_ACCESS_KEY>"
export AWS_SECRET_ACCESS_KEY="<YOUR_AWS_SECRET_ACCESS_KEY>"
export AWS_REGION="us-east-1"
```

- To extract `trips metadata (i.e year, month, size etc.)`, run:
```sh
python extract_trips_metadata.py
```

- To extract `zones data (i.e taxi+_zone_lookup, taxi_zones etc.)`, run:
```sh
python extract_zones_data.py
```


## Contribute

It will be nice, if you open an issue first so that we can know what is going on, then, fork this repo and push in your ideas. Do not forget to add a bit of test(s) of what value you adding.

## Questions/Issues/Contacts

lallyelias87@gmail.com, or open a GitHub issue


## Licence

The MIT License (MIT)

Copyright (c) lykmapipo & Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
