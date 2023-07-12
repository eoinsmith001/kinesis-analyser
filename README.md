# kinesis-analyser

A small utility to pull raw records from a Kinesis stream, and display summary statistics and high-resolution (i.e. per-second) graphs.

## Requirements

* Python3
* `pip3 install boto3`
* `pip3 install matplotlib`
* `pip3 install pandas`
* Valid `boto3` credentials (i.e. valid AWS credentials in the usual places)

## Usage

```
pull_from_kinesis [-h] [-s STREAM_NAME] [-n N_RECORDS]
// produces a CSV file <STREAM_NAME>.csv
// by default the start position is set to now()-21hours
// n records is 10 by default, useful for verification only
// probably want to use values like 10000 for real cases


plot_kinesis_csv.py [-h] [-f FILENAME]
// reads CSV file and resamples to 1s intervals for both count and sum (of data size)
// prints descriptive statistics 
// displays simple count and data per second graphs
```

## Sample output
<img width="1169" alt="image" src="https://github.com/eoinsmith001/kinesis-analyser/assets/3486700/a5bcc0e3-49eb-4031-ab9a-0f1c5ef962b5">

