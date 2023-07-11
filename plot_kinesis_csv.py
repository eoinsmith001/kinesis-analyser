#!/usr/bin/env python3

from pandas import read_csv, to_datetime
import matplotlib.pyplot as plt
from datetime import datetime
import argparse


def do_report(csv_filename):
  # should all be in this format...
  # 2023-07-06 15:22:05.076000+01:00,3534
  date_format= "%Y-%d-%m %H:%M:%S.%f%z"

  df = read_csv(csv_filename, parse_dates=['date'], date_format=date_format)
  df['date'] = to_datetime(df['date'])
  rsCount = df.set_index('date').resample("1S").count()
  rsSize  = df.set_index('date').resample("1S").sum()
  print('Data Count Descriptive')
  print(rsCount.describe())
  print()
  print('Data Size Descriptive')
  print(rsSize.describe())
  print()
  fig, axs = plt.subplots(2, figsize=(12, 6))
  fig.suptitle(f'Kinesis Shard {csv_filename}')
  axs[0].plot(rsCount, color='blue', label='Event Count per Second')
  axs[0].axhline(y=1000, color='r', linestyle='-')
  axs[0].set_ylim([0,1000+10])
  axs[0].legend()
  axs[1].plot(rsSize, color='blue', label='Data Size per Second (KB)')
  axs[1].axhline(y=1024, color='r', linestyle='-')
  axs[1].set_ylim([0,1024+10])
  axs[1].legend()
  plt.show()

parser = argparse.ArgumentParser(
                    prog='plot_kinesis_csv.py',
                    description='shows data rate per second through shard',
                    epilog='Text at the bottom of help')

parser.add_argument('-f', '--filename', dest='filename', action='store')
args = parser.parse_args()
do_report(args.filename)
