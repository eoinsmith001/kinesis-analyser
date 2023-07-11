#!/usr/bin/env python3

import boto3
import logging
from datetime import datetime, timedelta, timezone
import argparse

FORMAT = '%(asctime)s %(clientip)-15s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('kinesis_client')

class KinesisStream:
    """Encapsulates a Kinesis stream."""
    def __init__(self, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = None
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def get_records(self, max_records, start_time):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :param start_time: datetime representing start point
        :return: Yields the current batch of retrieved records.

        ShardIteratorType='AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
        """
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.name, ShardId=self.details['Shards'][0]['ShardId'],
                Timestamp=start_time,
                ShardIteratorType='AT_TIMESTAMP')
            shard_iter = response['ShardIterator']
            record_count = 0
            while record_count < max_records:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=10)
                shard_iter = response['NextShardIterator']
                records = response['Records']
                logger.info("Got %s records.", len(records))
                record_count += len(records)
                yield records
        except ClientError:
            logger.exception("Couldn't get records from stream %s.", self.name)
            raise

    def describe(self, name):
        """
        Gets metadata about a stream.

        :param name: The name of the stream.
        :return: Metadata about the stream.
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=name)
            self.name = name
            self.details = response['StreamDescription']
            logger.info("Got stream %s.", name)
        except ClientError:
            logger.exception("Couldn't get %s.", name)
            raise
        else:
            return self.details


def query_kinesis(stream_name, max_records=10):
  client = boto3.client('kinesis')
  s = KinesisStream(client)
  s.describe(stream_name)

  datetime_format = "%Y/%m/%d %H:%M:%S%Z"
  KB = 1024.0
  start = datetime.strptime("2023/07/10 15:05:00UTC", datetime_format)

  # datetime_format = "%Y/%m/%d %H:%M:%S%Z"
  # start = datetime.strptime("2023/07/10 15:05:00UTC", datetime_format)
  start = datetime.now(timezone.utc)
  delta = timedelta(days=0, hours=-21)
  start = start + delta
  fmt_strftime = "%Y-%m-%d %H:%M:%S.%f%z"
  print(f'Pulling from Kinesis {stream_name} from time: {datetime.strftime(start, fmt_strftime)}')

  with open(f'{stream_name}.csv', 'w') as f_out:
    f_out.write('date,event_size\n')
    for r in s.get_records(max_records, start):
      for e in r:
        # example: 2023-07-09 14:44:57.096000+01:00
        aat = e['ApproximateArrivalTimestamp'].strftime(fmt_strftime)
        f_out.write(f'{aat},{len(e["Data"])/KB}\n')

parser = argparse.ArgumentParser(
                    prog='pull_from_kinesis',
                    description='Pulls data from Kinesis',
                    epilog='Text at the bottom of help')
parser.add_argument('-s', '--stream', dest='stream_name', action='store')
parser.add_argument('-n', '--n-records', type=int, default=10)
args = parser.parse_args()
query_kinesis(args.stream_name, args.n_records)
