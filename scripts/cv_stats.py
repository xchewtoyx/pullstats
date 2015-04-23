#!/usr/bin/env python
import os
import json
import logging

from pullstats.bigquery.client import BigQueryClient, BaseValidator

QUERY='''
SELECT
    metadata.timestamp AS time,
    protoPayload.resource AS resource,
    REGEXP_EXTRACT(protoPayload.line.logMessage, r' status=(\d+)') AS status,
    REGEXP_EXTRACT(protoPayload.line.logMessage, r' url=([^?]+)') AS cv_url,
    REGEXP_EXTRACT(protoPayload.line.logMessage,
                   r' msec=([\d.]+)') AS response_time,
    REGEXP_EXTRACT(protoPayload.line.logMessage, r' size=(\d+)') AS size,
    REGEXP_EXTRACT(protoPayload.line.logMessage, r' retry=(\d+)') AS retry
FROM (TABLE_DATE_RANGE(
         pull_api_logs.appengine_googleapis_com_request_log_,
         DATE_ADD(CURRENT_TIMESTAMP(), -300, "SECOND"),
         CURRENT_TIMESTAMP()))
WHERE
protoPayload.line.logMessage CONTAINS 'cvstats:' AND
metadata.timestamp > DATE_ADD(CURRENT_TIMESTAMP(), -300, 'SECOND')
LIMIT 1000;
'''

# Enter your Google Developer Project number
PROJECT_NUMBER = '462995942151'

class ComicvineValidator(BaseValidator):
    def time(self, value):
        return int(float(value)*1000)

    def status(self, value):
        return int(value)

    def response_time(self, value):
        return float(value)

    def size(self, value):
        return int(value)

    def retry(self, value):
        return int(value)


def main():
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)
    bq = BigQueryClient(PROJECT_NUMBER, os.path.dirname(__file__))
    data_points = bq.query(QUERY, validator=ComicvineValidator())
    logging.info('Found %d data points', len(data_points))

    if data_points:
        columns = [
            'time',
            'resource',
            'status',
            'cv_url',
            'response_time',
            'size',
            'retry'
        ]
        influx_points=[{
            'name': 'cvstats',
            'columns': columns,
            'points': [
                [row[key] for key in columns] for row in data_points
            ],
        }]

        print json.dumps(influx_points)

if __name__ == '__main__':
    main()
