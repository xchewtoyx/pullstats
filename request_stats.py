#!/usr/bin/env python
import os
import json
import logging

import httplib2

from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

QUERY='''
SELECT
    MAX(metadata.timestamp) AS timestamp,
    MAX(protoPayload.resource) AS resource,
    MAX(protoPayload.status) AS status,
    MAX(protoPayload.responseSize) AS size,
    MAX(protoPayload.latency) AS latency
FROM (TABLE_DATE_RANGE(
         pull_api_logs.appengine_googleapis_com_request_log_,
         DATE_ADD(CURRENT_TIMESTAMP(), -900, "SECOND"),
         CURRENT_TIMESTAMP()))
WHERE metadata.timestamp > DATE_ADD(CURRENT_TIMESTAMP(), -900, 'SECOND')
GROUP BY protoPayload.requestId
ORDER BY timestamp
LIMIT 1000
'''

# Convert result columns from text to the correct type
VALIDATOR = {
    'timestamp': lambda x: int(float(x)*1000),
    'resource': lambda x: x,
    'status': lambda x: int(x),
    'size': lambda x: int(x),
    'latency': lambda x: float(x.strip('s')),
}

# Enter your Google Developer Project number
PROJECT_NUMBER = '462995942151'

class BigQuery(object):
    def __init__(self):
        self.flow = flow_from_clientsecrets(
            file_path('client_secrets.json'),
            scope='https://www.googleapis.com/auth/bigquery')
        self.storage = Storage(file_path('bigquery_credentials.dat'))
        self.credentials = self.storage.get()
        if self.credentials is None or self.credentials.invalid:
            self.authorize()
        self.http = self.credentials.authorize(httplib2.Http())
        self.service = build('bigquery', 'v2', http=self.http)

    def authorize(self):
        self.credentials = tools.run_flow(
            self.flow, self.storage,
            tools.argparser.parse_args())

    def query(self, query):
        query_request = self.service.jobs()
        query_response = query_request.query(
            projectId=PROJECT_NUMBER, body={'query': query}).execute()

        data_points = []
        for row in query_response['rows']:
            result_row = []
            for i, value in enumerate(row['f']):
                column = query_response['schema']['fields'][i]['name']
                result_row.append(VALIDATOR[column](value['v']))
            data_points.append(result_row)

        return data_points


def file_path(basename):
    return os.path.join(os.path.dirname(__file__), basename)

def main():
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)
    bq = BigQuery()
    data_points = bq.query(QUERY)
    logging.info('Found %d data points:', len(data_points))

    if data_points:
        influx_points=[{
            'name': 'requests',
            'columns': [
                'time',
                'resource',
                'status_code',
                'size_bytes',
                'latency_secs',
            ],
            'points': data_points
        }]
    
        print json.dumps(influx_points)
    
if __name__ == '__main__':
    main()
