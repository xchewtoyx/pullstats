#!/usr/bin/env python
import json

import httplib2

from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

QUERY='''
SELECT
    metadata.timestamp AS timestamp,
    protoPayload.resource AS resource,
    REGEXP_EXTRACT(protoPayload.line.logMessage, r'(\d+)$') AS cv_calls
FROM (TABLE_DATE_RANGE(
         pull_api_logs.appengine_googleapis_com_request_log_,
         DATE_ADD(CURRENT_TIMESTAMP(), -900, "SECOND"),
         CURRENT_TIMESTAMP()))
WHERE 
metadata.timestamp > DATE_ADD(CURRENT_TIMESTAMP(), -900, 'SECOND') AND
protoPayload.line.logMessage CONTAINS 'Comicvine api'
ORDER BY timestamp
LIMIT 1000;
'''

# Convert result columns from text to the correct type
VALIDATOR = {
    'timestamp': lambda x: int(float(x)*1000),
    'resource': lambda x: x,
    'cv_calls': lambda x: int(x),
}

# Enter your Google Developer Project number
PROJECT_NUMBER = '462995942151'

class BigQuery(object):
    def __init__(self):
        self.flow = flow_from_clientsecrets(
            'client_secrets.json',
            scope='https://www.googleapis.com/auth/bigquery')
        self.storage = Storage('bigquery_credentials.dat')
        self.credentials = self.storage.get()
        if self.credentials is None or self.credentials.invalid:
            self.authorize()
        self.http = self.credentials.authorize(httplib2.Http())
        self.service = build('bigquery', 'v2', http=self.http)

    def authorize(self):
        self.credentials = tools.run_flow(
            self.flow, self.storage,
            tools.argparser.parse_args([]))

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

def main():
    bq = BigQuery()
    data_points = bq.query(QUERY)

    influx_points=[{
        'name': 'requests',
        'columns': [
            'time',
            'resource',
            'cv_calls',
        ],
        'points': data_points
    }]

    print json.dumps(influx_points)

if __name__ == '__main__':
    main()
