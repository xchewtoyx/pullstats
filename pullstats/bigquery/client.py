import os
import json
import logging

import httplib2

from apiclient.discovery import build

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

logger = logging.getLogger(__name__) #pragma: no cover

class BaseValidator(object):
    def __init__(self, register_default=True):
        self.register_default = register_default

    def _default_validator(self, value):
        return value

    def __call__(self, column, value):
        return getattr(self, column)(value)

    def __getattr__(self, attribute):
        if self.register_default:
            return self._default_validator
        else:
            raise AttributeError('%r has no attribute %r' % (
                type(self), attribute))


class BigQueryClient(object):
    scopes = [ 'https://www.googleapis.com/auth/bigquery' ]

    def __init__(self, project_id, storage_path=os.getcwd(), http_client=None):
        if not http_client:
            http_client = httplib2.Http()
        self.project_id = project_id
        self.flow = flow_from_clientsecrets(
            os.path.join(storage_path, 'client_secrets.json'),
            scope=self.scopes)
        self.storage = Storage(os.path.join(
            storage_path, 'bigquery_credentials.dat'))
        self.credentials = self.storage.get()
        if self.credentials is None or self.credentials.invalid:
            self._authorize() # pragma: no cover
        self.http = self.credentials.authorize(http_client)

    def _authorize(self): # pragma: no cover
        self.credentials = tools.run_flow(
            self.flow, self.storage,
            tools.argparser.parse_args())

    def _service(self): # pragma: no cover
        return build('bigquery', 'v2', http=self.http)

    def query(self, query, validator=BaseValidator()):
        query_request = self._service().jobs()
        query_response = query_request.query(
            projectId=self.project_id, body={'query': query}).execute()

        data_points = []
        logger.info('Got %d results', len(query_response.get('rows', [])))
        for row in query_response.get('rows', []):
            result_row = {}
            for i, value in enumerate(row['f']):
                column = query_response['schema']['fields'][i]['name']
                result_row.update({
                    column: validator(column, value['v']),
                })
            data_points.append(result_row)

        return data_points
